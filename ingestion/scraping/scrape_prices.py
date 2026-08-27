"""Scrape a 3-category MVP price sample from Jumia Maroc (Phase 2.8).

Categories: Smartphone, PC Portable, Televiseur - deliberately different HS
tariff positions, so the future CODE_NGP model has a real multi-class
problem to learn instead of the single-class Phase 2.6 Smartphone-only set.

Source: https://www.jumia.ma/{smartphones,pc-portables,smart-tv}/ (public
category listing pages only - no product-detail pages are visited, since
brand/model/price are already embedded as data attributes on each listing
card). Pagination (?page=2, ...) is used only until enough products have
been collected per category, up to config.SCRAPING_MAX_PAGES_PER_CATEGORY.

robots.txt (https://www.jumia.ma/robots.txt) explicitly allows bot access
provided the User-Agent clearly self-identifies and stays under 200
requests/minute. We use an honest, contact-bearing User-Agent and a 2.5s
delay between requests - nowhere near that limit. If the site ever responds
with 403/429 (blocking signal), the script logs it and stops cleanly instead
of retrying or working around it.

Category scope: each listing card carries Jumia's own GA4 category
attributes. The cleanest (single-value, verified 100% consistent per
category on the live pages) tag lives at a different taxonomy depth per
category - see config.SCRAPING_CATEGORY_GA4_FILTER. Only cards matching the
expected value are kept; this excludes tablets from the phones page, and any
other page-sharing sibling product type. A keyword-based accessory filter
(coque, chargeur, cable, souris, telecommande, ...) is applied on top as a
second safety net, and is the only check when a card lacks the GA4
attribute entirely.

Technical characteristics (ram, stockage, taille_ecran, batterie, reseau,
processeur, camera) are parsed from the product title text itself via
regex - this is text already present on the fetched page, not invented.
A field is left empty whenever the title doesn't contain an unambiguous
match (never guessed) - e.g. TV listings rarely mention RAM/processeur, so
those fields are legitimately empty for most Televiseur rows.

CODE_NGP is always written empty (NULL once read by pandas/Spark): it is
determined in a later phase from the Tarif douanier + ML model, not here.

Convention for `type_prix`: see config.SCRAPING_PRICE_TYPE - documented,
not fabricated.

Idempotence: every run re-fetches live data and overwrites
data/prix_web.csv completely (no append), so re-running never accumulates
stale or duplicate rows.

Usage
-----
    python ingestion/scraping/scrape_prices.py
    python ingestion/scraping/scrape_prices.py --max-per-category 25 --delay 3
"""

import argparse
import csv
import logging
import re
import sys
import time
from datetime import date
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("scrape_prices")

FIELDNAMES = [
    "marque",
    "modele",
    "description",
    "prix",
    "devise",
    "type_prix",
    "site_source",
    "url",
    "date_scraping",
    "categorie",
    "ram",
    "stockage",
    "reseau",
    "taille_ecran",
    "processeur",
    "batterie",
    "camera",
    "CODE_NGP",
]

# --- Spec parsing: only patterns that are unambiguous in the title text ---
RAM_LABELED_RE = re.compile(
    r"(\d+(?:\s*\+\s*\d+)?)\s*(?:Go|GB|Gb)?\s*(?:\([^)]*\)\s*)?RAM", re.IGNORECASE
)
ROM_LABELED_RE = re.compile(
    r"(\d+(?:\s*\+\s*\d+)?)\s*(?:Go|GB|Gb)?\s*(?:\([^)]*\)\s*)?ROM", re.IGNORECASE
)
COMBO_RE = re.compile(r"(\d+)\s*(?:Go|GB|Gb)\s*\+\s*(\d+)\s*(?:Go|GB|Gb)", re.IGNORECASE)
STORAGE_ONLY_RE = re.compile(r"(\d+)\s*(?:Go|GB|Gb)\b", re.IGNORECASE)
SSD_HDD_RE = re.compile(r"(\d+)\s*(?:Go|GB|Gb)\s*(SSD|HDD)", re.IGNORECASE)
SCREEN_RE = re.compile(r"(\d+[.,]\d+)\s*[\"'’″]|(\d+)\s*(?:pouces|'')", re.IGNORECASE)
BATTERY_RE = re.compile(r"(\d{3,5})\s*m[Aa][Hh]")
NETWORK_RE = re.compile(r"\b(5G|4G|3G|NFC)\b", re.IGNORECASE)
CAMERA_RE = re.compile(r"(\d+)\s*MP(?!\d)", re.IGNORECASE)
# Known CPU family tokens, checked in order - first unambiguous match wins.
PROCESSOR_RE = re.compile(
    r"(Ryzen\s*[3579]\s*(?:Pro)?[\w-]*"
    r"|Core\s*i[3579][\w-]*"
    r"|Core\s*[357][\w-]*"
    r"|Celeron[\w-]*"
    r"|Pentium[\w-]*"
    r"|Snapdragon[\w-]*"
    r"|MediaTek[\w-]*"
    r"|Exynos[\w-]*"
    r"|Apple\s*M[1234][\w]*)",
    re.IGNORECASE,
)


def extract_specs(text):
    """Parse technical characteristics from a listing title.

    Only returns a value when the title contains an unambiguous match;
    otherwise leaves the field empty (never guessed). Not every field is
    expected to be present for every category - e.g. TV titles rarely
    mention RAM/processeur, phone titles rarely mention processeur.
    """
    specs = {
        "ram": "",
        "stockage": "",
        "reseau": "",
        "taille_ecran": "",
        "processeur": "",
        "batterie": "",
        "camera": "",
    }
    if not text:
        return specs

    ram_match = RAM_LABELED_RE.search(text)
    if ram_match:
        specs["ram"] = f"{ram_match.group(1).replace(' ', '')} Go"

    # Most specific first: "512 Go SSD" / "500 Go HDD" (common on laptop
    # listings) names both the capacity and the storage technology.
    ssd_hdd_match = SSD_HDD_RE.search(text)
    if ssd_hdd_match:
        specs["stockage"] = f"{ssd_hdd_match.group(1)} Go {ssd_hdd_match.group(2).upper()}"

    rom_match = ROM_LABELED_RE.search(text)
    if not specs["stockage"] and rom_match:
        specs["stockage"] = f"{rom_match.group(1).replace(' ', '')} Go"

    if not specs["ram"] or not specs["stockage"]:
        combo_match = COMBO_RE.search(text)
        if combo_match:
            low, high = sorted((int(combo_match.group(1)), int(combo_match.group(2))))
            if not specs["ram"]:
                specs["ram"] = f"{low} Go"
            if not specs["stockage"]:
                specs["stockage"] = f"{high} Go"

    if not specs["stockage"]:
        storage_match = STORAGE_ONLY_RE.search(text)
        if storage_match:
            specs["stockage"] = f"{storage_match.group(1)} Go"

    screen_match = SCREEN_RE.search(text)
    if screen_match:
        value = screen_match.group(1) or screen_match.group(2)
        specs["taille_ecran"] = f'{value.replace(",", ".")}"'

    battery_match = BATTERY_RE.search(text)
    if battery_match:
        specs["batterie"] = f"{battery_match.group(1)} mAh"

    camera_match = CAMERA_RE.search(text)
    if camera_match:
        specs["camera"] = f"{camera_match.group(1)} MP"

    processor_match = PROCESSOR_RE.search(text)
    if processor_match:
        specs["processeur"] = processor_match.group(1).strip()

    network_hits = NETWORK_RE.findall(text)
    if network_hits:
        seen = []
        for hit in network_hits:
            token = hit.upper()
            if token not in seen:
                seen.append(token)
        specs["reseau"] = "/".join(seen)

    return specs


def is_accessory(text):
    lowered = (text or "").lower()
    return any(kw in lowered for kw in config.SCRAPING_ACCESSORY_KEYWORDS)


def parse_price(text):
    """'2,099.00 Dhs' / '8 999 DH' / '8999.00 MAD' -> (2099.0, 'MAD')."""
    if not text:
        return None, None
    upper = text.upper()
    devise = "MAD" if any(tok in upper for tok in ("DHS", "DH", "MAD")) else None
    cleaned = (
        upper.replace("DHS", "")
        .replace("MAD", "")
        .replace("DH", "")
        .replace("\xa0", "")
        .replace(" ", "")
        .replace(",", "")
        .strip()
    )
    try:
        return round(float(cleaned), 2), devise
    except ValueError:
        return None, devise


def fetch_page(session, url):
    try:
        resp = session.get(url, timeout=config.SCRAPING_REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as exc:
        logger.warning("Could not reach %s (%s) - skipping page.", url, exc)
        return None, False

    if resp.status_code in (403, 429):
        logger.error(
            "Site appears to be blocking scraping (HTTP %s) on %s - stopping cleanly.",
            resp.status_code,
            url,
        )
        return None, True  # signal: stop the whole run

    if resp.status_code != 200:
        logger.warning("Unexpected HTTP %s for %s - skipping page.", resp.status_code, url)
        return None, False

    return resp.text, False


def parse_cards(html, categorie, remaining_slots):
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("article.prd > a.core")
    logger.info("Found %d product cards on this page", len(cards))

    ga4_filter = config.SCRAPING_CATEGORY_GA4_FILTER.get(categorie)

    rows = []
    skipped_wrong_category = 0
    today = date.today().isoformat()
    for card in cards:
        if len(rows) >= remaining_slots:
            break
        try:
            href = card.get("href")
            if not href:
                continue
            product_url = href if href.startswith("http") else config.SCRAPING_BASE_URL + href

            marque = (card.get("data-ga4-item_brand") or "").strip()

            modele = (card.get("data-ga4-item_name") or "").strip()
            if not modele:
                name_el = card.select_one("div.name")
                modele = name_el.get_text(strip=True) if name_el else ""

            full_title = f"{marque} {modele}".strip()

            # Category scope: keep only cards that genuinely belong to this
            # category (e.g. exclude tablets from the phones page), using
            # Jumia's own GA4 category tag at the depth verified as clean
            # for this category (see config.SCRAPING_CATEGORY_GA4_FILTER).
            if ga4_filter is not None:
                tag_value = card.get(ga4_filter["attr"])
                if tag_value is not None and tag_value != ga4_filter["expected"]:
                    skipped_wrong_category += 1
                    continue
            if is_accessory(full_title):
                skipped_wrong_category += 1
                continue

            price_el = card.select_one("div.prc")
            price_text = price_el.get_text(strip=True) if price_el else None
            prix, devise = parse_price(price_text)

            if prix is None or prix <= 0 or not modele:
                logger.info("Skipping product with missing/invalid price or model: %s", product_url)
                continue

            specs = extract_specs(full_title)

            row = {
                "marque": marque,
                "modele": modele,
                "description": modele,
                "prix": prix,
                "devise": devise or "MAD",
                "type_prix": config.SCRAPING_PRICE_TYPE,
                "site_source": config.SCRAPING_SITE_SOURCE,
                "url": product_url,
                "date_scraping": today,
                "categorie": categorie,
                "CODE_NGP": "",
            }
            row.update(specs)
            rows.append(row)
        except Exception as exc:  # unexpected HTML shape on this one card only
            logger.warning("Unexpected HTML for a product card - skipping (%s)", exc)
            continue

    if skipped_wrong_category:
        logger.info(
            "Skipped %d cards not matching category '%s' (wrong sub-type/accessory)",
            skipped_wrong_category,
            categorie,
        )
    return rows


def fetch_category(session, categorie, base_url, max_products, delay, max_pages):
    logger.info("Fetching category '%s' -> %s", categorie, base_url)
    all_rows = []
    seen_urls = set()

    for page in range(1, max_pages + 1):
        page_url = base_url if page == 1 else f"{base_url}?page={page}"
        html, must_stop = fetch_page(session, page_url)
        if must_stop:
            return all_rows, True
        if html is None:
            break

        remaining_slots = max_products - len(all_rows)
        rows = parse_cards(html, categorie, remaining_slots)
        for row in rows:
            if row["url"] in seen_urls:
                continue
            seen_urls.add(row["url"])
            all_rows.append(row)

        if len(all_rows) >= max_products:
            break
        if page < max_pages:
            time.sleep(delay)

    return all_rows, False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-per-category", type=int, default=config.SCRAPING_MAX_PRODUCTS_PER_CATEGORY
    )
    parser.add_argument(
        "--max-pages", type=int, default=config.SCRAPING_MAX_PAGES_PER_CATEGORY
    )
    parser.add_argument("--delay", type=float, default=config.SCRAPING_REQUEST_DELAY_SECONDS)
    parser.add_argument("--output", type=Path, default=config.SCRAPING_OUTPUT_CSV)
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update({"User-Agent": config.SCRAPING_USER_AGENT})

    all_rows = []
    seen_urls = set()
    duplicate_urls = 0

    for categorie, url in config.SCRAPING_CATEGORIES.items():
        rows, must_stop = fetch_category(
            session, categorie, url, args.max_per_category, args.delay, args.max_pages
        )
        for row in rows:
            if row["url"] in seen_urls:
                duplicate_urls += 1
                continue
            seen_urls.add(row["url"])
            all_rows.append(row)
        if must_stop:
            logger.error("Stopping scraping run cleanly due to a site-blocking signal.")
            break

    if not all_rows:
        logger.error("No products collected - not writing an empty CSV.")
        sys.exit(1)

    # Secondary dedup pass: same marque+modele combination (case-insensitive)
    # occasionally slips through URL dedup as two distinct listing URLs for
    # what is effectively the same product/variant text.
    seen_marque_modele = set()
    deduped_rows = []
    duplicate_marque_modele = 0
    for row in all_rows:
        key = (row["marque"].strip().lower(), row["modele"].strip().lower())
        if key in seen_marque_modele:
            duplicate_marque_modele += 1
            continue
        seen_marque_modele.add(key)
        deduped_rows.append(row)
    all_rows = deduped_rows

    unexpected_category = [r for r in all_rows if r["categorie"] not in config.SCRAPING_CATEGORIES]
    if unexpected_category:
        raise AssertionError(
            f"{len(unexpected_category)} rows have a categorie outside "
            f"{list(config.SCRAPING_CATEGORIES)} - refusing to write CSV."
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(all_rows)

    logger.info(
        "Wrote %d products to %s (%d duplicate URLs skipped, %d duplicate marque+modele skipped)",
        len(all_rows),
        args.output,
        duplicate_urls,
        duplicate_marque_modele,
    )
    for cat in config.SCRAPING_CATEGORIES:
        count = sum(1 for r in all_rows if r["categorie"] == cat)
        logger.info("  %s: %d products", cat, count)


if __name__ == "__main__":
    main()
