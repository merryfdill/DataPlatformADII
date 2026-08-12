"""Scrape a small, representative MVP price sample from Jumia Maroc.

Source: https://www.jumia.ma (public category listing pages only - no
product-detail pages are visited, since brand/model/price are already
embedded as data attributes on each listing card, keeping this to 3 HTTP
requests total for the whole run).

robots.txt (https://www.jumia.ma/robots.txt) explicitly allows bot access
provided the User-Agent clearly self-identifies and stays under 200
requests/minute. We use an honest, contact-bearing User-Agent and a
2.5s delay between the 3 category requests - nowhere near that limit.
If the site ever responds with 403/429 (blocking signal), the script logs
it and stops cleanly instead of retrying or working around it.

Convention for `type_prix`: see config.SCRAPING_PRICE_TYPE - documented,
not fabricated.

Idempotence: every run re-fetches live data and overwrites
data/prix_web.csv completely (no append), so re-running never accumulates
stale or duplicate rows.

Usage
-----
    python ingestion/scraping/scrape_prices.py
    python ingestion/scraping/scrape_prices.py --max-per-category 15 --delay 3
"""

import argparse
import csv
import logging
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
    "prix",
    "devise",
    "type_prix",
    "site_source",
    "url",
    "date_scraping",
    "categorie",
]


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


def fetch_category(session, categorie, url, max_products):
    logger.info("Fetching category '%s' -> %s", categorie, url)
    try:
        resp = session.get(url, timeout=config.SCRAPING_REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as exc:
        logger.warning("Could not reach %s (%s) - skipping category.", url, exc)
        return [], False

    if resp.status_code in (403, 429):
        logger.error(
            "Site appears to be blocking scraping (HTTP %s) on %s - stopping cleanly.",
            resp.status_code,
            url,
        )
        return [], True  # signal: stop the whole run

    if resp.status_code != 200:
        logger.warning("Unexpected HTTP %s for %s - skipping category.", resp.status_code, url)
        return [], False

    soup = BeautifulSoup(resp.text, "lxml")
    cards = soup.select("article.prd > a.core")
    logger.info("Found %d product cards for '%s'", len(cards), categorie)

    rows = []
    today = date.today().isoformat()
    for card in cards:
        if len(rows) >= max_products:
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

            price_el = card.select_one("div.prc")
            price_text = price_el.get_text(strip=True) if price_el else None
            prix, devise = parse_price(price_text)

            if prix is None or not modele:
                logger.info("Skipping product with missing price/model: %s", product_url)
                continue

            rows.append(
                {
                    "marque": marque,
                    "modele": modele,
                    "prix": prix,
                    "devise": devise or "MAD",
                    "type_prix": config.SCRAPING_PRICE_TYPE,
                    "site_source": config.SCRAPING_SITE_SOURCE,
                    "url": product_url,
                    "date_scraping": today,
                    "categorie": categorie,
                }
            )
        except Exception as exc:  # unexpected HTML shape on this one card only
            logger.warning("Unexpected HTML for a product card - skipping (%s)", exc)
            continue

    return rows, False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-per-category", type=int, default=config.SCRAPING_MAX_PRODUCTS_PER_CATEGORY
    )
    parser.add_argument("--delay", type=float, default=config.SCRAPING_REQUEST_DELAY_SECONDS)
    parser.add_argument("--output", type=Path, default=config.SCRAPING_OUTPUT_CSV)
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update({"User-Agent": config.SCRAPING_USER_AGENT})

    all_rows = []
    seen_urls = set()
    categories = list(config.SCRAPING_CATEGORIES.items())
    duplicates = 0

    for i, (categorie, url) in enumerate(categories):
        rows, must_stop = fetch_category(session, categorie, url, args.max_per_category)
        for row in rows:
            if row["url"] in seen_urls:
                duplicates += 1
                continue
            seen_urls.add(row["url"])
            all_rows.append(row)
        if must_stop:
            logger.error("Stopping scraping run cleanly due to a site-blocking signal.")
            break
        if i < len(categories) - 1:
            time.sleep(args.delay)

    if not all_rows:
        logger.error("No products collected - not writing an empty CSV.")
        sys.exit(1)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(all_rows)

    logger.info("Wrote %d products to %s (%d duplicates skipped)", len(all_rows), args.output, duplicates)
    for cat in config.SCRAPING_CATEGORIES:
        count = sum(1 for r in all_rows if r["categorie"] == cat)
        logger.info("  %s: %d products", cat, count)


if __name__ == "__main__":
    main()
