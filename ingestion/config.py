"""Shared configuration for the ingestion scripts (BADR simulation, scraping, bronze load)."""

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
MODELS_DIR = PROJECT_ROOT / "models"

# Reuses the same .env the Phase 1 docker-compose stack was started with
# (falls back to no-op if it doesn't exist - defaults below then apply).
load_dotenv(PROJECT_ROOT / ".env")

# --- BADR (simulated customs declaration source) ---
BADR_DB_PATH = DATA_DIR / "badr.db"
BADR_TABLE_NAME = "badr_declarations"
BADR_DEFAULT_NUM_ROWS = 5000
BADR_DEFAULT_SEED = 42
BADR_HISTORY_DAYS = 730  # ~2 years of declarations, for historical analysis

# Trading partners plausible for Moroccan customs (ADII), mapped to their
# typical trade-invoicing currency. Used to keep PAYS/DEVISE coherent.
BADR_COUNTRY_CURRENCY = {
    "France": "EUR",
    "Espagne": "EUR",
    "Allemagne": "EUR",
    "Italie": "EUR",
    "Belgique": "EUR",
    "Pays-Bas": "EUR",
    "Portugal": "EUR",
    "Chine": "USD",
    "Turquie": "USD",
    "Etats-Unis": "USD",
    "Bresil": "USD",
    "Inde": "USD",
    "Coree du Sud": "USD",
    "Japon": "USD",
    "Royaume-Uni": "GBP",
}

# Occasionally a declaration is invoiced in a major trade currency other than
# the country's typical one (still coherent, just not the dominant case).
BADR_ALTERNATE_CURRENCIES = ["USD", "EUR"]
BADR_ALTERNATE_CURRENCY_RATE = 0.08

# Plausible HS/NGP-style codes (chapter prefix kept realistic), grouped by
# product category so a "reclassification" can stay in a nearby category.
BADR_HS_CODES_BY_CATEGORY = {
    "textile": ["61091000", "61102000", "62034200", "62046200", "62171000"],
    "electronique": ["85171200", "85287200", "85444200", "85182100", "85258010"],
    "machines": ["84713000", "84501100", "84796000", "84159000", "84335900"],
    "vehicules": ["87032319", "87089900", "87141000", "87032190", "87168000"],
    "alimentaire": ["08055000", "09011100", "19053100", "20079900", "21069098"],
    "plastique": ["39172100", "39231000", "39269097", "39204300"],
    "meubles": ["94036000", "94017100", "94054200"],
    "jouets": ["95030030", "95069990"],
}

# --- BADR quantity/weight/value generation, by business category (Phase 2.19) ---
# Phase 2.18 found VALEUR/POIDS were generated fully independently of the
# declaration's product category, which made them physically implausible at
# the unit level (e.g. a "1 smartphone" declaration weighing hundreds of kg)
# and made VALEUR_MAD/PRIX_REFERENCE mathematically incomparable (aggregate
# value vs unit price). This adds a genuine QUANTITE (commercial lot size)
# per declaration, keyed by the SAME 8 business categories already used
# above for CODE_NGP selection (no parallel per-code scheme invented), and
# makes POIDS_INITIAL/VALEUR_INITIALE scale with QUANTITE x a per-unit
# baseline instead of being drawn independently of it.
#
# These parameters are synthetic, order-of-magnitude judgment calls for an
# MVP (typical import-lot sizes and typical unit weight/value by commodity
# type) - NOT derived from the scraping/ML pipeline (PRIX_REFERENCE is never
# read here) and NOT tied to individual CODE_NGP values.
#   qty_mu/qty_sigma       : lognormal params for QUANTITE (units/declaration)
#   unit_weight_kg         : typical weight of a single unit, for this category
#   unit_value_mu/sigma    : lognormal params for a single unit's synthetic
#                            declared value (currency-agnostic scale, same
#                            convention the old flat VALEUR draw used)
BADR_QUANTITY_PARAMS_BY_CATEGORY = {
    "electronique": {"qty_mu": 3.69, "qty_sigma": 1.0, "unit_weight_kg": 2.0, "unit_value_mu": 5.01, "unit_value_sigma": 0.6},
    "machines": {"qty_mu": 2.71, "qty_sigma": 1.0, "unit_weight_kg": 20.0, "unit_value_mu": 5.99, "unit_value_sigma": 0.6},
    "textile": {"qty_mu": 5.70, "qty_sigma": 1.0, "unit_weight_kg": 0.3, "unit_value_mu": 3.00, "unit_value_sigma": 0.5},
    "vehicules": {"qty_mu": 1.61, "qty_sigma": 0.8, "unit_weight_kg": 150.0, "unit_value_mu": 7.09, "unit_value_sigma": 0.6},
    "alimentaire": {"qty_mu": 5.52, "qty_sigma": 1.0, "unit_weight_kg": 0.8, "unit_value_mu": 3.18, "unit_value_sigma": 0.5},
    "plastique": {"qty_mu": 5.01, "qty_sigma": 1.0, "unit_weight_kg": 1.5, "unit_value_mu": 3.69, "unit_value_sigma": 0.5},
    "meubles": {"qty_mu": 2.30, "qty_sigma": 0.9, "unit_weight_kg": 30.0, "unit_value_mu": 6.40, "unit_value_sigma": 0.6},
    "jouets": {"qty_mu": 5.30, "qty_sigma": 1.0, "unit_weight_kg": 0.4, "unit_value_mu": 3.40, "unit_value_sigma": 0.5},
}

# --- NGP reference for the scraping MVP (Phase 2.9 official Tarif analysis) ---
# category -> 8-digit HS/NGP code, taken from the official ADII Tarif des
# droits de douane (Edition 1er janvier 2022): position 85.17 for
# smartphones, 84.71 for PC portables, 85.28 for televiseurs. This is
# documented business knowledge from the customs tariff, NOT derived from
# BADR and NOT generated by Faker - see the Phase 2.9 analysis for the full
# 6/8/10-digit breakdown and sources. The 10-digit national subdivision
# (.90 smartphone / .90 PC portable / .99 televiseur, all meaning "finished,
# assembled retail unit") is a fixed business rule for this retail-scraping
# source, not a class the ML model is asked to predict in the Phase 2.10 MVP.
SCRAPING_CATEGORY_TO_NGP8 = {
    "Smartphone": "85171300",
    "PC Portable": "84713000",
    "Televiseur": "85287200",
}

# --- Web scraping (MVP price sample) ---
# Phase 2.8: extended from the Phase 2.6 Smartphone-only MVP to 3 categories
# (Smartphone / PC Portable / Televiseur) - different HS positions, so this
# gives the future CODE_NGP model a real multi-class problem to learn.
# Source: Jumia Maroc. robots.txt (jumia.ma/robots.txt) explicitly allows bot
# access provided the User-Agent self-identifies and stays under 200 req/min.
SCRAPING_BASE_URL = "https://www.jumia.ma"
SCRAPING_SITE_SOURCE = "jumia.ma"
SCRAPING_CATEGORIES = {
    "Smartphone": f"{SCRAPING_BASE_URL}/smartphones/",
    "PC Portable": f"{SCRAPING_BASE_URL}/pc-portables/",
    "Televiseur": f"{SCRAPING_BASE_URL}/smart-tv/",
}
# ~20-30 products per category (MVP scope, not a bulk scrape): 3 x 25 = ~75 total.
SCRAPING_MAX_PRODUCTS_PER_CATEGORY = 25
# Jumia paginates category listings via ?page=N; fetched only until enough
# products are collected (stops after page 1 if that's already sufficient -
# each of the 3 category pages has 40+ cards on page 1 alone, comfortably
# above the 25/category cap).
SCRAPING_MAX_PAGES_PER_CATEGORY = 2
SCRAPING_REQUEST_DELAY_SECONDS = 2.5
SCRAPING_REQUEST_TIMEOUT = 15
SCRAPING_USER_AGENT = (
    "ADII-DataPlatform-Scraper/1.0 "
    "(+contact: meryemelfdill@gmail.com; educational data-engineering MVP; "
    "respects robots.txt; well under 200 req/min)"
)
# Convention: Jumia is a B2C retail marketplace; the price on category pages
# is the public sale price shown to consumers (TTC per standard Moroccan
# e-commerce practice). Not a verified tax breakdown, just a documented label.
SCRAPING_PRICE_TYPE = "RETAIL_TTC"
SCRAPING_OUTPUT_CSV = DATA_DIR / "prix_web.csv"

# Jumia tags each listing card with its own GA4 category taxonomy, but the
# cleanest (single-value, 100% consistent) tag lives at a different depth per
# category: item_category3 for phones (item_category4 splits Android/iOS),
# item_category4 for laptops and TVs (verified against live listing pages).
# Only cards matching the expected value are kept - this excludes tablets
# from the phones page and any other page-sharing sibling product type.
SCRAPING_CATEGORY_GA4_FILTER = {
    "Smartphone": {"attr": "data-ga4-item_category3", "expected": "Smartphones"},
    "PC Portable": {"attr": "data-ga4-item_category4", "expected": "Laptops"},
    "Televiseur": {"attr": "data-ga4-item_category4", "expected": "Smart TVs"},
}

# Defensive keyword filter (fallback / belt-and-braces on top of the GA4 tag
# check, and the only check when a card lacks the GA4 attribute entirely):
# titles containing these tokens are accessories/parts, not the product
# itself, and must never end up in the dataset regardless of category.
SCRAPING_ACCESSORY_KEYWORDS = [
    "coque", "housse", "protection", "verre trempe", "verre trempé",
    "film ", "chargeur", "cable", "câble", "ecouteur", "écouteur",
    "casque", "powerbank", "power bank", "batterie externe", "support ",
    "adaptateur", "airpods", "montre connect", "smartwatch",
    "tablette", "tablet", "ipad", "sacoche", "sim card", "carte sim",
    "souris", "clavier", "sac a dos", "sac à dos", "telecommande",
    "télécommande", "webcam", "disque dur externe", "cle usb", "clé usb",
    "hub usb", "dock station",
]

# --- MinIO / S3 (reuses the Phase 1 docker-compose credentials & bucket) ---
# Same variable names already defined in .env.example - not new secrets.
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "datalake")

# MINIO_ENDPOINT (http://minio:9000) is the Docker-network-internal address,
# only reachable from other containers. This script runs on the HOST, so it
# must go through the published port instead - docker-compose.yml maps
# minio's 9000 to the host's 9000 (see `ports: ["9000:9000", ...]`).
# MINIO_ENDPOINT_HOST is a new, additive env var (documented in .env.example)
# specifically for host-run scripts; it does not change MINIO_ENDPOINT itself.
MINIO_ENDPOINT_HOST = os.environ.get("MINIO_ENDPOINT_HOST", "http://localhost:9000")

# --- Bronze layer (technical Parquet copies of the two MVP sources) ---
# Legacy fixed keys - still used by the currently-live main_pipeline.py
# (single daily-recompute DAG). Left untouched so that DAG keeps working
# unchanged while the new daily-simulation DAGs (Etape 3) are built.
BRONZE_BADR_KEY = "bronze/badr/badr.parquet"
BRONZE_SCRAPING_KEY = "bronze/scraping/prix_web.parquet"


def bronze_badr_key(run_date: str) -> str:
    """Partitioned Bronze BADR key for one day (run_date: 'YYYY-MM-DD').
    Each partition holds the FULL current badr_declarations table as of
    that ingestion (ingest_badr's query is unchanged - `SELECT * FROM
    badr_declarations` - only the destination key gains a date), not just
    that day's newly-appended rows. A full-snapshot-per-day design, chosen
    so bronze_to_silver.py never has to union multiple BADR partitions:
    "latest partition = current complete state" always holds.
    """
    return f"bronze/badr/date={run_date}/badr.parquet"


def bronze_scraping_key(run_date: str) -> str:
    """Partitioned Bronze scraping key for one day. Each partition holds
    only that day's live scrape (scrape_prices.py already only ever
    produces "today's" products - nothing changes on the source side).
    This is what actually historizes scraped prices day over day, so a
    future arbitrage run can know which price was in effect when a given
    declaration was judged.
    """
    return f"bronze/scraping/date={run_date}/prix_web.parquet"
