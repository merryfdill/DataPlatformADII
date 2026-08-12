"""Shared configuration for the ingestion scripts (BADR simulation, scraping, bronze load)."""

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

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

# --- Web scraping (MVP price sample) ---
# Source: Jumia Maroc. robots.txt (jumia.ma/robots.txt) explicitly allows bot
# access provided the User-Agent self-identifies and stays under 200 req/min.
SCRAPING_BASE_URL = "https://www.jumia.ma"
SCRAPING_SITE_SOURCE = "jumia.ma"
SCRAPING_CATEGORIES = {
    "Smartphone": f"{SCRAPING_BASE_URL}/smartphones/",
    "PC Portable": f"{SCRAPING_BASE_URL}/pc-portables/",
    "Televiseur": f"{SCRAPING_BASE_URL}/smart-tv/",
}
SCRAPING_MAX_PRODUCTS_PER_CATEGORY = 20
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
BRONZE_BADR_KEY = "bronze/badr/badr.parquet"
BRONZE_SCRAPING_KEY = "bronze/scraping/prix_web.parquet"
