"""Build the currency-rate reference table (Phase 2.15 structure, Phase 2.16
populated with official rates).

BADR declarations are invoiced in EUR, USD or GBP (verified against the
actual Silver BADR data - see docs/valeur_badr.md). To compare BADR.VALEUR
against PRIX_REFERENCE (Phase 2.14, in MAD), each currency needs a rate to
MAD. No exchange-rate source existed anywhere in this project before Phase
2.16 (checked: config.py, .env.example, generate_badr.py - the only
currency-adjacent constant found is BADR_ALTERNATE_CURRENCY_RATE, which is a
probability of picking a non-default invoicing currency, not an exchange
rate).

Rates below are Bank Al-Maghrib's official "Cours de référence" (the
Moroccan central bank's daily reference exchange rate, the authoritative
MAD rate source - https://www.bkam.ma/Marches/Principaux-indicateurs/
Marche-des-changes/Cours-de-change/Cours-de-reference), read on 2026-08-14,
dated by BAM 2026-08-13 (most recent publication at fetch time; BAM
publishes a new reference rate each business day at 16h15). Verified via
two independent extractions (WebFetch summary + raw HTML table parse) that
agreed exactly: 1 EUR = 10.7272 MAD, 1 USD = 9.3019 MAD, 1 GBP = 12.550 MAD
("1 LIVRE STERLING" in BAM's table). No rate here is invented or estimated.

Table: DEVISE | TAUX_MAD | SOURCE | DATE_TAUX
Usage: VALEUR_MAD = VALEUR * TAUX_MAD

Stored separately from BADR (s3://datalake/silver/reference/, same prefix
already used for ngp_code_normalization.parquet in Phase 2.13) - never
written into silver/badr/ or data/badr.db.

Usage
-----
    python ingestion/build_taux_change.py
"""

import io
import logging
import sys
from pathlib import Path

import boto3
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("build_taux_change")

TAUX_CHANGE_KEY = "silver/reference/taux_change.parquet"

BAM_SOURCE = (
    "Bank Al-Maghrib, Cours de reference officiel "
    "(https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-des-changes/Cours-de-change/Cours-de-reference)"
)
BAM_DATE_TAUX = "2026-08-13"  # date BAM du cours ; recupere le 2026-08-14

# One row per currency actually observed in BADR (verified, not assumed -
# see docs/valeur_badr.md). TAUX_MAD = MAD per 1 unit of that currency,
# from Bank Al-Maghrib's official reference rate table (see module
# docstring) - not invented, not estimated.
TAUX_CHANGE_TABLE = [
    {"DEVISE": "EUR", "TAUX_MAD": 10.7272, "SOURCE": BAM_SOURCE, "DATE_TAUX": BAM_DATE_TAUX},
    {"DEVISE": "USD", "TAUX_MAD": 9.3019, "SOURCE": BAM_SOURCE, "DATE_TAUX": BAM_DATE_TAUX},
    {"DEVISE": "GBP", "TAUX_MAD": 12.550, "SOURCE": BAM_SOURCE, "DATE_TAUX": BAM_DATE_TAUX},
]


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT_HOST,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION,
    )


def build_taux_change_table():
    return pd.DataFrame(TAUX_CHANGE_TABLE)


def upload_parquet(s3, df, bucket, key):
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    head = s3.head_object(Bucket=bucket, Key=key)
    logger.info("Uploaded %d bytes to s3://%s/%s", head["ContentLength"], bucket, key)


def main():
    s3 = get_s3_client()
    table = build_taux_change_table()
    if table["TAUX_MAD"].isnull().any():
        raise ValueError("A TAUX_MAD is NULL in TAUX_CHANGE_TABLE - no rate must be missing at this point.")
    logger.info("Currency rate reference table (official Bank Al-Maghrib rates, see docstring):\n%s", table.to_string(index=False))

    upload_parquet(s3, table, config.MINIO_BUCKET, TAUX_CHANGE_KEY)

    body = s3.get_object(Bucket=config.MINIO_BUCKET, Key=TAUX_CHANGE_KEY)["Body"].read()
    reloaded = pd.read_parquet(io.BytesIO(body))
    if len(reloaded) != len(table):
        raise ValueError("Round-trip read-back row count mismatch.")
    logger.info("Round-trip read-back OK: %d rows at s3://%s/%s", len(reloaded), config.MINIO_BUCKET, TAUX_CHANGE_KEY)


if __name__ == "__main__":
    main()
