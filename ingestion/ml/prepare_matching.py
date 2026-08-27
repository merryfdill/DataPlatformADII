"""Prepare the CODE_NGP-level matching dataset between Silver BADR and the
ML-predicted scraping output (Phase 2.12, extended in Phase 2.13 with an
explicit NGP normalization layer).

Reads s3://datalake/silver/scraping_ml/ (written by apply_model.py) and
s3://datalake/silver/badr/ (untouched, existing Silver BADR), applies the
reviewed code normalization table from ingestion/ml/ngp_normalization.py
(BADR.CODE_NGP -> CODE_NGP_NORMALISE - never edits BADR itself), and builds
a CODE_NGP-level reconciliation: for every normalized code seen on either
side, how many BADR declarations and how many scraped products share it.

The join key is BADR.CODE_NGP_NORMALISE <-> SCRAPING.CODE_NGP_PREDIT - never
marque or modele, and never an individual product-level match. This is
deliberately NOT the final arbitrage: no reference price, no currency
conversion, no ratio, no NORMAL/MINORE/MAJORE classification. That is a
later phase.

Usage
-----
    python ingestion/ml/prepare_matching.py
"""

import io
import logging
import sys
from pathlib import Path

import boto3
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ngp_normalization as norm  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("prepare_matching")

SILVER_SCRAPING_ML_KEY = "silver/scraping_ml/scraping_predictions.parquet"
SILVER_BADR_PREFIX = "silver/badr/"
MATCHING_KEY = "silver/matching/ngp_matching_summary.parquet"
NORMALIZATION_REFERENCE_KEY = "silver/reference/ngp_code_normalization.parquet"


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT_HOST,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION,
    )


def read_parquet_prefix(s3, bucket, prefix):
    parts = [
        obj["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
    if not parts:
        raise FileNotFoundError(f"No .parquet object found under s3://{bucket}/{prefix}")
    frames = [pd.read_parquet(io.BytesIO(s3.get_object(Bucket=bucket, Key=k)["Body"].read())) for k in parts]
    return pd.concat(frames, ignore_index=True)


def read_parquet_key(s3, bucket, key):
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def build_matching_summary(scraping_ml, badr_normalized):
    """Join key: BADR.CODE_NGP_NORMALISE <-> SCRAPING.CODE_NGP_PREDIT."""
    scraping_counts = scraping_ml["CODE_NGP_PREDIT"].value_counts()
    badr_counts = badr_normalized["CODE_NGP_NORMALISE"].value_counts()

    # For each normalized code, which original BADR code(s) fed into it -
    # informational only, makes the 85171200 -> 85171300 fold-in visible.
    original_codes_by_normalized = (
        badr_normalized.groupby("CODE_NGP_NORMALISE")["CODE_NGP_ORIGINAL"]
        .apply(lambda s: ",".join(sorted(s.unique())))
    )

    all_codes = sorted(set(scraping_counts.index) | set(badr_counts.index))
    summary = pd.DataFrame({
        "CODE_NGP": all_codes,
        "CODE_NGP_ORIGINAL_BADR": [original_codes_by_normalized.get(c, "") for c in all_codes],
        "nb_badr": [int(badr_counts.get(c, 0)) for c in all_codes],
        "nb_scraping": [int(scraping_counts.get(c, 0)) for c in all_codes],
    })
    summary["matching_possible"] = (summary["nb_badr"] > 0) & (summary["nb_scraping"] > 0)
    return summary.sort_values("nb_scraping", ascending=False).reset_index(drop=True)


def upload_parquet(s3, df, bucket, key):
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    head = s3.head_object(Bucket=bucket, Key=key)
    logger.info("Uploaded %d bytes to s3://%s/%s", head["ContentLength"], bucket, key)


def verify_expected_mappings(badr_normalized, scraping_ml):
    """Step 6 of the Phase 2.13 brief: check the 3 expected reconciliations
    explicitly, don't just trust the general summary table.
    """
    logger.info("--- Verification des 3 correspondances attendues ---")

    checks = [
        ("85171200", "85171300", "Smartphone"),
        ("84713000", "84713000", "PC Portable"),
        ("85287200", "85287200", "Televiseur"),
    ]
    all_ok = True
    for original, normalized, label in checks:
        n_badr_original = (badr_normalized["CODE_NGP_ORIGINAL"] == original).sum()
        n_badr_normalized = (badr_normalized["CODE_NGP_NORMALISE"] == normalized).sum()
        n_scraping = (scraping_ml["CODE_NGP_PREDIT"] == normalized).sum()
        ok = n_badr_normalized == n_badr_original and n_badr_normalized > 0 and n_scraping > 0
        all_ok = all_ok and ok
        logger.info(
            "%s: BADR original %s (n=%d) -> normalise %s (n=%d) | scraping %s (n=%d) -> matching_possible=%s",
            label, original, n_badr_original, normalized, n_badr_normalized, normalized, n_scraping, ok,
        )
    if not all_ok:
        raise ValueError("One of the 3 expected Phase 2.13 reconciliations did not hold - see log above.")
    logger.info("Les 3 correspondances attendues sont verifiees.")


def main():
    s3 = get_s3_client()

    scraping_ml = read_parquet_key(s3, config.MINIO_BUCKET, SILVER_SCRAPING_ML_KEY)
    badr = read_parquet_prefix(s3, config.MINIO_BUCKET, SILVER_BADR_PREFIX)
    logger.info("Loaded scraping_ml: %d rows. Loaded Silver BADR: %d rows.", len(scraping_ml), len(badr))

    normalization_table = norm.get_normalization_table()
    logger.info("--- Table de normalisation NGP (ingestion/ml/ngp_normalization.py) ---\n%s",
                normalization_table[["ancien_code", "code_normalise", "source"]].to_string(index=False))

    badr_normalized = norm.apply_normalization(badr, source_column="CODE_NGP")
    n_changed = (badr_normalized["CODE_NGP_ORIGINAL"] != badr_normalized["CODE_NGP_NORMALISE"]).sum()
    logger.info("BADR: %d/%d lignes ont un CODE_NGP_NORMALISE different de l'original (normalisation appliquee "
                "uniquement dans cette vue en memoire, jamais ecrite dans Silver BADR ni badr.db)", n_changed, len(badr))

    verify_expected_mappings(badr_normalized, scraping_ml)

    n_total_scraping = len(scraping_ml)
    n_with_prediction = scraping_ml["CODE_NGP_PREDIT"].notna().sum()
    scraping_codes = set(scraping_ml["CODE_NGP_PREDIT"].dropna().unique())
    badr_codes_normalized = set(badr_normalized["CODE_NGP_NORMALISE"].dropna().unique())
    common_codes = scraping_codes & badr_codes_normalized

    n_scraping_matched = scraping_ml["CODE_NGP_PREDIT"].isin(common_codes).sum()
    n_scraping_unmatched = n_total_scraping - n_scraping_matched

    logger.info("--- Analyse du matching (apres normalisation) ---")
    logger.info("Total produits scraping: %d", n_total_scraping)
    logger.info("Produits scraping avec CODE_NGP_PREDIT: %d", n_with_prediction)
    logger.info("Codes NGP distincts cote scraping: %d -> %s", len(scraping_codes), sorted(scraping_codes))
    logger.info("Codes NGP normalises distincts cote BADR: %d -> %s", len(badr_codes_normalized), sorted(badr_codes_normalized))
    logger.info("Codes NGP communs (apres normalisation): %d -> %s", len(common_codes), sorted(common_codes))
    logger.info("Produits scraping avec au moins une correspondance BADR: %d", n_scraping_matched)
    logger.info("Produits scraping SANS correspondance BADR: %d", n_scraping_unmatched)

    summary = build_matching_summary(scraping_ml, badr_normalized)
    logger.info("--- CODE_NGP | CODE_NGP_ORIGINAL_BADR | nb_badr | nb_scraping | matching_possible ---\n%s",
                summary.to_string(index=False))

    if n_scraping_unmatched:
        unmatched_codes = scraping_codes - common_codes
        logger.warning(
            "%d produit(s) scraping n'ont AUCUNE correspondance BADR meme apres normalisation (code(s) %s) - "
            "ceci n'est pas force ni masque, juste rapporte.",
            n_scraping_unmatched, sorted(unmatched_codes),
        )

    upload_parquet(s3, normalization_table, config.MINIO_BUCKET, NORMALIZATION_REFERENCE_KEY)
    upload_parquet(s3, summary, config.MINIO_BUCKET, MATCHING_KEY)

    for key, expected_len in [(NORMALIZATION_REFERENCE_KEY, len(normalization_table)), (MATCHING_KEY, len(summary))]:
        reloaded = read_parquet_key(s3, config.MINIO_BUCKET, key)
        if len(reloaded) != expected_len:
            raise ValueError(f"Round-trip read-back row count mismatch for s3://{config.MINIO_BUCKET}/{key}.")
        logger.info("Round-trip read-back OK: %d rows at s3://%s/%s", len(reloaded), config.MINIO_BUCKET, key)


if __name__ == "__main__":
    main()
