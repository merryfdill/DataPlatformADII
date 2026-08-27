"""Apply the Phase 2.11 trained pipeline to Silver scraping (Phase 2.12).

Reads s3://datalake/silver/scraping/ (NOT Bronze - Bronze is never touched
by this script), loads models/ngp_classifier.joblib exactly as saved (no
retraining, no Dataset A, `categorie` never used as a feature), predicts
CODE_NGP_PREDIT for every row, and writes the result to a NEW, separate
location: s3://datalake/silver/scraping_ml/. The original Silver scraping
object is never overwritten, and the original CODE_NGP column (NULL) is
kept as-is and clearly distinct from the new CODE_NGP_PREDIT column.

This does not perform the BADR reconciliation/arbitrage - that is
ingestion/ml/prepare_matching.py (CODE_NGP-level matching only) and later
phases (reference price, ratio, NORMAL/MINORE/MAJORE), not here.

Usage
-----
    python ingestion/ml/apply_model.py                         # legacy fixed key only
    python ingestion/ml/apply_model.py --run-date 2026-09-01    # also writes the dated partition
"""

import io
import json
import logging
import sys
from pathlib import Path

import boto3
import joblib
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import features as feat  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("apply_model")

SILVER_SCRAPING_PREFIX = "silver/scraping/"
# Legacy fixed key - still what the currently-live main_pipeline.py's
# classification_ngp task produces (calls main() with no run_date).
SILVER_SCRAPING_ML_KEY = "silver/scraping_ml/scraping_predictions.parquet"


def silver_scraping_ml_key(run_date: str) -> str:
    """Partitioned scraping_ml key for one day - needed so a later
    arbitrage run (Etape 3, spark/jobs/prix_reference.py) can read the
    classified/priced products for a specific historical day instead of
    only ever seeing "whatever the most recent classification produced".
    """
    return f"silver/scraping_ml/date={run_date}/scraping_predictions.parquet"


MODEL_PATH = config.MODELS_DIR / "ngp_classifier.joblib"

CONFIDENCE_THRESHOLDS = [(0.80, "HIGH"), (0.60, "MEDIUM")]


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT_HOST,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION,
    )


def read_parquet_prefix(s3, bucket, prefix):
    """Reads and concatenates every .parquet object under a prefix (Spark
    writes one-or-more part-files, not a single fixed filename)."""
    parts = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                parts.append(obj["Key"])
    if not parts:
        raise FileNotFoundError(f"No .parquet object found under s3://{bucket}/{prefix}")
    logger.info("Reading %d parquet part(s) under s3://%s/%s", len(parts), bucket, prefix)
    frames = []
    for key in parts:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        frames.append(pd.read_parquet(io.BytesIO(body)))
    return pd.concat(frames, ignore_index=True)


def confidence_level(p):
    for threshold, label in CONFIDENCE_THRESHOLDS:
        if p >= threshold:
            return label
    return "LOW"


def load_pipeline():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"No trained pipeline at {MODEL_PATH} - run ingestion/ml/train_model.py first.")
    return joblib.load(MODEL_PATH)


def predict_batch(pipeline, df):
    """df: raw Silver scraping rows. Returns df with 4 new columns added:
    CODE_NGP_PREDIT, NGP_PROBA (JSON string per row), NGP_CONFIANCE (max
    proba), NGP_CONFIDENCE_LEVEL. The original CODE_NGP column is untouched.
    """
    working = feat.ensure_raw_columns(df)
    working["texte_produit"] = feat.build_texte_produit_series(working)
    working = feat.add_numeric_features(working)

    X = working[feat.FEATURE_COLUMNS]

    predictions = pipeline.predict(X)

    out = df.copy()
    out["CODE_NGP_PREDIT"] = predictions

    if hasattr(pipeline, "predict_proba"):
        probas = pipeline.predict_proba(X)
        classes = list(pipeline.classes_)
        out["NGP_PROBA"] = [
            json.dumps({cls: round(float(p), 4) for cls, p in zip(classes, row)}, ensure_ascii=False)
            for row in probas
        ]
        out["NGP_CONFIANCE"] = probas.max(axis=1).round(4)
        out["NGP_CONFIDENCE_LEVEL"] = out["NGP_CONFIANCE"].apply(confidence_level)
    else:
        logger.warning("Model has no predict_proba - NGP_PROBA/NGP_CONFIANCE/NGP_CONFIDENCE_LEVEL left NULL.")
        out["NGP_PROBA"] = None
        out["NGP_CONFIANCE"] = None
        out["NGP_CONFIDENCE_LEVEL"] = None

    return out


def consistency_checks(before, after):
    logger.info("--- Consistency checks ---")
    logger.info("rows before prediction: %d", len(before))
    logger.info("rows after prediction:  %d", len(after))
    if len(before) != len(after):
        raise ValueError("Row count changed during prediction - no row must be added or dropped.")

    n_null_pred = after["CODE_NGP_PREDIT"].isnull().sum()
    logger.info("CODE_NGP_PREDIT NULL count: %d", n_null_pred)

    n_classes = after["CODE_NGP_PREDIT"].nunique()
    logger.info("distinct predicted classes: %d", n_classes)
    logger.info("prediction distribution:\n%s", after["CODE_NGP_PREDIT"].value_counts().to_string())

    if after["NGP_CONFIANCE"].notna().any():
        logger.info(
            "NGP_CONFIANCE: min=%.4f mean=%.4f max=%.4f",
            after["NGP_CONFIANCE"].min(),
            after["NGP_CONFIANCE"].mean(),
            after["NGP_CONFIANCE"].max(),
        )
        logger.info("NGP_CONFIDENCE_LEVEL distribution:\n%s", after["NGP_CONFIDENCE_LEVEL"].value_counts().to_string())

    # Original CODE_NGP must remain untouched (still NULL at this pipeline stage).
    n_original_non_null = after["CODE_NGP"].notna().sum()
    logger.info("original CODE_NGP non-NULL count (must be 0): %d", n_original_non_null)
    if n_original_non_null:
        raise ValueError("Original CODE_NGP column was unexpectedly non-NULL - it must stay untouched.")


def diagnostic_categorie_vs_prediction(df):
    logger.info("--- Diagnostic ONLY (categorie was never used as a model feature) ---")
    crosstab = df.groupby(["categorie", "CODE_NGP_PREDIT"]).size().reset_index(name="nombre")
    logger.info("categorie | CODE_NGP_PREDIT | nombre\n%s", crosstab.to_string(index=False))

    expected = config.SCRAPING_CATEGORY_TO_NGP8
    matches_expected = all(
        set(df.loc[df["categorie"] == cat, "CODE_NGP_PREDIT"].unique()) == {code}
        for cat, code in expected.items()
    )
    if matches_expected:
        logger.warning(
            "Le modele predit exactement categorie -> NGP attendu (%s). "
            "C'est coherent avec ce dataset (73 produits, 3 categories, vocabulaire tres disjoint - "
            "voir docs/ml_model.md) mais NE constitue PAS une preuve de classification douaniere "
            "independante de la categorie : `categorie` n'a jamais ete une feature, mais la marque et "
            "le vocabulaire de gamme produit agissent comme un proxy quasi parfait dans cet echantillon.",
            expected,
        )
    else:
        logger.info("Le modele ne reproduit pas exactement le mapping categorie -> NGP attendu (voir crosstab ci-dessus).")


def upload_parquet(s3, df, bucket, key):
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    head = s3.head_object(Bucket=bucket, Key=key)
    logger.info("Uploaded %d bytes to s3://%s/%s", head["ContentLength"], bucket, key)


def main(run_date=None):
    """run_date (optional, 'YYYY-MM-DD'): when given, ALSO writes to the
    partitioned silver/scraping_ml/date=.../ key (in addition to the legacy
    fixed key, always written) - additive, not a breaking change. Deliberately
    a plain Python parameter, not argparse/sys.argv: main_pipeline.py's
    classification_ngp task (and daily_ingestion.py's, Etape 3) call this
    function directly within the Airflow process, so parsing the real
    process's argv here would be wrong/unsafe. See __main__ below for the
    CLI entrypoint.
    """
    s3 = get_s3_client()
    silver = read_parquet_prefix(s3, config.MINIO_BUCKET, SILVER_SCRAPING_PREFIX)
    logger.info("Silver scraping: %d rows, %d columns: %s", len(silver), silver.shape[1], list(silver.columns))

    pipeline = load_pipeline()
    logger.info("Loaded pipeline from %s", MODEL_PATH)

    predicted = predict_batch(pipeline, silver)
    consistency_checks(silver, predicted)
    diagnostic_categorie_vs_prediction(predicted)

    keys = [SILVER_SCRAPING_ML_KEY]
    if run_date:
        keys.append(silver_scraping_ml_key(run_date))

    for key in keys:
        upload_parquet(s3, predicted, config.MINIO_BUCKET, key)
        # Round-trip read-back to confirm the write is actually readable.
        body = s3.get_object(Bucket=config.MINIO_BUCKET, Key=key)["Body"].read()
        reloaded = pd.read_parquet(io.BytesIO(body))
        if len(reloaded) != len(predicted):
            raise ValueError(f"Round-trip read-back row count mismatch for {key}.")
        logger.info("Round-trip read-back OK: %d rows at s3://%s/%s", len(reloaded), config.MINIO_BUCKET, key)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-date",
        type=str,
        default=None,
        help="YYYY-MM-DD - also writes the partitioned silver/scraping_ml/date=.../ key.",
    )
    cli_args = parser.parse_args()
    main(cli_args.run_date)
