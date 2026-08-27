"""Silver scraping_ml -> reference price table, per CODE_NGP (Phase 2.14).

Reads the ML-predicted scraping products (Phase 2.12,
s3a://datalake/silver/scraping_ml/scraping_predictions.parquet), and builds
a market reference price per CODE_NGP_PREDIT: how many products, min/median/
mean/max price. This is a pure aggregation of the scraping side only - no
comparison against BADR.VALEUR, no currency conversion, no ratio, no
NORMAL/MINORE/MAJORE classification. That belongs to a later phase.

Why Spark: same tool already used for bronze_to_silver.py, kept consistent
with the project's existing Silver-layer convention rather than doing this
aggregation in pandas.

Median: computed with Spark SQL's exact `percentile(col, 0.5)` (not the
approximate `percentile_approx`) - with only ~23-25 rows per CODE_NGP group,
the exact computation is cheap and there is no reason to accept
approximation error on a dataset this small.

Currency: every price in Silver scraping is denominated in MAD (Jumia
Maroc retail prices - see ingestion/config.py SCRAPING_PRICE_TYPE). This is
verified against the actual data before computing anything (not assumed),
and the result is tagged DEVISE_REFERENCE="MAD" explicitly. No BADR
currency (EUR/USD/GBP/MAD) is read or mixed in at this stage.

Run with (same flags as bronze_to_silver.py - see that file for why):

    spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
        /home/iceberg/jobs/prix_reference.py

Optionally append `--date-debut YYYY-MM-DD --date-fin YYYY-MM-DD` after the
job path (Etape 3, arbitrage DAG) to compute the reference price over that
whole period instead of a single day - reads and unions every existing
silver/scraping_ml/date=.../ partition in range (see
ingestion/ml/apply_model.py silver_scraping_ml_key). If NEITHER partition
exists in range (the historical BADR population predates daily scraping
entirely, so there IS no per-day price for it), falls back to the legacy
fixed scraping_predictions.parquet - the only price data that ever existed
for that population - with a clear warning, so the first arbitrage run over
the full historical range reproduces today's validated result exactly.
Omit both flags entirely for the original single-file behavior, still what
the currently-live main_pipeline.py relies on.

Idempotence: `.mode("overwrite")` on a fixed path, coalesced to 1 file
(volumes are tiny - at most ~25 rows per group, 3 groups) - a rerun replaces
the previous output instead of accumulating files.
"""

import argparse
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_SCRAPING_ML_PATH = "s3a://datalake/silver/scraping_ml/scraping_predictions.parquet"
SILVER_SCRAPING_ML_PARTITION_TEMPLATE = "s3a://datalake/silver/scraping_ml/date={run_date}/scraping_predictions.parquet"
REFERENCE_PRIX_PATH = "s3a://datalake/silver/reference/prix_reference/"

EXPECTED_CODES = {"85171300", "84713000", "85287200"}
EXPECTED_DEVISE = "MAD"


def _path_exists(spark, path: str) -> bool:
    """Hadoop FS existence check - no boto3 needed, reuses the S3A
    filesystem Spark already has loaded via --packages hadoop-aws."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(hadoop_conf)
    return fs.exists(jvm_path)


def resolve_source_paths(spark, date_debut: str | None, date_fin: str | None) -> list[str]:
    """Returns the list of source paths to read. See module docstring for
    the fallback rule when no dated partition exists in [date_debut, date_fin].
    """
    if not date_debut or not date_fin:
        print(f"\nMode legacy (pas de plage fournie) - source unique : {SILVER_SCRAPING_ML_PATH}")
        return [SILVER_SCRAPING_ML_PATH]

    d0 = date.fromisoformat(date_debut)
    d1 = date.fromisoformat(date_fin)
    if d1 < d0:
        raise ValueError(f"--date-fin ({date_fin}) est avant --date-debut ({date_debut}).")

    all_days = [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]
    found, missing = [], []
    for day in all_days:
        path = SILVER_SCRAPING_ML_PARTITION_TEMPLATE.format(run_date=day)
        if _path_exists(spark, path):
            found.append(path)
        else:
            missing.append(day)

    print(f"\nPeriode demandee : {date_debut} -> {date_fin} ({len(all_days)} jour(s))")
    print(f"Partitions scraping_ml trouvees : {len(found)}/{len(all_days)}")
    if missing:
        print(f"Jours SANS partition scraping_ml (pas de scraping quotidien historise pour ces jours) : "
              f"{missing[:10]}{' ...' if len(missing) > 10 else ''}")

    if not found:
        print(
            "\nAucune partition datee dans cette periode - repli sur le fichier legacy "
            f"({SILVER_SCRAPING_ML_PATH}), seule donnee de prix jamais disponible pour cette population "
            "(periode historique anterieure au demarrage du scraping quotidien)."
        )
        return [SILVER_SCRAPING_ML_PATH]

    return found


def main(date_debut=None, date_fin=None):
    spark = SparkSession.builder.appName("prix_reference").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=" * 60)
        print("PRIX_REFERENCE: Silver scraping_ml -> Silver reference")
        print("=" * 60)

        source_paths = resolve_source_paths(spark, date_debut, date_fin)
        df = spark.read.parquet(*source_paths)
        total = df.count()
        print(f"\nSilver scraping_ml row count: {total}")
        print("Schema:")
        df.printSchema()

        # --- Currency check BEFORE any computation - never assumed ---
        devises = sorted(r["devise"] for r in df.select("devise").distinct().collect())
        print(f"\nDistinct 'devise' values found in source: {devises}")
        if devises != [EXPECTED_DEVISE]:
            raise ValueError(
                f"Expected only '{EXPECTED_DEVISE}' in scraping prices, found {devises} - "
                "refusing to compute a reference price that would silently mix currencies."
            )
        print(f"Confirmed: all prices are in {EXPECTED_DEVISE}. No BADR currency is read at this stage.")

        # --- Filter: CODE_NGP_PREDIT not null, prix not null, prix > 0 ---
        filtered = df.filter(
            F.col("CODE_NGP_PREDIT").isNotNull() & F.col("prix").isNotNull() & (F.col("prix") > 0)
        )
        n_filtered = filtered.count()
        n_dropped = total - n_filtered
        print(f"\nRows after filter (CODE_NGP_PREDIT not null, prix not null, prix>0): {n_filtered} / {total}")
        print(f"Rows dropped by filter: {n_dropped} (expected 0 - Phase 2.12 already guarantees no NULL/invalid here)")

        # --- Per-code product counts (validation #2 - actual counts, not hardcoded) ---
        counts = filtered.groupBy("CODE_NGP_PREDIT").count().orderBy("CODE_NGP_PREDIT")
        print("\nProduct count per CODE_NGP_PREDIT (actual data, not hardcoded expectations):")
        counts.show(truncate=False)

        found_codes = {r["CODE_NGP_PREDIT"] for r in counts.collect()}
        unexpected = found_codes - EXPECTED_CODES
        if unexpected:
            raise ValueError(f"Unexpected CODE_NGP_PREDIT value(s) found: {unexpected}")
        missing = EXPECTED_CODES - found_codes
        if missing:
            raise ValueError(f"Expected CODE_NGP(s) missing from the data: {missing}")
        print(f"Confirmed: exactly the 3 expected codes are present, no unexpected code: {sorted(found_codes)}")

        # --- Aggregation ---
        agg = (
            filtered.groupBy("CODE_NGP_PREDIT")
            .agg(
                F.count("*").alias("NB_PRODUITS"),
                F.min("prix").alias("PRIX_MIN"),
                F.expr("percentile(prix, 0.5)").alias("PRIX_MEDIAN"),
                F.avg("prix").alias("PRIX_MOYEN"),
                F.max("prix").alias("PRIX_MAX"),
            )
            .withColumnRenamed("CODE_NGP_PREDIT", "CODE_NGP")
            .withColumn("DEVISE_REFERENCE", F.lit(EXPECTED_DEVISE))
            .orderBy("CODE_NGP")
        )

        print("\nPRIX_REFERENCE result:")
        agg.show(truncate=False)

        # --- Validations on the aggregated result ---
        rows = agg.collect()
        for r in rows:
            code = r["CODE_NGP"]
            pmin, pmed, pmoy, pmax = r["PRIX_MIN"], r["PRIX_MEDIAN"], r["PRIX_MOYEN"], r["PRIX_MAX"]
            if not (pmin <= pmed <= pmax):
                raise ValueError(f"{code}: PRIX_MIN <= PRIX_MEDIAN <= PRIX_MAX violated ({pmin}, {pmed}, {pmax})")
            if not (pmin <= pmoy <= pmax):
                raise ValueError(f"{code}: PRIX_MIN <= PRIX_MOYEN <= PRIX_MAX violated ({pmin}, {pmoy}, {pmax})")
            if r["NB_PRODUITS"] <= 0:
                raise ValueError(f"{code}: NB_PRODUITS <= 0")
        print("\nValidation OK for every CODE_NGP: PRIX_MIN <= PRIX_MEDIAN <= PRIX_MAX "
              "and PRIX_MIN <= PRIX_MOYEN <= PRIX_MAX (median/mean order not assumed either way).")

        agg.coalesce(1).write.mode("overwrite").parquet(REFERENCE_PRIX_PATH)
        print(f"\nWritten to {REFERENCE_PRIX_PATH}")

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Source rows: {total} | after filter: {n_filtered} | CODE_NGP groups: {len(rows)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date-debut", type=str, default=None, help="YYYY-MM-DD - start of the arbitrated period.")
    parser.add_argument("--date-fin", type=str, default=None, help="YYYY-MM-DD - end of the arbitrated period.")
    cli_args = parser.parse_args()
    main(cli_args.date_debut, cli_args.date_fin)
