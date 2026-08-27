"""Silver BADR + currency reference -> BADR value working view.

Phase 2.15 built this job with TAUX_MAD left NULL (no rate source existed
yet). Phase 2.16 populated s3a://datalake/silver/reference/taux_change.parquet
with Bank Al-Maghrib's official reference rates (see
ingestion/build_taux_change.py for the source/date), so this job (unchanged
logic) now produces a fully populated VALEUR_MAD.

Reads Silver BADR (s3a://datalake/silver/badr/, untouched, read-only) and
the currency-rate reference table, joins them on DEVISE, and writes a
working dataset with VALEUR_MAD = VALEUR * TAUX_MAD.

Phase 2.20 update: Phase 2.19 added QUANTITE to Silver BADR and regenerated
it; this job's output SELECT list is updated to carry QUANTITE through (it
was previously omitted simply because it didn't exist yet when this job was
first written in Phase 2.15/2.16) so Phase 2.20 can compute
VALEUR_UNITAIRE_MAD = VALEUR_MAD / QUANTITE downstream. No other logic
changed.

This does NOT compare VALEUR_MAD against PRIX_REFERENCE, does NOT compute a
ratio, and does NOT classify anything NORMAL/MINORE/MAJORE - that is a later
phase. Silver BADR itself is never written to - this job only reads it and
writes a brand-new, separate dataset.

Why Spark: consistent with bronze_to_silver.py and prix_reference.py -
same tool already used for the project's Silver-layer transformations.

Run with (same flags as bronze_to_silver.py / prix_reference.py):

    spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
        /home/iceberg/jobs/badr_valeur_prep.py

Idempotence: `.mode("overwrite")` on a fixed path, coalesced to 1 file
(5000 rows is tiny) - a rerun replaces the previous output.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

SILVER_BADR_PATH = "s3a://datalake/silver/badr/"
TAUX_CHANGE_PATH = "s3a://datalake/silver/reference/taux_change.parquet"
BADR_VALEUR_PATH = "s3a://datalake/silver/badr_valeur/"

EXPECTED_DEVISES = {"EUR", "USD", "GBP"}


def main():
    spark = SparkSession.builder.appName("badr_valeur_prep").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=" * 60)
        print("BADR_VALEUR_PREP: Silver BADR + taux_change -> working view")
        print("=" * 60)

        badr = spark.read.parquet(SILVER_BADR_PATH)
        n_badr = badr.count()
        print(f"\nSilver BADR row count (read-only source): {n_badr}")

        taux = spark.read.parquet(TAUX_CHANGE_PATH)
        print("\nTaux de change reference (Bank Al-Maghrib official rates, Phase 2.16):")
        taux.show(truncate=False)

        n_taux_null = taux.filter(F.col("TAUX_MAD").isNull()).count()
        if n_taux_null:
            raise ValueError(f"{n_taux_null} row(s) in taux_change have a NULL TAUX_MAD - refusing to proceed.")

        devises_badr = {r["DEVISE"] for r in badr.select("DEVISE").distinct().collect()}
        print(f"\nDistinct DEVISE in Silver BADR: {sorted(devises_badr)}")
        unexpected = devises_badr - EXPECTED_DEVISES
        if unexpected:
            print(f"NOTE: unexpected currency/currencies found: {unexpected} - not in the reference table, "
                  "TAUX_MAD/VALEUR_MAD will be NULL for those rows (not dropped, just unmatched).")

        working = (
            badr.join(taux, on="DEVISE", how="left")
            .withColumn("VALEUR_MAD", (F.col("VALEUR").cast(DecimalType(18, 4)) * F.col("TAUX_MAD")))
            .select(
                "id", "DATE_DEPOT", "CODE_NGP", "CODE_NGP_INITIAL",
                "VALEUR", "DEVISE", "TAUX_MAD", "VALEUR_MAD", "QUANTITE", "POIDS",
            )
        )

        n_working = working.count()
        print(f"\nWorking dataset row count: {n_working} (must equal source: {n_badr})")
        if n_working != n_badr:
            raise ValueError(f"Row count changed during join: {n_badr} -> {n_working}")

        n_taux_mad_null = working.filter(F.col("TAUX_MAD").isNull()).count()
        n_valeur_mad_null = working.filter(F.col("VALEUR_MAD").isNull()).count()
        n_valeur_mad_non_null = n_working - n_valeur_mad_null
        print(f"Rows with a non-NULL VALEUR_MAD: {n_valeur_mad_non_null} / {n_working}")
        print(f"Rows with a NULL TAUX_MAD: {n_taux_mad_null} (must be 0 - all 3 BADR currencies have an official rate)")
        print(f"Rows with a NULL VALEUR_MAD: {n_valeur_mad_null} (must be 0 for the same reason)")
        if n_taux_mad_null:
            raise ValueError(f"{n_taux_mad_null} row(s) have a NULL TAUX_MAD - a BADR currency has no matching rate.")
        if n_valeur_mad_null:
            raise ValueError(f"{n_valeur_mad_null} row(s) have a NULL VALEUR_MAD.")

        print("\nSample rows:")
        working.show(8, truncate=False)

        working.coalesce(1).write.mode("overwrite").parquet(BADR_VALEUR_PATH)
        print(f"\nWritten to {BADR_VALEUR_PATH}")

        # Confirm Silver BADR itself was only read, never touched by this job.
        badr_recount = spark.read.parquet(SILVER_BADR_PATH).count()
        print(f"\nSilver BADR row count after this job (must still be {n_badr}): {badr_recount}")
        if badr_recount != n_badr:
            raise ValueError("Silver BADR row count changed - it must never be written to by this job.")

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Silver BADR: {n_badr} rows (untouched) -> badr_valeur working view: {n_working} rows, "
              f"VALEUR_MAD populated for {n_valeur_mad_non_null} rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
