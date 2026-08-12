"""Bronze -> Silver: read the two raw Parquet sources from MinIO, apply only
technical quality/standardization transformations (types, trimming, null and
duplicate checks), and write the result back to MinIO as Parquet.

This is deliberately NOT the matching/valuation stage: no match_score,
prix_reference, ratio or NORMAL/MINORE/MAJORE classification is computed
here. The BADR schema (columns and names) is the one handed over by ADII and
is kept as-is - no renaming, no invented business columns.

Run with (see the exact command used in the Phase 2.4 report):

    spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
        /home/iceberg/jobs/bronze_to_silver.py

Why --packages instead of baking jars into the image: the spark-iceberg base
image ships Iceberg's own S3 support (iceberg-aws-bundle) for Iceberg
tables/catalog, but not the generic Hadoop `s3a://` filesystem connector
needed to read/write plain Parquet files with Spark's DataFrameReader. Adding
it at spark-submit time avoids touching the Dockerfile/image for this phase.

Idempotence: both Silver datasets are written with `.mode("overwrite")` to a
fixed path, so a rerun replaces the previous output directory's contents
instead of accumulating files. Each dataset is `.coalesce(1)` before writing
since MVP volumes are tiny (5000 and 58 rows) - this keeps the output to one
clean part-file per dataset rather than a scattered multi-partition layout.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType

BRONZE_BADR_PATH = "s3a://datalake/bronze/badr/badr.parquet"
BRONZE_SCRAPING_PATH = "s3a://datalake/bronze/scraping/prix_web.parquet"
SILVER_BADR_PATH = "s3a://datalake/silver/badr/"
SILVER_SCRAPING_PATH = "s3a://datalake/silver/scraping/"

EXPECTED_BADR_ROWS = 5000
EXPECTED_SCRAPING_ROWS = 58


def null_report(df, label):
    total = df.count()
    print(f"\n--- {label}: NULL count per column (of {total} rows) ---")
    agg_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns
    ]
    row = df.agg(*agg_exprs).collect()[0].asDict()
    for col_name, null_count in row.items():
        pct = (null_count / total * 100) if total else 0
        print(f"  {col_name}: {null_count} ({pct:.1f}%)")


def process_badr(spark):
    print("\n" + "=" * 60)
    print("BADR: Bronze -> Silver")
    print("=" * 60)

    bronze = spark.read.parquet(BRONZE_BADR_PATH)
    print("\nBronze schema:")
    bronze.printSchema()
    bronze_count = bronze.count()
    print(f"Bronze row count: {bronze_count}")
    if bronze_count != EXPECTED_BADR_ROWS:
        print(
            f"NOTE: Bronze BADR has {bronze_count} rows, expected ~{EXPECTED_BADR_ROWS}. "
            "Not overwritten/invented - carrying the real count through."
        )

    # Technical standardization only - same columns, same meaning as the
    # customs-provided view. `id` is the only column not part of that view;
    # it is the SQLite surrogate primary key already present in Bronze
    # (kept as a technical row identifier, not a new business column).
    silver = (
        bronze.withColumn("id", F.col("id").cast(LongType()))
        .withColumn("DATE_DEPOT", F.to_date(F.trim(F.col("DATE_DEPOT")), "yyyy-MM-dd"))
        .withColumn("VALEUR_INITIALE", F.col("VALEUR_INITIALE").cast(DecimalType(12, 2)))
        .withColumn("VALEUR", F.col("VALEUR").cast(DecimalType(12, 2)))
        .withColumn("POIDS", F.col("POIDS").cast(DecimalType(12, 3)))
        .withColumn("POIDS_INITIAL", F.col("POIDS_INITIAL").cast(DecimalType(12, 3)))
        .withColumn("CODE_NGP", F.trim(F.col("CODE_NGP")))
        .withColumn("CODE_NGP_INITIAL", F.trim(F.col("CODE_NGP_INITIAL")))
        .withColumn("PAYS", F.trim(F.col("PAYS")))
        .withColumn("DEVISE", F.trim(F.col("DEVISE")))
    )

    print("\nSilver schema:")
    silver.printSchema()

    null_report(silver, "BADR Silver")

    business_cols = [c for c in silver.columns if c != "id"]
    dup_count = (
        silver.groupBy(business_cols).count().filter(F.col("count") > 1).agg(F.sum("count")).collect()[0][0]
        or 0
    )
    print(f"\nBADR: rows involved in a full business-column duplicate (excluding technical id): {dup_count}")
    print(
        "Decision: not dropping any - with synthetic Faker data, an incidental full match across "
        "9 independent fields is not on its own proof of a true duplicate declaration, and the "
        "task asks not to silently drop rows that aren't clearly identifiable as duplicates."
    )

    silver_count = silver.count()
    print(f"\nBADR: Bronze rows={bronze_count}, Silver rows={silver_count} (no rows dropped)")

    print("\nSample rows:")
    silver.show(10, truncate=False)

    silver.coalesce(1).write.mode("overwrite").parquet(SILVER_BADR_PATH)
    print(f"Written to {SILVER_BADR_PATH}")

    return bronze_count, silver_count


def process_scraping(spark):
    print("\n" + "=" * 60)
    print("SCRAPING: Bronze -> Silver")
    print("=" * 60)

    bronze = spark.read.parquet(BRONZE_SCRAPING_PATH)
    print("\nBronze schema:")
    bronze.printSchema()
    bronze_count = bronze.count()
    print(f"Bronze row count: {bronze_count}")
    if bronze_count != EXPECTED_SCRAPING_ROWS:
        print(
            f"NOTE: Bronze scraping has {bronze_count} rows, expected ~{EXPECTED_SCRAPING_ROWS}. "
            "Not overwritten/invented - carrying the real count through."
        )

    # Technical standardization: trim text columns, type the price as a
    # proper decimal (never mixed with the currency symbol), type the
    # scraping date. marque/modele/categorie are trimmed but kept in their
    # original casing - a lowercased/regex-stripped matching KEY belongs to
    # the future matching stage as a derived key, not to Silver's canonical
    # human-readable value.
    silver = (
        bronze.withColumn("marque", F.trim(F.col("marque")))
        .withColumn("modele", F.trim(F.col("modele")))
        .withColumn("prix", F.col("prix").cast(DecimalType(10, 2)))
        .withColumn("devise", F.upper(F.trim(F.col("devise"))))
        .withColumn("type_prix", F.trim(F.col("type_prix")))
        .withColumn("site_source", F.trim(F.col("site_source")))
        .withColumn("url", F.trim(F.col("url")))
        .withColumn("date_scraping", F.to_date(F.trim(F.col("date_scraping")), "yyyy-MM-dd"))
        .withColumn("categorie", F.trim(F.col("categorie")))
    )

    print("\nSilver schema:")
    silver.printSchema()

    null_report(silver, "Scraping Silver")

    invalid_url_count = silver.filter(
        F.col("url").isNull() | (~F.col("url").startswith("https://www.jumia.ma"))
    ).count()
    print(f"\nScraping: rows with null/unexpected URL: {invalid_url_count} (not dropped, just reported)")

    invalid_price_count = silver.filter(F.col("prix").isNull() | (F.col("prix") <= 0)).count()
    print(f"Scraping: rows with null/non-positive price: {invalid_price_count} (not dropped, just reported)")

    dup_url_count = (
        silver.groupBy("url").count().filter(F.col("count") > 1).agg(F.sum("count")).collect()[0][0] or 0
    )
    print(f"Scraping: rows sharing a duplicate URL: {dup_url_count}")

    silver_count = silver.count()
    print(f"\nScraping: Bronze rows={bronze_count}, Silver rows={silver_count} (no rows dropped)")

    print("\nSample rows:")
    silver.show(10, truncate=False)

    silver.coalesce(1).write.mode("overwrite").parquet(SILVER_SCRAPING_PATH)
    print(f"Written to {SILVER_SCRAPING_PATH}")

    return bronze_count, silver_count


def main():
    spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        badr_bronze_n, badr_silver_n = process_badr(spark)
        scraping_bronze_n, scraping_silver_n = process_scraping(spark)

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"BADR:     Bronze={badr_bronze_n}  Silver={badr_silver_n}")
        print(f"Scraping: Bronze={scraping_bronze_n}  Silver={scraping_silver_n}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
