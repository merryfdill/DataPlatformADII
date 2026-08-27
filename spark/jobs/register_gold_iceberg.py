"""Register the existing Spark Gold arbitrage output as an Iceberg table
(Phase 2.22), so Trino/dbt can actually see it.

Phase 2.21's arbitrage_gold.py wrote s3a://datalake/gold/arbitrage/ as plain
Parquet - fine for Spark-to-Spark reads, but NOT visible to Trino: Trino's
only configured catalog (infrastructure/trino/catalog/iceberg.properties) is
an Iceberg REST catalog, which can only see tables it manages via Iceberg
metadata, not an arbitrary Parquet directory. This script does NOT recompute
anything - it reads the existing, already-validated Gold Parquet as-is and
writes it, unchanged, as the Iceberg table iceberg.gold.arbitrage, using the
Spark Iceberg catalog that is already fully configured in
spark/conf/spark-defaults.conf (spark.sql.catalog.iceberg -> the same REST
catalog + MinIO warehouse Trino itself uses). No Docker/infrastructure
change - this exercises wiring that was already present but never used by
any earlier phase (every prior Spark job wrote plain Parquet).

Run with (same flags as the project's other Spark jobs - still needed to
read the SOURCE parquet via s3a://; the Iceberg write side uses the
already-configured Iceberg catalog, no extra packages needed for that part):

    spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
        /home/iceberg/jobs/register_gold_iceberg.py

Idempotence: createOrReplace() on a fixed table name - a rerun replaces the
table's content with the current Parquet content, no accumulation.
"""

from pyspark.sql import SparkSession

GOLD_PARQUET_PATH = "s3a://datalake/gold/arbitrage/"
ICEBERG_TABLE = "iceberg.gold.arbitrage"


def main():
    spark = SparkSession.builder.appName("register_gold_iceberg").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=" * 60)
        print("REGISTER_GOLD_ICEBERG: Parquet (unchanged) -> Iceberg table")
        print("=" * 60)

        df = spark.read.parquet(GOLD_PARQUET_PATH)
        n = df.count()
        print(f"\nRead {n} rows from {GOLD_PARQUET_PATH} (read-only, not recomputed)")
        print("Schema:")
        df.printSchema()

        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")
        df.writeTo(ICEBERG_TABLE).createOrReplace()
        print(f"\nWritten to Iceberg table {ICEBERG_TABLE}")

        reread = spark.table(ICEBERG_TABLE)
        n_reread = reread.count()
        print(f"Re-read from {ICEBERG_TABLE}: {n_reread} rows (must equal {n})")
        if n_reread != n:
            raise ValueError("Row count mismatch after Iceberg registration.")

        print("\nSample rows from the Iceberg table:")
        reread.orderBy("BADR_ID").show(5, truncate=False)

        # Confirm the original Parquet source was only read, never modified.
        n_source_after = spark.read.parquet(GOLD_PARQUET_PATH).count()
        print(f"\nOriginal Gold Parquet row count after this job (must still be {n}): {n_source_after}")
        if n_source_after != n:
            raise ValueError("Source Gold Parquet row count changed - it must never be written to by this job.")

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"{n} rows registered as {ICEBERG_TABLE} - source Parquet unchanged, no business logic recomputed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
