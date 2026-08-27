"""RATIO = VALEUR_MAD / PRIX_REFERENCE, per BADR declaration (Phase 2.17).

Reads three existing Silver datasets - never writes to any of them:
  - s3a://datalake/silver/badr_valeur/        (Phase 2.16, 5000 rows: BADR
    declarations with VALEUR_MAD already converted, CODE_NGP still the
    RAW/un-normalized BADR code)
  - s3a://datalake/silver/reference/ngp_code_normalization.parquet
    (Phase 2.13, ancien_code -> code_normalise, e.g. 85171200 -> 85171300)
  - s3a://datalake/silver/reference/prix_reference/ (Phase 2.14, one row per
    normalized CODE_NGP: NB_PRODUITS, PRIX_MIN, PRIX_MEDIAN, PRIX_MOYEN,
    PRIX_MAX, DEVISE_REFERENCE)

Reconciliation key: exactly as in Phase 2.13's matching, BADR.CODE_NGP is
normalized first (identity if the code isn't in the normalization table -
same rule as ingestion/ml/ngp_normalization.py), THEN joined against
prix_reference on the normalized code. This is why the normalization
reference table is read here instead of re-deriving/duplicating the mapping
in this job.

PRIX_REFERENCE (the denominator) = PRIX_MEDIAN from the Phase 2.14 table.
The median was chosen as "the" reference price because it is the standard,
outlier-robust reference statistic (already the one Phase 2.14 highlighted
first) - not a new business decision made here. This choice is documented,
not silently assumed.

Only BADR declarations whose normalized CODE_NGP is one of the 3 scraped
categories (85171300/84713000/85287200) have a PRIX_REFERENCE at all - the
other 31 BADR codes are structurally out of the scraping's scope (Phase
2.13 finding) and are excluded by the required filter (PRIX_REFERENCE not
null), not by an arbitrary drop.

This job does NOT define NORMAL/MINORE/MAJORE and does NOT invent a
threshold for that - it only computes the ratio and its distribution.
"Valeurs aberrantes" below uses Tukey's 1.5xIQR rule, a standard descriptive
-statistics convention for flagging outliers, not a customs business
threshold.

Run with (same flags as the project's other Spark jobs):

    spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
        /home/iceberg/jobs/ratio_valorisation.py

Idempotence: `.mode("overwrite")` on fixed paths, coalesced to 1 file each
(volumes are small) - a rerun replaces the previous output.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BADR_VALEUR_PATH = "s3a://datalake/silver/badr_valeur/"
NGP_NORMALIZATION_PATH = "s3a://datalake/silver/reference/ngp_code_normalization.parquet"
PRIX_REFERENCE_PATH = "s3a://datalake/silver/reference/prix_reference/"
BADR_RATIO_PATH = "s3a://datalake/silver/badr_ratio/"
BADR_RATIO_STATS_PATH = "s3a://datalake/silver/badr_ratio_stats/"


def compute_stats(df, group_col=None):
    """count/min/Q1/median/mean/Q3/max/null_count + Tukey 1.5xIQR outlier
    count on RATIO, overall (group_col=None) or per group_col value.
    """
    grouped = df.groupBy(group_col) if group_col else df.groupBy(F.lit("ALL").alias("CODE_NGP"))
    stats = grouped.agg(
        F.count("RATIO").alias("NB_LIGNES"),
        F.min("RATIO").alias("RATIO_MIN"),
        F.expr("percentile(RATIO, 0.25)").alias("RATIO_Q1"),
        F.expr("percentile(RATIO, 0.5)").alias("RATIO_MEDIAN"),
        F.avg("RATIO").alias("RATIO_MOYEN"),
        F.expr("percentile(RATIO, 0.75)").alias("RATIO_Q3"),
        F.max("RATIO").alias("RATIO_MAX"),
    )
    stats = stats.withColumn("IQR", F.col("RATIO_Q3") - F.col("RATIO_Q1"))
    stats = stats.withColumn("BORNE_BASSE_TUKEY", F.col("RATIO_Q1") - 1.5 * F.col("IQR"))
    stats = stats.withColumn("BORNE_HAUTE_TUKEY", F.col("RATIO_Q3") + 1.5 * F.col("IQR"))
    return stats


def count_outliers(df, bounds_row):
    low, high = bounds_row["BORNE_BASSE_TUKEY"], bounds_row["BORNE_HAUTE_TUKEY"]
    return df.filter((F.col("RATIO") < low) | (F.col("RATIO") > high)).count()


def main():
    spark = SparkSession.builder.appName("ratio_valorisation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=" * 60)
        print("RATIO_VALORISATION: badr_valeur + prix_reference -> RATIO")
        print("=" * 60)

        badr_valeur = spark.read.parquet(BADR_VALEUR_PATH)
        n_badr_valeur = badr_valeur.count()
        print(f"\nbadr_valeur row count (read-only source): {n_badr_valeur}")

        normalization = spark.read.parquet(NGP_NORMALIZATION_PATH)
        print("\nTable de normalisation NGP (Phase 2.13, read-only):")
        normalization.select("ancien_code", "code_normalise").show(truncate=False)

        prix_reference = spark.read.parquet(PRIX_REFERENCE_PATH)
        print("\nprix_reference (Phase 2.14, read-only):")
        prix_reference.show(truncate=False)

        # --- Step 1: normalize BADR.CODE_NGP exactly as Phase 2.13 does ---
        normalized = (
            badr_valeur.join(normalization, badr_valeur["CODE_NGP"] == normalization["ancien_code"], "left")
            .withColumn("CODE_NGP_NORMALISE", F.coalesce(F.col("code_normalise"), badr_valeur["CODE_NGP"]))
            .select(
                badr_valeur["id"], badr_valeur["DATE_DEPOT"], "CODE_NGP_NORMALISE",
                badr_valeur["VALEUR_MAD"], badr_valeur["DEVISE"],
            )
        )
        n_changed = normalized.filter(F.col("CODE_NGP_NORMALISE") != badr_valeur["CODE_NGP"]).count()
        print(f"\nLignes dont le CODE_NGP a change apres normalisation: {n_changed} (attendu: les 105 lignes 85171200 -> 85171300)")

        # --- Step 2: join to prix_reference on the normalized code ---
        joined = normalized.join(
            prix_reference,
            normalized["CODE_NGP_NORMALISE"] == prix_reference["CODE_NGP"],
            "left",
        ).select(
            normalized["id"].alias("BADR_ID"),
            normalized["DATE_DEPOT"],
            normalized["CODE_NGP_NORMALISE"].alias("CODE_NGP"),
            normalized["VALEUR_MAD"],
            prix_reference["PRIX_MEDIAN"].alias("PRIX_REFERENCE"),
            normalized["DEVISE"],
        )

        n_joined = joined.count()
        print(f"\nLignes apres jointure (avant filtre): {n_joined} (doit egaler {n_badr_valeur})")
        if n_joined != n_badr_valeur:
            raise ValueError(f"Row count changed during join: {n_badr_valeur} -> {n_joined}")

        # --- Step 3: pre-filter validation (counts, not silent drops) ---
        n_valeur_mad_null = joined.filter(F.col("VALEUR_MAD").isNull()).count()
        n_prix_ref_null = joined.filter(F.col("PRIX_REFERENCE").isNull()).count()
        n_prix_ref_non_positive = joined.filter(
            F.col("PRIX_REFERENCE").isNotNull() & (F.col("PRIX_REFERENCE") <= 0)
        ).count()
        print(f"\nVALEUR_MAD NULL: {n_valeur_mad_null} (attendu 0 - Phase 2.16 a converti les 5000 lignes)")
        print(f"PRIX_REFERENCE NULL: {n_prix_ref_null} (attendu ~4629 - codes BADR hors des 3 categories scrapees, "
              "voir Phase 2.13 : seuls 85171300/84713000/85287200 ont un prix de reference)")
        print(f"PRIX_REFERENCE <= 0: {n_prix_ref_non_positive} (attendu 0 - deja valide en Phase 2.14)")

        # --- Step 4: filter (justified, not arbitrary) + compute RATIO ---
        filtered = joined.filter(
            F.col("VALEUR_MAD").isNotNull()
            & F.col("PRIX_REFERENCE").isNotNull()
            & (F.col("PRIX_REFERENCE") > 0)
        ).withColumn("RATIO", F.col("VALEUR_MAD") / F.col("PRIX_REFERENCE"))

        n_filtered = filtered.count()
        print(f"\nLignes retenues pour le calcul du RATIO: {n_filtered} / {n_joined} "
              f"({n_joined - n_filtered} exclues, toutes pour cause de PRIX_REFERENCE absent - code NGP hors perimetre scraping)")

        result = filtered.select("BADR_ID", "CODE_NGP", "VALEUR_MAD", "PRIX_REFERENCE", "RATIO", "DEVISE", "DATE_DEPOT")

        print("\nRepartition des lignes retenues par CODE_NGP:")
        result.groupBy("CODE_NGP").count().orderBy("CODE_NGP").show()

        # --- Step 5: manual verification on a couple of rows ---
        print("\n--- Verification manuelle (quelques lignes) ---")
        sample = result.orderBy("BADR_ID").limit(3).collect()
        all_ok = True
        for r in sample:
            expected = float(r["VALEUR_MAD"]) / float(r["PRIX_REFERENCE"])
            actual = float(r["RATIO"])
            ok = abs(expected - actual) < 1e-9
            all_ok = all_ok and ok
            print(f"  BADR_ID={r['BADR_ID']} CODE_NGP={r['CODE_NGP']} VALEUR_MAD={r['VALEUR_MAD']} "
                  f"PRIX_REFERENCE={r['PRIX_REFERENCE']} -> RATIO attendu={expected:.6f} obtenu={actual:.6f} OK={ok}")
        if not all_ok:
            raise ValueError("Manual RATIO verification failed for at least one sample row.")

        # --- Step 6: statistics (overall + per CODE_NGP) ---
        overall_stats = compute_stats(result).collect()[0]
        n_outliers_overall = count_outliers(result, overall_stats)
        print("\n--- Statistiques RATIO (global) ---")
        print(overall_stats)
        print(f"Valeurs aberrantes (Tukey 1.5xIQR, methode statistique standard, pas un seuil metier): {n_outliers_overall}")

        per_code_stats_df = compute_stats(result, "CODE_NGP")
        per_code_rows = per_code_stats_df.collect()
        print("\n--- Statistiques RATIO par CODE_NGP ---")
        per_code_stats_df.show(truncate=False)

        outlier_counts = {}
        for row in per_code_rows:
            code = row["CODE_NGP"]
            n_out = count_outliers(result.filter(F.col("CODE_NGP") == code), row)
            outlier_counts[code] = n_out
            print(f"  {code}: {n_out} valeurs aberrantes (Tukey, sur {row['NB_LIGNES']} lignes)")

        stats_rows = [{
            "CODE_NGP": "ALL",
            "NB_LIGNES": overall_stats["NB_LIGNES"],
            "RATIO_MIN": overall_stats["RATIO_MIN"],
            "RATIO_Q1": overall_stats["RATIO_Q1"],
            "RATIO_MEDIAN": overall_stats["RATIO_MEDIAN"],
            "RATIO_MOYEN": overall_stats["RATIO_MOYEN"],
            "RATIO_Q3": overall_stats["RATIO_Q3"],
            "RATIO_MAX": overall_stats["RATIO_MAX"],
            "NB_VALEURS_ABERRANTES": n_outliers_overall,
        }]
        for row in per_code_rows:
            stats_rows.append({
                "CODE_NGP": row["CODE_NGP"],
                "NB_LIGNES": row["NB_LIGNES"],
                "RATIO_MIN": row["RATIO_MIN"],
                "RATIO_Q1": row["RATIO_Q1"],
                "RATIO_MEDIAN": row["RATIO_MEDIAN"],
                "RATIO_MOYEN": row["RATIO_MOYEN"],
                "RATIO_Q3": row["RATIO_Q3"],
                "RATIO_MAX": row["RATIO_MAX"],
                "NB_VALEURS_ABERRANTES": outlier_counts[row["CODE_NGP"]],
            })
        stats_table = spark.createDataFrame(stats_rows)

        n_ratio_null = result.filter(F.col("RATIO").isNull()).count()
        print(f"\nNULL dans RATIO (table finale): {n_ratio_null} (doit etre 0)")
        if n_ratio_null:
            raise ValueError("RATIO has NULL values in the final table - filter did not fully exclude them.")

        # --- Step 7: write ---
        result.coalesce(1).write.mode("overwrite").parquet(BADR_RATIO_PATH)
        print(f"\nWritten {n_filtered} rows to {BADR_RATIO_PATH}")

        stats_table.coalesce(1).write.mode("overwrite").parquet(BADR_RATIO_STATS_PATH)
        print(f"Written {stats_table.count()} rows to {BADR_RATIO_STATS_PATH}")

        # --- Step 8: confirm read-only sources untouched ---
        n_badr_valeur_after = spark.read.parquet(BADR_VALEUR_PATH).count()
        print(f"\nbadr_valeur row count after this job (must still be {n_badr_valeur}): {n_badr_valeur_after}")
        if n_badr_valeur_after != n_badr_valeur:
            raise ValueError("badr_valeur row count changed - it must never be written to by this job.")

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"badr_valeur: {n_badr_valeur} rows (untouched) -> badr_ratio: {n_filtered} rows "
              f"({n_joined - n_filtered} excluded, no PRIX_REFERENCE for their CODE_NGP)")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
