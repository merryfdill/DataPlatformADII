"""VALEUR_UNITAIRE_MAD and RATIO_UNITAIRE, per BADR declaration (Phase 2.20).

Phase 2.17 computed RATIO = VALEUR_MAD / PRIX_REFERENCE. Phase 2.18's
diagnostic showed this was not economically interpretable: VALEUR_MAD is a
declaration-level (multi-unit) value, while PRIX_REFERENCE is a unit retail
price - dividing one by the other conflates shipment size with valuation.
Phase 2.19 added QUANTITE to BADR to fix exactly this granularity mismatch.
This job computes the corrected, unit-level ratio:

    VALEUR_UNITAIRE_MAD = VALEUR_MAD / QUANTITE
    RATIO_UNITAIRE      = VALEUR_UNITAIRE_MAD / PRIX_REFERENCE
                         = (VALEUR_MAD / QUANTITE) / PRIX_REFERENCE

The OLD ratio (s3a://datalake/silver/badr_ratio/, Phase 2.17) is read only
for a side-by-side comparison in the printed report - it is NOT
overwritten, and stays available as the Phase 2.17 historical artifact.

Sources (all read-only):
  - s3a://datalake/silver/badr_valeur/ (Phase 2.16, refreshed here in Phase
    2.20 to carry QUANTITE - see spark/jobs/badr_valeur_prep.py)
  - s3a://datalake/silver/reference/ngp_code_normalization.parquet
    (Phase 2.13 - BADR.CODE_NGP is normalized before matching, exactly as
    Phase 2.17 already does; not re-derived here, same reference table)
  - s3a://datalake/silver/reference/prix_reference/ (Phase 2.14, from the
    scraping - NOT recomputed here, Jumia is never touched by this job)

This job does NOT define NORMAL/MINORE/MAJORE, does NOT pick a threshold,
and does NOT create Gold - it only computes RATIO_UNITAIRE and describes its
distribution. "Valeurs aberrantes" below uses Tukey's 1.5xIQR rule (same
descriptive-statistics convention as Phase 2.17), not a customs threshold.

Run with (same flags as the project's other Spark jobs):

    spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
        /home/iceberg/jobs/ratio_unitaire.py

Idempotence: `.mode("overwrite")` on fixed, NEW paths (badr_ratio_unitaire*),
coalesced to 1 file each - a rerun replaces only this job's own output.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

BADR_VALEUR_PATH = "s3a://datalake/silver/badr_valeur/"
NGP_NORMALIZATION_PATH = "s3a://datalake/silver/reference/ngp_code_normalization.parquet"
PRIX_REFERENCE_PATH = "s3a://datalake/silver/reference/prix_reference/"
OLD_BADR_RATIO_PATH = "s3a://datalake/silver/badr_ratio/"  # Phase 2.17, read-only, for comparison only

BADR_RATIO_UNITAIRE_PATH = "s3a://datalake/silver/badr_ratio_unitaire/"
BADR_RATIO_UNITAIRE_STATS_PATH = "s3a://datalake/silver/badr_ratio_unitaire_stats/"


def compute_stats(df, value_col, group_col=None):
    grouped = df.groupBy(group_col) if group_col else df.groupBy(F.lit("ALL").alias("CODE_NGP"))
    stats = grouped.agg(
        F.count(value_col).alias("NB_LIGNES"),
        F.min(value_col).alias("MIN"),
        F.expr(f"percentile({value_col}, 0.25)").alias("Q1"),
        F.expr(f"percentile({value_col}, 0.5)").alias("MEDIAN"),
        F.avg(value_col).alias("MOYEN"),
        F.expr(f"percentile({value_col}, 0.75)").alias("Q3"),
        F.max(value_col).alias("MAX"),
        F.stddev(value_col).alias("ECART_TYPE"),
    )
    stats = stats.withColumn("IQR", F.col("Q3") - F.col("Q1"))
    stats = stats.withColumn("BORNE_BASSE_TUKEY", F.col("Q1") - 1.5 * F.col("IQR"))
    stats = stats.withColumn("BORNE_HAUTE_TUKEY", F.col("Q3") + 1.5 * F.col("IQR"))
    return stats


def count_outliers(df, value_col, bounds_row):
    low, high = bounds_row["BORNE_BASSE_TUKEY"], bounds_row["BORNE_HAUTE_TUKEY"]
    return df.filter((F.col(value_col) < low) | (F.col(value_col) > high)).count()


def main():
    spark = SparkSession.builder.appName("ratio_unitaire").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=" * 60)
        print("RATIO_UNITAIRE: VALEUR_UNITAIRE_MAD + RATIO_UNITAIRE")
        print("=" * 60)

        badr_valeur = spark.read.parquet(BADR_VALEUR_PATH)
        n_badr_valeur = badr_valeur.count()
        print(f"\nbadr_valeur row count (read-only source): {n_badr_valeur}")
        print("Schema:")
        badr_valeur.printSchema()
        if "QUANTITE" not in badr_valeur.columns:
            raise ValueError("QUANTITE not found in badr_valeur - refresh badr_valeur_prep.py output first.")

        normalization = spark.read.parquet(NGP_NORMALIZATION_PATH)
        prix_reference = spark.read.parquet(PRIX_REFERENCE_PATH)
        print("\nprix_reference (Phase 2.14, read-only, NOT recomputed here):")
        prix_reference.show(truncate=False)

        # --- Step 1: normalize BADR.CODE_NGP (same table/rule as Phase 2.13/2.17) ---
        normalized = (
            badr_valeur.join(normalization, badr_valeur["CODE_NGP"] == normalization["ancien_code"], "left")
            .withColumn("CODE_NGP_NORMALISE", F.coalesce(F.col("code_normalise"), badr_valeur["CODE_NGP"]))
            .select(
                badr_valeur["id"].alias("BADR_ID"), badr_valeur["DATE_DEPOT"], "CODE_NGP_NORMALISE",
                badr_valeur["VALEUR_MAD"], badr_valeur["QUANTITE"], badr_valeur["POIDS"], badr_valeur["DEVISE"],
            )
        )

        # --- Step 2: join to prix_reference on the normalized code ---
        joined = normalized.join(
            prix_reference, normalized["CODE_NGP_NORMALISE"] == prix_reference["CODE_NGP"], "left"
        ).select(
            normalized["BADR_ID"], normalized["DATE_DEPOT"],
            normalized["CODE_NGP_NORMALISE"].alias("CODE_NGP"),
            normalized["VALEUR_MAD"], normalized["QUANTITE"], normalized["POIDS"], normalized["DEVISE"],
            prix_reference["PRIX_MEDIAN"].alias("PRIX_REFERENCE"),
        )

        n_joined = joined.count()
        print(f"\nLignes apres jointure (avant filtre): {n_joined} (doit egaler {n_badr_valeur})")
        if n_joined != n_badr_valeur:
            raise ValueError(f"Row count changed during join: {n_badr_valeur} -> {n_joined}")

        # --- Step 3: pre-filter validations (counted and reported, not silently dropped) ---
        n_valeur_mad_null = joined.filter(F.col("VALEUR_MAD").isNull()).count()
        n_quantite_null = joined.filter(F.col("QUANTITE").isNull()).count()
        n_quantite_non_positive = joined.filter(F.col("QUANTITE").isNotNull() & (F.col("QUANTITE") <= 0)).count()
        n_prix_ref_null = joined.filter(F.col("PRIX_REFERENCE").isNull()).count()
        n_prix_ref_non_positive = joined.filter(
            F.col("PRIX_REFERENCE").isNotNull() & (F.col("PRIX_REFERENCE") <= 0)
        ).count()
        n_code_ngp_null = joined.filter(F.col("CODE_NGP").isNull()).count()

        print("\n--- Verifications avant calcul (aucune division par zero possible) ---")
        print(f"VALEUR_MAD NULL: {n_valeur_mad_null} (attendu 0)")
        print(f"QUANTITE NULL: {n_quantite_null} (attendu 0)")
        print(f"QUANTITE <= 0: {n_quantite_non_positive} (attendu 0 - CHECK QUANTITE >= 1 en base)")
        print(f"PRIX_REFERENCE NULL: {n_prix_ref_null} (attendu ~4629 - codes BADR hors perimetre scraping, cf. Phase 2.13/2.17)")
        print(f"PRIX_REFERENCE <= 0: {n_prix_ref_non_positive} (attendu 0 - deja valide en Phase 2.14)")
        print(f"CODE_NGP NULL: {n_code_ngp_null} (attendu 0)")

        # --- Step 4: filter (justified, documented - not arbitrary) ---
        valid = joined.filter(
            F.col("VALEUR_MAD").isNotNull()
            & F.col("QUANTITE").isNotNull() & (F.col("QUANTITE") > 0)
            & F.col("PRIX_REFERENCE").isNotNull() & (F.col("PRIX_REFERENCE") > 0)
            & F.col("CODE_NGP").isNotNull()
        )
        n_valid = valid.count()
        n_excluded = n_joined - n_valid
        print(f"\nLignes retenues pour le calcul: {n_valid} / {n_joined} "
              f"({n_excluded} exclues - toutes pour cause de PRIX_REFERENCE absent, "
              "CODE_NGP hors des 3 categories scrapees, cf. Phase 2.13)")

        # --- Step 5/6: VALEUR_UNITAIRE_MAD and RATIO_UNITAIRE, full precision, no premature rounding ---
        result = (
            valid.withColumn("VALEUR_UNITAIRE_MAD", F.col("VALEUR_MAD").cast(DecimalType(28, 10)) / F.col("QUANTITE"))
            .withColumn("RATIO_UNITAIRE", F.col("VALEUR_UNITAIRE_MAD") / F.col("PRIX_REFERENCE"))
            .select(
                "BADR_ID", "CODE_NGP", "DEVISE", "DATE_DEPOT", "QUANTITE",
                "VALEUR_MAD", "VALEUR_UNITAIRE_MAD", "PRIX_REFERENCE", "RATIO_UNITAIRE", "POIDS",
            )
        )

        print("\nRepartition des lignes retenues par CODE_NGP:")
        result.groupBy("CODE_NGP").count().orderBy("CODE_NGP").show()

        # --- Step 7: coherence checks on the final table ---
        n_qty_bad = result.filter(F.col("QUANTITE") < 1).count()
        n_valeur_mad_bad = result.filter(F.col("VALEUR_MAD") <= 0).count()
        n_vum_bad = result.filter(F.col("VALEUR_UNITAIRE_MAD") <= 0).count()
        n_pref_bad = result.filter(F.col("PRIX_REFERENCE") <= 0).count()
        n_ratio_bad = result.filter(F.col("RATIO_UNITAIRE") <= 0).count()
        print("\n--- Verification de coherence (table finale) ---")
        print(f"QUANTITE < 1: {n_qty_bad} (doit etre 0)")
        print(f"VALEUR_MAD <= 0: {n_valeur_mad_bad} (doit etre 0)")
        print(f"VALEUR_UNITAIRE_MAD <= 0: {n_vum_bad} (doit etre 0)")
        print(f"PRIX_REFERENCE <= 0: {n_pref_bad} (doit etre 0)")
        print(f"RATIO_UNITAIRE <= 0: {n_ratio_bad} (doit etre 0)")
        if any([n_qty_bad, n_valeur_mad_bad, n_vum_bad, n_pref_bad, n_ratio_bad]):
            raise ValueError("Coherence check failed on the final ratio_unitaire table - see counts above.")

        # --- Step 8: manual verification (>= 3 rows) ---
        print("\n--- Verification manuelle (>= 3 lignes) ---")
        sample = result.orderBy("BADR_ID").limit(5).collect()
        all_ok = True
        for r in sample:
            expected_vum = float(r["VALEUR_MAD"]) / float(r["QUANTITE"])
            expected_ratio = expected_vum / float(r["PRIX_REFERENCE"])
            ok_vum = abs(expected_vum - float(r["VALEUR_UNITAIRE_MAD"])) < 1e-6
            ok_ratio = abs(expected_ratio - float(r["RATIO_UNITAIRE"])) < 1e-9
            all_ok = all_ok and ok_vum and ok_ratio
            print(f"  BADR_ID={r['BADR_ID']} CODE_NGP={r['CODE_NGP']} VALEUR_MAD={r['VALEUR_MAD']} "
                  f"QUANTITE={r['QUANTITE']} -> VALEUR_UNITAIRE_MAD attendu={expected_vum:.6f} "
                  f"obtenu={float(r['VALEUR_UNITAIRE_MAD']):.6f} OK={ok_vum} | "
                  f"PRIX_REFERENCE={r['PRIX_REFERENCE']} -> RATIO_UNITAIRE attendu={expected_ratio:.6f} "
                  f"obtenu={float(r['RATIO_UNITAIRE']):.6f} OK={ok_ratio}")
        if not all_ok:
            raise ValueError("Manual verification failed for at least one sample row.")

        # --- Step 9: statistics (global + per CODE_NGP) ---
        overall_ratio_stats = compute_stats(result, "RATIO_UNITAIRE").collect()[0]
        n_outliers_overall = count_outliers(result, "RATIO_UNITAIRE", overall_ratio_stats)
        print("\n--- Statistiques RATIO_UNITAIRE (global) ---")
        print(overall_ratio_stats)
        print(f"Valeurs aberrantes (Tukey 1.5xIQR): {n_outliers_overall}")

        overall_vum_stats = compute_stats(result, "VALEUR_UNITAIRE_MAD").collect()[0]
        print("\n--- Statistiques VALEUR_UNITAIRE_MAD (global) ---")
        print(overall_vum_stats)

        per_code_ratio_stats = compute_stats(result, "RATIO_UNITAIRE", "CODE_NGP")
        per_code_ratio_rows = per_code_ratio_stats.collect()
        print("\n--- Statistiques RATIO_UNITAIRE par CODE_NGP ---")
        per_code_ratio_stats.show(truncate=False)

        per_code_vum_stats = compute_stats(result, "VALEUR_UNITAIRE_MAD", "CODE_NGP").collect()
        vum_by_code = {r["CODE_NGP"]: r for r in per_code_vum_stats}
        print("\n--- Statistiques VALEUR_UNITAIRE_MAD par CODE_NGP ---")
        for r in per_code_vum_stats:
            print(f"  {r['CODE_NGP']}: min={r['MIN']:.2f} median={r['MEDIAN']:.2f} mean={r['MOYEN']:.2f} max={r['MAX']:.2f}")

        outlier_counts = {}
        for row in per_code_ratio_rows:
            code = row["CODE_NGP"]
            n_out = count_outliers(result.filter(F.col("CODE_NGP") == code), "RATIO_UNITAIRE", row)
            outlier_counts[code] = n_out
            print(f"  {code}: {n_out} valeurs aberrantes (Tukey, sur {row['NB_LIGNES']} lignes)")

        # --- Step 10: build the stats output table (RATIO_UNITAIRE stats + PRIX_REFERENCE per code) ---
        prix_ref_by_code = {r["CODE_NGP"]: r["PRIX_MEDIAN"] for r in prix_reference.collect()}
        stats_rows = [{
            "CODE_NGP": "ALL",
            "NB_LIGNES": overall_ratio_stats["NB_LIGNES"],
            "PRIX_REFERENCE": None,
            "VALEUR_UNITAIRE_MIN": overall_vum_stats["MIN"],
            "VALEUR_UNITAIRE_MEDIAN": overall_vum_stats["MEDIAN"],
            "VALEUR_UNITAIRE_MOYEN": overall_vum_stats["MOYEN"],
            "VALEUR_UNITAIRE_MAX": overall_vum_stats["MAX"],
            "RATIO_MIN": overall_ratio_stats["MIN"],
            "RATIO_Q1": overall_ratio_stats["Q1"],
            "RATIO_MEDIAN": overall_ratio_stats["MEDIAN"],
            "RATIO_MOYEN": overall_ratio_stats["MOYEN"],
            "RATIO_Q3": overall_ratio_stats["Q3"],
            "RATIO_MAX": overall_ratio_stats["MAX"],
            "RATIO_ECART_TYPE": overall_ratio_stats["ECART_TYPE"],
            "NB_VALEURS_ABERRANTES": n_outliers_overall,
        }]
        for row in per_code_ratio_rows:
            code = row["CODE_NGP"]
            vum = vum_by_code[code]
            stats_rows.append({
                "CODE_NGP": code,
                "NB_LIGNES": row["NB_LIGNES"],
                "PRIX_REFERENCE": float(prix_ref_by_code.get(code)) if prix_ref_by_code.get(code) is not None else None,
                "VALEUR_UNITAIRE_MIN": vum["MIN"],
                "VALEUR_UNITAIRE_MEDIAN": vum["MEDIAN"],
                "VALEUR_UNITAIRE_MOYEN": vum["MOYEN"],
                "VALEUR_UNITAIRE_MAX": vum["MAX"],
                "RATIO_MIN": row["MIN"],
                "RATIO_Q1": row["Q1"],
                "RATIO_MEDIAN": row["MEDIAN"],
                "RATIO_MOYEN": row["MOYEN"],
                "RATIO_Q3": row["Q3"],
                "RATIO_MAX": row["MAX"],
                "RATIO_ECART_TYPE": row["ECART_TYPE"],
                "NB_VALEURS_ABERRANTES": outlier_counts[code],
            })
        stats_table = spark.createDataFrame(stats_rows)

        # --- Step 11: compare with the OLD Phase 2.17 ratio (read-only, not overwritten) ---
        print("\n--- Comparaison avec l'ancien RATIO (Phase 2.17, VALEUR_MAD / PRIX_REFERENCE) ---")
        old_ratio = spark.read.parquet(OLD_BADR_RATIO_PATH).select(
            F.col("BADR_ID"), F.col("RATIO").alias("ANCIEN_RATIO")
        )
        comparison = result.join(old_ratio, on="BADR_ID", how="inner").select(
            "BADR_ID", "CODE_NGP", "QUANTITE", "VALEUR_MAD", "VALEUR_UNITAIRE_MAD",
            "PRIX_REFERENCE", "ANCIEN_RATIO", "RATIO_UNITAIRE",
        )
        n_comparable = comparison.count()
        print(f"Lignes comparables (presentes dans l'ancien ET le nouveau calcul): {n_comparable} "
              "(NOTE: l'ancien ratio a ete calcule AVANT la regeneration BADR de la Phase 2.19, donc "
              "VALEUR_MAD/BADR_ID peuvent ne plus correspondre exactement aux memes montants - "
              "comparaison conceptuelle/illustrative, pas une reconciliation ligne a ligne stricte)")
        print("\nExemples avec un ecart important entre ancien RATIO et nouveau RATIO_UNITAIRE:")
        comparison.withColumn("ECART_ABSOLU", F.abs(F.col("ANCIEN_RATIO") - F.col("RATIO_UNITAIRE"))) \
            .orderBy(F.desc("ECART_ABSOLU")).show(10, truncate=False)

        for code in ["85171300", "84713000", "85287200"]:
            print(f"\nExemples reels pour {code}:")
            result.filter(F.col("CODE_NGP") == code).orderBy("BADR_ID").select(
                "BADR_ID", "CODE_NGP", "QUANTITE", "VALEUR_MAD", "VALEUR_UNITAIRE_MAD", "PRIX_REFERENCE", "RATIO_UNITAIRE"
            ).show(3, truncate=False)

        # --- Step 12: write outputs (NEW paths - badr_ratio/ from Phase 2.17 is untouched) ---
        result.coalesce(1).write.mode("overwrite").parquet(BADR_RATIO_UNITAIRE_PATH)
        print(f"\nWritten {n_valid} rows to {BADR_RATIO_UNITAIRE_PATH}")

        stats_table.coalesce(1).write.mode("overwrite").parquet(BADR_RATIO_UNITAIRE_STATS_PATH)
        print(f"Written {stats_table.count()} rows to {BADR_RATIO_UNITAIRE_STATS_PATH}")

        # --- Step 13: confirm read-only sources untouched ---
        n_badr_valeur_after = spark.read.parquet(BADR_VALEUR_PATH).count()
        print(f"\nbadr_valeur row count after this job (must still be {n_badr_valeur}): {n_badr_valeur_after}")
        if n_badr_valeur_after != n_badr_valeur:
            raise ValueError("badr_valeur row count changed - it must never be written to by this job.")

        old_ratio_count_after = spark.read.parquet(OLD_BADR_RATIO_PATH).count()
        print(f"Ancien badr_ratio (Phase 2.17) row count after this job (doit rester inchange): {old_ratio_count_after}")

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"badr_valeur: {n_badr_valeur} rows (untouched) -> badr_ratio_unitaire: {n_valid} rows "
              f"({n_excluded} excluded, no PRIX_REFERENCE for their CODE_NGP)")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
