"""RATIO_UNITAIRE -> ARBITRAGE (NORMAL/MINORE/MAJORE) -> Gold (Phase 2.21).

This is a BUSINESS RULE, not a second ML model: no Isolation Forest, no
anomaly-detection classifier, no .joblib file. The thresholds are simple
per-CODE_NGP percentiles of RATIO_UNITAIRE, computed transparently by Spark
from the actual Silver data at run time - reproducible and explicable, not
hand-picked constants like "0.8/1.2".

Why per-category, not a single global threshold: the Phase 2.20 distribution
shows materially different RATIO_UNITAIRE medians by CODE_NGP (Televiseur
~0.74, Smartphone ~1.08, PC Portable ~1.46 - see docs/arbitrage_gold.md for
the full percentile table). A single global cutoff would systematically
over-flag one category and under-flag another for reasons that have nothing
to do with actual mis-valuation - it would just be measuring the category
difference. Per-CODE_NGP deciles correct for this.

Why deciles (P10/P90) rather than quartiles (P25/P75): a customs
risk-targeting rule that flags half of all traffic as "abnormal" (which
quartile banding would do, by construction) is not operationally useful.
Deciles flag a realistic minority (~10% low, ~10% high) for review while
still being a plain percentile rule, not an arbitrary pick.

    RATIO_UNITAIRE < P10(CODE_NGP)  -> MINORE
    P10 <= RATIO_UNITAIRE <= P90    -> NORMAL
    RATIO_UNITAIRE > P90(CODE_NGP)  -> MAJORE

IMPORTANT: BADR is simulated (Faker). These thresholds are prototype/
simulation thresholds derived from synthetic data - see docs/arbitrage_gold.md.
They are NOT an official ADII customs rule and must never be presented as one.

ASCII values (NORMAL/MINORE/MAJORE, no accents) are used for the ARBITRAGE
column to avoid encoding issues in Parquet/Trino/dbt downstream - documented
choice, not an oversight.

Sources (all read-only, none modified):
  - s3a://datalake/silver/badr_ratio_unitaire/ (Phase 2.20 - already the
    matched/filtered 338-row subset with a valid RATIO_UNITAIRE)
  - s3a://datalake/silver/badr/ (original Silver BADR - joined in only for
    PAYS, CODE_NGP_INITIAL and raw VALEUR, which badr_ratio_unitaire does not
    carry; no other prior job is modified to add these)

Run with (same flags as the project's other Spark jobs):

    spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
        /home/iceberg/jobs/arbitrage_gold.py

Idempotence: `.mode("overwrite")` on a fixed path, coalesced to 1 file - a
rerun replaces the previous Gold output.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BADR_RATIO_UNITAIRE_PATH = "s3a://datalake/silver/badr_ratio_unitaire/"
SILVER_BADR_PATH = "s3a://datalake/silver/badr/"
GOLD_ARBITRAGE_PATH = "s3a://datalake/gold/arbitrage/"

EXPECTED_CODES = {"85171300", "84713000", "85287200"}
LOW_PCT, HIGH_PCT = 0.10, 0.90


def main():
    spark = SparkSession.builder.appName("arbitrage_gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=" * 60)
        print("ARBITRAGE_GOLD: RATIO_UNITAIRE -> NORMAL/MINORE/MAJORE -> Gold")
        print("=" * 60)

        ratio_unitaire = spark.read.parquet(BADR_RATIO_UNITAIRE_PATH)
        n_in = ratio_unitaire.count()
        print(f"\nbadr_ratio_unitaire row count (read-only source): {n_in}")
        print("Schema:")
        ratio_unitaire.printSchema()

        # --- Sanity check: no CODE_NGP outside the 3 scraped categories ---
        found_codes = {r["CODE_NGP"] for r in ratio_unitaire.select("CODE_NGP").distinct().collect()}
        unexpected = found_codes - EXPECTED_CODES
        if unexpected:
            raise ValueError(f"Unexpected CODE_NGP found in badr_ratio_unitaire: {unexpected} - refusing to proceed.")
        print(f"CODE_NGP present: {sorted(found_codes)} (subset of {sorted(EXPECTED_CODES)}, confirmed)")

        n_ratio_null = ratio_unitaire.filter(F.col("RATIO_UNITAIRE").isNull()).count()
        print(f"RATIO_UNITAIRE NULL: {n_ratio_null} (attendu 0 - deja garanti par la Phase 2.20)")
        if n_ratio_null:
            raise ValueError("RATIO_UNITAIRE has NULL values - arbitrage cannot be computed for those rows.")

        # --- Step 1: per-CODE_NGP P10/P90 of RATIO_UNITAIRE, computed from the real data ---
        thresholds = (
            ratio_unitaire.groupBy("CODE_NGP")
            .agg(
                F.count("*").alias("N"),
                F.expr(f"percentile(RATIO_UNITAIRE, {LOW_PCT})").alias("SEUIL_MINORE"),
                F.expr(f"percentile(RATIO_UNITAIRE, {HIGH_PCT})").alias("SEUIL_MAJORE"),
                F.expr("percentile(RATIO_UNITAIRE, 0.5)").alias("MEDIANE"),
            )
            .orderBy("CODE_NGP")
        )
        print(f"\nSeuils d'arbitrage par CODE_NGP (P{int(LOW_PCT*100)}/P{int(HIGH_PCT*100)}, "
              "calcules dynamiquement a partir des donnees reelles - PROTOTYPE, donnees BADR simulees) :")
        thresholds.show(truncate=False)
        thresholds_rows = {r["CODE_NGP"]: r for r in thresholds.collect()}

        # --- Step 2: join in PAYS/CODE_NGP_INITIAL/VALEUR (raw) from Silver BADR ---
        badr = spark.read.parquet(SILVER_BADR_PATH).select(
            F.col("id").alias("BADR_ID"), "PAYS", "CODE_NGP_INITIAL", "VALEUR"
        )
        # Captured here (not hardcoded) for the Step 10 "source untouched" check
        # below - BADR is no longer a fixed 5000 rows (Etape 1 daily-simulation
        # phase appends to it), so the only correct baseline is what THIS job
        # itself saw when it read Silver BADR, not a stale constant.
        n_badr_before = badr.count()
        enriched = ratio_unitaire.join(badr, on="BADR_ID", how="left")
        n_enriched = enriched.count()
        print(f"\nLignes apres jointure PAYS/CODE_NGP_INITIAL/VALEUR: {n_enriched} (doit egaler {n_in})")
        if n_enriched != n_in:
            raise ValueError(f"Row count changed during enrichment join: {n_in} -> {n_enriched}")

        n_pays_null = enriched.filter(F.col("PAYS").isNull()).count()
        n_code_initial_null = enriched.filter(F.col("CODE_NGP_INITIAL").isNull()).count()
        n_valeur_null = enriched.filter(F.col("VALEUR").isNull()).count()
        print(f"PAYS NULL apres jointure: {n_pays_null} (attendu 0 - chaque BADR_ID existe dans Silver BADR)")
        print(f"CODE_NGP_INITIAL NULL apres jointure: {n_code_initial_null} (attendu 0)")
        print(f"VALEUR NULL apres jointure: {n_valeur_null} (attendu 0)")
        if n_pays_null or n_code_initial_null or n_valeur_null:
            raise ValueError("Enrichment join left unexpected NULLs - BADR_ID mismatch between sources.")

        # --- Step 3: apply the arbitrage rule (business rule, not ML) ---
        seuil_map_minore = F.create_map(*[
            x for code, r in thresholds_rows.items() for x in (F.lit(code), F.lit(float(r["SEUIL_MINORE"])))
        ])
        seuil_map_majore = F.create_map(*[
            x for code, r in thresholds_rows.items() for x in (F.lit(code), F.lit(float(r["SEUIL_MAJORE"])))
        ])

        gold = (
            enriched
            .withColumn("SEUIL_MINORE", seuil_map_minore[F.col("CODE_NGP")])
            .withColumn("SEUIL_MAJORE", seuil_map_majore[F.col("CODE_NGP")])
            .withColumn(
                "ARBITRAGE",
                F.when(F.col("RATIO_UNITAIRE") < F.col("SEUIL_MINORE"), F.lit("MINORE"))
                 .when(F.col("RATIO_UNITAIRE") > F.col("SEUIL_MAJORE"), F.lit("MAJORE"))
                 .otherwise(F.lit("NORMAL")),
            )
            .select(
                "BADR_ID", "DATE_DEPOT", "CODE_NGP", "CODE_NGP_INITIAL", "PAYS", "DEVISE",
                "QUANTITE", "VALEUR", "VALEUR_MAD", "PRIX_REFERENCE", "VALEUR_UNITAIRE_MAD",
                "RATIO_UNITAIRE", "ARBITRAGE",
            )
        )

        n_out = gold.count()
        n_lost = n_in - n_out
        print(f"\nLignes Gold en sortie: {n_out} (entree: {n_in}, perdues: {n_lost})")
        if n_lost != 0:
            raise ValueError(f"{n_lost} row(s) lost while building Gold - investigate before proceeding.")

        # --- Step 4: NULL report on important columns ---
        print("\n--- NULL count par colonne importante ---")
        for col in ["CODE_NGP", "VALEUR_MAD", "PRIX_REFERENCE", "VALEUR_UNITAIRE_MAD", "RATIO_UNITAIRE", "ARBITRAGE"]:
            n_null = gold.filter(F.col(col).isNull()).count()
            print(f"  {col}: {n_null}")
            if n_null:
                raise ValueError(f"Unexpected NULL in Gold column '{col}'.")

        # --- Step 5: distribution NORMAL/MINORE/MAJORE ---
        print("\n--- Distribution ARBITRAGE (global) ---")
        gold.groupBy("ARBITRAGE").count().orderBy("ARBITRAGE").show()

        print("--- Distribution ARBITRAGE par CODE_NGP ---")
        gold.groupBy("CODE_NGP", "ARBITRAGE").count().orderBy("CODE_NGP", "ARBITRAGE").show()

        # --- Step 6: ratio stats per arbitrage class (must show MINORE < NORMAL < MAJORE) ---
        print("--- Statistiques RATIO_UNITAIRE par classe ARBITRAGE ---")
        ratio_by_class = (
            gold.groupBy("ARBITRAGE")
            .agg(
                F.count("*").alias("N"),
                F.min("RATIO_UNITAIRE").alias("MIN"),
                F.expr("percentile(RATIO_UNITAIRE, 0.5)").alias("MEDIANE"),
                F.avg("RATIO_UNITAIRE").alias("MOYENNE"),
                F.max("RATIO_UNITAIRE").alias("MAX"),
            )
            .orderBy("ARBITRAGE")
        )
        ratio_by_class.show(truncate=False)
        class_stats = {r["ARBITRAGE"]: r for r in ratio_by_class.collect()}

        if "MINORE" in class_stats and "NORMAL" in class_stats:
            ok_low = class_stats["MINORE"]["MEDIANE"] < class_stats["NORMAL"]["MEDIANE"]
            print(f"MINORE mediane < NORMAL mediane: {ok_low} "
                  f"({class_stats['MINORE']['MEDIANE']:.4f} < {class_stats['NORMAL']['MEDIANE']:.4f})")
            if not ok_low:
                raise ValueError("Coherence check failed: MINORE median ratio is not below NORMAL median ratio.")
        if "MAJORE" in class_stats and "NORMAL" in class_stats:
            ok_high = class_stats["MAJORE"]["MEDIANE"] > class_stats["NORMAL"]["MEDIANE"]
            print(f"MAJORE mediane > NORMAL mediane: {ok_high} "
                  f"({class_stats['MAJORE']['MEDIANE']:.4f} > {class_stats['NORMAL']['MEDIANE']:.4f})")
            if not ok_high:
                raise ValueError("Coherence check failed: MAJORE median ratio is not above NORMAL median ratio.")

        # --- Step 7: manual verification (>= 3 rows) ---
        print("\n--- Verification manuelle (>= 3 lignes) ---")
        sample = gold.orderBy("BADR_ID").limit(5).collect()
        all_ok = True
        for r in sample:
            th = thresholds_rows[r["CODE_NGP"]]
            low, high = float(th["SEUIL_MINORE"]), float(th["SEUIL_MAJORE"])
            ratio = float(r["RATIO_UNITAIRE"])
            expected = "MINORE" if ratio < low else ("MAJORE" if ratio > high else "NORMAL")
            ok = expected == r["ARBITRAGE"]
            all_ok = all_ok and ok
            print(f"  BADR_ID={r['BADR_ID']} CODE_NGP={r['CODE_NGP']} RATIO_UNITAIRE={ratio:.4f} "
                  f"[P10={low:.4f}, P90={high:.4f}] -> attendu={expected} obtenu={r['ARBITRAGE']} OK={ok}")
        if not all_ok:
            raise ValueError("Manual arbitrage verification failed for at least one sample row.")

        print("\nExemples par classe ARBITRAGE:")
        for cls in ["MINORE", "NORMAL", "MAJORE"]:
            print(f"--- {cls} ---")
            gold.filter(F.col("ARBITRAGE") == cls).orderBy("BADR_ID").select(
                "BADR_ID", "CODE_NGP", "QUANTITE", "VALEUR_UNITAIRE_MAD", "PRIX_REFERENCE", "RATIO_UNITAIRE", "ARBITRAGE"
            ).show(3, truncate=False)

        # --- Step 8: write Gold ---
        gold.coalesce(1).write.mode("overwrite").parquet(GOLD_ARBITRAGE_PATH)
        print(f"\nWritten {n_out} rows to {GOLD_ARBITRAGE_PATH}")

        # --- Step 9: confirm Gold is actually readable after writing ---
        reread = spark.read.parquet(GOLD_ARBITRAGE_PATH)
        n_reread = reread.count()
        print(f"Gold re-read after write: {n_reread} rows (must equal {n_out})")
        if n_reread != n_out:
            raise ValueError("Gold round-trip read-back row count mismatch.")

        # --- Step 10: confirm sources untouched ---
        n_in_after = spark.read.parquet(BADR_RATIO_UNITAIRE_PATH).count()
        n_badr_after = spark.read.parquet(SILVER_BADR_PATH).count()
        print(f"\nbadr_ratio_unitaire row count after this job (must still be {n_in}): {n_in_after}")
        print(f"Silver BADR row count after this job (must still be {n_badr_before}): {n_badr_after}")
        if n_in_after != n_in or n_badr_after != n_badr_before:
            raise ValueError("A read-only source changed row count - it must never be written to by this job.")

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"badr_ratio_unitaire: {n_in} rows -> Gold arbitrage: {n_out} rows (0 perdues)")
        print(f"Seuils: P{int(LOW_PCT*100)}/P{int(HIGH_PCT*100)} par CODE_NGP, calcules a l'execution "
              "(PROTOTYPE - donnees BADR simulees, PAS un seuil douanier officiel)")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
