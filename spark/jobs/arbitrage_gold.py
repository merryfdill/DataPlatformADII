"""RATIO_UNITAIRE -> ARBITRAGE (NORMAL/MINORE/MAJORE) -> Gold.

This is a BUSINESS RULE, not a second ML model: no Isolation Forest, no
anomaly-detection classifier, no .joblib file.

RULE (official, provided by the ADII supervisor 2026-08-28 - replaces the
earlier P10/P90 prototype). ABSOLUTE, symmetric threshold: each declaration
is judged against ITS OWN reference price, not against the rest of the
population.

    RATIO_UNITAIRE < BORNE_BASSE            -> MINORE  (declared > SEUIL below ref)
    BORNE_BASSE <= RATIO_UNITAIRE <= BORNE_HAUTE -> NORMAL
    RATIO_UNITAIRE > BORNE_HAUTE            -> MAJORE  (declared > SEUIL above ref)

    BORNE_BASSE = 1 - ARBITRAGE_SEUIL_MINORE_PCT / 100
    BORNE_HAUTE = 1 + ARBITRAGE_SEUIL_MAJORE_PCT / 100

Both percentages default to 10 (NORMAL band [0.90, 1.10]) and are the ONLY
knob: set them in .env, recreate the spark-iceberg container, done - no code
change, no image rebuild (spark/jobs/ is bind-mounted). Two separate vars so
the low and high sides can be given different values later.

Why this replaced P10/P90: percentiles are RELATIVE to the population - they
always flag ~10% of each side whatever the data, and they SHIFT for every
already-judged declaration as the population grows. An absolute threshold
gives a real, stable verdict per declaration; the counts vary for genuine
reasons. (Re-running still reclassifies old lots, but only because
PRIX_REFERENCE - the median of that period's scraped prices - moves between
runs; that is why arbitrage stays manual and period-scoped.)

IMPORTANT: BADR is simulated (Faker) and PRIX_REFERENCE comes from scraping.
This is a prototype rule on synthetic data - see docs/arbitrage_gold.md. The
10% figure is the supervisor's stated rule but the whole pipeline is a
demonstration, not a production ADII customs system.

ASCII values (NORMAL/MINORE/MAJORE, no accents) are used for the ARBITRAGE
column to avoid encoding issues in Parquet/Trino/dbt downstream - documented
choice, not an oversight.

Sources (all read-only, none modified):
  - s3a://datalake/silver/badr_ratio_unitaire/ (already the matched/filtered
    subset with a valid RATIO_UNITAIRE - declarations whose CODE_NGP is
    outside the 3 scraped categories have no PRIX_REFERENCE and were dropped
    upstream by ratio_unitaire.py; they get NO verdict and stay out of Gold)
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

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BADR_RATIO_UNITAIRE_PATH = "s3a://datalake/silver/badr_ratio_unitaire/"
SILVER_BADR_PATH = "s3a://datalake/silver/badr/"
GOLD_ARBITRAGE_PATH = "s3a://datalake/gold/arbitrage/"

EXPECTED_CODES = {"85171300", "84713000", "85287200"}

# --- Business rule threshold - the ONE knob, isolated here ---
SEUIL_MINORE_PCT = float(os.environ.get("ARBITRAGE_SEUIL_MINORE_PCT", "10"))
SEUIL_MAJORE_PCT = float(os.environ.get("ARBITRAGE_SEUIL_MAJORE_PCT", "10"))
BORNE_BASSE = 1.0 - SEUIL_MINORE_PCT / 100.0
BORNE_HAUTE = 1.0 + SEUIL_MAJORE_PCT / 100.0

if not (0.0 <= SEUIL_MINORE_PCT < 100.0):
    raise ValueError(
        f"ARBITRAGE_SEUIL_MINORE_PCT={SEUIL_MINORE_PCT} hors bornes [0, 100) - "
        "borne basse <= 0 rendrait MINORE impossible (le ratio est toujours > 0)."
    )
if SEUIL_MAJORE_PCT < 0.0:
    raise ValueError(f"ARBITRAGE_SEUIL_MAJORE_PCT={SEUIL_MAJORE_PCT} negatif.")


def main():
    spark = SparkSession.builder.appName("arbitrage_gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=" * 60)
        print("ARBITRAGE_GOLD: RATIO_UNITAIRE -> NORMAL/MINORE/MAJORE -> Gold")
        print("=" * 60)

        # Effective thresholds for THIS run - logged so the Airflow task log
        # shows exactly which rule was applied.
        print("\nRegle d'arbitrage appliquee (seuil absolu, isole dans ARBITRAGE_SEUIL_*_PCT) :")
        print(f"  ARBITRAGE_SEUIL_MINORE_PCT = {SEUIL_MINORE_PCT} %  ->  borne basse NORMAL = {BORNE_BASSE:.6f}")
        print(f"  ARBITRAGE_SEUIL_MAJORE_PCT = {SEUIL_MAJORE_PCT} %  ->  borne haute NORMAL = {BORNE_HAUTE:.6f}")
        print(f"    RATIO_UNITAIRE < {BORNE_BASSE:.6f}                    -> MINORE")
        print(f"    {BORNE_BASSE:.6f} <= RATIO_UNITAIRE <= {BORNE_HAUTE:.6f} -> NORMAL")
        print(f"    RATIO_UNITAIRE > {BORNE_HAUTE:.6f}                    -> MAJORE")

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
        print(f"RATIO_UNITAIRE NULL: {n_ratio_null} (attendu 0 - deja garanti en amont par ratio_unitaire.py)")
        if n_ratio_null:
            raise ValueError("RATIO_UNITAIRE has NULL values - arbitrage cannot be computed for those rows.")

        # Descriptive per-CODE_NGP median (context only - NOT a threshold; the
        # rule is the absolute BORNE_BASSE/BORNE_HAUTE above).
        print("\nMediane RATIO_UNITAIRE par CODE_NGP (indicatif, pas un seuil) :")
        ratio_unitaire.groupBy("CODE_NGP").agg(
            F.count("*").alias("N"),
            F.expr("percentile(RATIO_UNITAIRE, 0.5)").alias("MEDIANE"),
        ).orderBy("CODE_NGP").show(truncate=False)

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

        # --- Step 3: apply the arbitrage rule (absolute business rule, not ML) ---
        gold = (
            enriched
            .withColumn(
                "ARBITRAGE",
                F.when(F.col("RATIO_UNITAIRE") < F.lit(BORNE_BASSE), F.lit("MINORE"))
                 .when(F.col("RATIO_UNITAIRE") > F.lit(BORNE_HAUTE), F.lit("MAJORE"))
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

        # --- Step 6: ratio stats per arbitrage class (MINORE all < BORNE_BASSE,
        # MAJORE all > BORNE_HAUTE, so medians must order MINORE < NORMAL < MAJORE) ---
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

        # Rule-exactness check: every row's class must match the absolute bornes.
        if "MINORE" in class_stats and class_stats["MINORE"]["MAX"] >= BORNE_BASSE:
            raise ValueError(
                f"MINORE contient un ratio >= borne basse ({class_stats['MINORE']['MAX']} >= {BORNE_BASSE})."
            )
        if "MAJORE" in class_stats and class_stats["MAJORE"]["MIN"] <= BORNE_HAUTE:
            raise ValueError(
                f"MAJORE contient un ratio <= borne haute ({class_stats['MAJORE']['MIN']} <= {BORNE_HAUTE})."
            )
        if "NORMAL" in class_stats and (
            class_stats["NORMAL"]["MIN"] < BORNE_BASSE or class_stats["NORMAL"]["MAX"] > BORNE_HAUTE
        ):
            raise ValueError(
                f"NORMAL deborde des bornes [{BORNE_BASSE}, {BORNE_HAUTE}] "
                f"(min={class_stats['NORMAL']['MIN']}, max={class_stats['NORMAL']['MAX']})."
            )

        # --- Step 7: manual verification (>= 3 rows) ---
        print("\n--- Verification manuelle (>= 3 lignes) ---")
        sample = gold.orderBy("BADR_ID").limit(5).collect()
        all_ok = True
        for r in sample:
            ratio = float(r["RATIO_UNITAIRE"])
            expected = "MINORE" if ratio < BORNE_BASSE else ("MAJORE" if ratio > BORNE_HAUTE else "NORMAL")
            ok = expected == r["ARBITRAGE"]
            all_ok = all_ok and ok
            print(f"  BADR_ID={r['BADR_ID']} CODE_NGP={r['CODE_NGP']} RATIO_UNITAIRE={ratio:.4f} "
                  f"[borne_basse={BORNE_BASSE:.4f}, borne_haute={BORNE_HAUTE:.4f}] "
                  f"-> attendu={expected} obtenu={r['ARBITRAGE']} OK={ok}")
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
        print(f"Regle: seuil absolu - borne basse {BORNE_BASSE:.4f} / borne haute {BORNE_HAUTE:.4f} "
              f"(ARBITRAGE_SEUIL_MINORE_PCT={SEUIL_MINORE_PCT}, ARBITRAGE_SEUIL_MAJORE_PCT={SEUIL_MAJORE_PCT}) "
              "- PROTOTYPE, donnees BADR simulees")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
