"""Prepare the ML dataset for CODE_NGP prediction (Phase 2.10).

Reads data/prix_web.csv (the Phase 2.8 3-category scraping: Smartphone / PC
Portable / Televiseur, CODE_NGP left NULL), attaches the CODE_NGP label from
the officially-sourced category -> 8-digit HS/NGP mapping documented in
config.SCRAPING_CATEGORY_TO_NGP8 (Phase 2.9 Tarif douanier analysis - not
BADR, not Faker, not guessed), builds a simple combined text feature, and
writes two feature-selection variants for the future model:

  - data/ml_dataset_baseline.csv : includes `categorie` as a feature. This is
    a trivial baseline (categorie already determines the label 1:1 by
    construction - see config.SCRAPING_CATEGORY_TO_NGP8), useful only to
    exercise the end-to-end classification pipeline, not as a real signal
    test.
  - data/ml_dataset_features.csv : excludes `categorie`, so a model trained
    on it has to find signal in marque/modele/description/technical specs
    instead of trivially reading off the label.

This script does NOT train a model, does NOT predict, does NOT touch BADR,
and does NOT modify data/prix_web.csv (read-only source). It only reads,
labels, selects columns, validates, and writes the two CSVs above.

Usage
-----
    python ingestion/ml/prepare_dataset.py
"""

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("prepare_dataset")

# Columns that exist in data/prix_web.csv but must never be used as model
# features: CODE_NGP is the label (currently NULL in the source - overwritten
# here from the business mapping, not read from the source column), the rest
# are metadata about the scrape itself, not about the product.
EXCLUDED_AS_FEATURES = {"CODE_NGP", "url", "date_scraping", "site_source"}

BASELINE_COLUMNS = [
    "marque", "modele", "description", "prix", "devise", "type_prix",
    "categorie", "ram", "stockage", "reseau", "taille_ecran", "processeur",
    "batterie", "camera", "texte_produit", "CODE_NGP",
]
FEATURES_COLUMNS = [
    "marque", "modele", "description", "prix", "devise", "type_prix",
    "ram", "stockage", "reseau", "taille_ecran", "processeur",
    "batterie", "camera", "texte_produit", "CODE_NGP",
]

EXPECTED_CLASS_COUNTS = {"85171300": 24, "84713000": 24, "85287200": 25}


def load_source():
    path = config.SCRAPING_OUTPUT_CSV
    if not path.exists():
        raise FileNotFoundError(f"Scraping source not found: {path}")
    df = pd.read_csv(path, encoding="utf-8-sig")
    logger.info("Loaded %d rows / %d columns from %s", len(df), df.shape[1], path)
    return df


def validate_source(df):
    """Fail fast on anything that would make label attachment unsafe."""
    unknown_categories = set(df["categorie"].unique()) - set(config.SCRAPING_CATEGORY_TO_NGP8)
    if unknown_categories:
        raise ValueError(
            f"Categories with no NGP mapping in config.SCRAPING_CATEGORY_TO_NGP8: {unknown_categories}"
        )

    required_non_null = ["marque", "modele", "description", "prix", "devise", "categorie", "url"]
    for col in required_non_null:
        n_null = df[col].isnull().sum()
        if n_null:
            raise ValueError(f"Unexpected NULL in required source column '{col}': {n_null} rows")

    if df["CODE_NGP"].notna().any():
        raise ValueError(
            "Source data/prix_web.csv already has non-NULL CODE_NGP values - "
            "expected NULL for all rows at this pipeline stage (label is attached here, not scraped)."
        )

    n_dup = df.duplicated(subset=["url"]).sum()
    if n_dup:
        raise ValueError(f"{n_dup} duplicate URLs found in source - expected 0 (Phase 2.8 already dedups).")

    logger.info("Source validation passed: categories known, no unexpected NULL, no duplicate URLs.")


def attach_label(df):
    """CODE_NGP = 8-digit HS/NGP code, from the official ADII Tarif mapping
    (config.SCRAPING_CATEGORY_TO_NGP8) - never from BADR, never from Faker.
    """
    df = df.copy()
    df["CODE_NGP"] = df["categorie"].map(config.SCRAPING_CATEGORY_TO_NGP8)
    if df["CODE_NGP"].isnull().any():
        raise ValueError("Label attachment left NULL CODE_NGP - a category fell outside the mapping.")
    return df


def build_text_feature(df):
    """texte_produit = marque + modele + description, concatenated as-is.

    marque/modele/description have 0 NULL in the current source (verified
    in validate_source), so no fabricated placeholder text is introduced
    here; the .fillna("") is defensive only, for future scraping runs.
    """
    df = df.copy()
    df["texte_produit"] = (
        df["marque"].fillna("").astype(str).str.strip()
        + " "
        + df["modele"].fillna("").astype(str).str.strip()
        + " "
        + df["description"].fillna("").astype(str).str.strip()
    ).str.replace(r"\s+", " ", regex=True).str.strip()
    return df


def null_report(df, label):
    logger.info("--- NULL count per column: %s (%d rows) ---", label, len(df))
    for col in df.columns:
        n_null = df[col].isnull().sum()
        pct = (n_null / len(df) * 100) if len(df) else 0
        logger.info("  %-14s %3d NULL (%.0f%%)", col, n_null, pct)


def validate_output(df, label):
    logger.info("--- Validation: %s ---", label)
    logger.info("rows=%d cols=%d", len(df), df.shape[1])
    logger.info("dtypes:\n%s", df.dtypes.to_string())

    n_dup = df.duplicated().sum()
    logger.info("exact duplicate rows: %d", n_dup)

    dist = df["CODE_NGP"].value_counts().to_dict()
    logger.info("class distribution: %s", dist)
    if dist != EXPECTED_CLASS_COUNTS:
        raise ValueError(f"Unexpected class distribution for {label}: {dist} != {EXPECTED_CLASS_COUNTS}")
    if len(df) != sum(EXPECTED_CLASS_COUNTS.values()):
        raise ValueError(f"Unexpected total row count for {label}: {len(df)}")

    null_report(df, label)


def select_columns(df, columns, label):
    missing = [c for c in columns if c not in df.columns]
    if missing:
        logger.warning("%s: columns requested but not found in source, skipped: %s", label, missing)
    present = [c for c in columns if c in df.columns]
    leaked = EXCLUDED_AS_FEATURES & set(present) - {"CODE_NGP"}
    if leaked:
        raise ValueError(f"{label}: metadata column(s) leaked into feature set: {leaked}")
    return df[present]


def main():
    df = load_source()
    validate_source(df)
    df = attach_label(df)
    df = build_text_feature(df)

    baseline = select_columns(df, BASELINE_COLUMNS, "ml_dataset_baseline")
    features = select_columns(df, FEATURES_COLUMNS, "ml_dataset_features")

    validate_output(baseline, "Dataset A (baseline, includes categorie)")
    validate_output(features, "Dataset B (features, excludes categorie)")

    baseline_path = config.DATA_DIR / "ml_dataset_baseline.csv"
    features_path = config.DATA_DIR / "ml_dataset_features.csv"
    baseline.to_csv(baseline_path, index=False, encoding="utf-8-sig")
    features.to_csv(features_path, index=False, encoding="utf-8-sig")
    logger.info("Wrote %s (%d rows)", baseline_path, len(baseline))
    logger.info("Wrote %s (%d rows)", features_path, len(features))

    # Train/test strategy proposal for Phase 2.11 - computed here only to
    # confirm the split is arithmetically sound with this class distribution;
    # not executed (no sklearn dependency added, no split files written).
    logger.info("--- Train/test split proposal for Phase 2.11 (not executed here) ---")
    for code, n in EXPECTED_CLASS_COUNTS.items():
        n_test = round(n * 0.2)
        logger.info("  class %s: n=%d -> ~%d train / ~%d test (stratified 80/20)", code, n, n - n_test, n_test)
    logger.info("  proposal: sklearn.model_selection.train_test_split(..., test_size=0.2, "
                "stratify=CODE_NGP, random_state=42)")


if __name__ == "__main__":
    main()
