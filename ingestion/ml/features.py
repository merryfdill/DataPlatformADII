"""Shared feature definitions for Phase 2.11 (train_model.py and predict_ngp.py
must build features identically, or a saved pipeline would see different
inputs at inference time than it was trained on).

Column choices, and why some scraped columns are excluded here:

- `camera` is 100% NULL in data/ml_dataset_features.csv (Phase 2.10 finding)
  and `batterie` is 72/73 NULL (a single real value). Imputing either would
  just produce a constant column carrying zero real per-row signal - not
  "fabricating a characteristic", but there is nothing to preprocess into a
  useful feature, so both are left out of the model entirely rather than
  silently turned into a fake constant.
- `ram` / `stockage` / `taille_ecran` are stored as compound strings
  ("8 Go", "256 Go SSD", "6.7\""), not clean numbers (Phase 2.10 deliberately
  left them as-is). `extract_leading_number` parses the leading numeric
  value already present in that text - this is preprocessing (deterministic
  parsing of an existing value), not inventing data; values that are still
  missing after parsing stay missing and are handled by the pipeline's
  imputer, never filled with a guessed number here.
- `categorie` is excluded on purpose (Phase 2.10/2.11 data-leakage decision:
  categorie determines CODE_NGP 1:1, see docs/ml_dataset.md).
"""

import re

import numpy as np

NUMBER_RE = re.compile(r"(\d+(?:[.,]\d+)?)")

TEXT_FEATURE = "texte_produit"
NUMERIC_SOURCE_COLUMNS = ["ram", "stockage", "taille_ecran"]
NUMERIC_FEATURES = ["prix", "ram_num", "stockage_num", "taille_ecran_num"]
CATEGORICAL_FEATURES = ["marque", "devise", "type_prix", "reseau", "processeur"]
FEATURE_COLUMNS = [TEXT_FEATURE] + NUMERIC_FEATURES + CATEGORICAL_FEATURES


def extract_leading_number(value):
    """'8 Go' -> 8.0, '256 Go SSD' -> 256.0, '6.7"' -> 6.7, NaN/None -> NaN."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return np.nan
    match = NUMBER_RE.search(str(value))
    if not match:
        return np.nan
    return float(match.group(1).replace(",", "."))


def build_texte_produit(marque, modele, description):
    """Same concatenation rule as ingestion/ml/prepare_dataset.py's
    build_text_feature, kept in sync manually since the Phase 2.10 dataset
    already ships texte_produit pre-built - this is only used here to build
    the same feature for a brand-new product at prediction time.
    """
    parts = [str(marque or "").strip(), str(modele or "").strip(), str(description or "").strip()]
    return re.sub(r"\s+", " ", " ".join(parts)).strip()


def add_numeric_features(df):
    """Adds ram_num/stockage_num/taille_ecran_num columns in place (copy)."""
    df = df.copy()
    for col in NUMERIC_SOURCE_COLUMNS:
        df[f"{col}_num"] = df[col].apply(extract_leading_number)
    return df


def build_texte_produit_series(df):
    """Vectorized form of build_texte_produit, for a full DataFrame (e.g. a
    Silver batch) that doesn't already ship a pre-built texte_produit column
    the way data/ml_dataset_features.csv does.
    """
    return df.apply(
        lambda r: build_texte_produit(r.get("marque"), r.get("modele"), r.get("description")), axis=1
    )


def ensure_raw_columns(df, columns=None):
    """Adds any of `columns` missing from df as an all-NaN column (copy).

    Used when applying the trained pipeline to a source that may not have
    every column the model was trained on (e.g. a stale/partial Silver
    scrape) - missing columns become genuinely missing values, handled by
    the pipeline's own imputers, never a fabricated guess.
    """
    if columns is None:
        columns = ["marque", "modele", "description", "prix", "devise", "type_prix"] + NUMERIC_SOURCE_COLUMNS + CATEGORICAL_FEATURES
    df = df.copy()
    for col in columns:
        if col not in df.columns:
            df[col] = np.nan
    return df
