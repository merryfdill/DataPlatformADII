"""Train and evaluate a first CODE_NGP classifier (Phase 2.11).

Uses data/ml_dataset_features.csv (Phase 2.10 Dataset B - `categorie` is NOT
a feature, since it determines CODE_NGP 1:1 and would leak the label). The
label is CODE_NGP, an 8-digit HS/NGP code with exactly 3 classes:
85171300 (Smartphone), 84713000 (PC Portable), 85287200 (Televiseur).

With only 73 products (24/24/25), this is explicitly an MVP baseline, not a
production-grade classifier - see docs/ml_model.md for the full critical
analysis of what these metrics do and don't demonstrate.

This script:
  1. loads and validates the dataset (3 classes present, CODE_NGP as str),
  2. builds a stratified 80/20 train/test split,
  3. trains 3 pipelines (Logistic Regression / Linear SVM / Random Forest),
     each with its own ColumnTransformer (text TF-IDF + numeric + categorical)
     fitted ONLY on the train fold,
  4. evaluates all 3 on the held-out test fold,
  5. saves the best pipeline (by macro F1) + metadata to models/.

Usage
-----
    python ingestion/ml/train_model.py
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    precision_recall_fscore_support,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.svm import LinearSVC

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import features as feat  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("train_model")

EXPECTED_CLASSES = {"85171300", "84713000", "85287200"}
RANDOM_STATE = 42
DATASET_PATH = config.DATA_DIR / "ml_dataset_features.csv"
MODEL_PATH = config.MODELS_DIR / "ngp_classifier.joblib"
METADATA_PATH = config.MODELS_DIR / "ngp_classifier_metadata.json"


def load_dataset():
    df = pd.read_csv(DATASET_PATH, dtype={"CODE_NGP": str}, encoding="utf-8-sig")
    logger.info("Loaded %d rows / %d columns from %s", len(df), df.shape[1], DATASET_PATH)

    classes = set(df["CODE_NGP"].unique())
    if classes != EXPECTED_CLASSES:
        raise ValueError(f"Unexpected classes in CODE_NGP: {classes} != {EXPECTED_CLASSES}")
    logger.info("Classes present: %s", sorted(classes))
    logger.info("Class distribution:\n%s", df["CODE_NGP"].value_counts().to_string())

    # texte_produit already has 0 NULL (Phase 2.10 validation) - defensive only.
    df["texte_produit"] = df["texte_produit"].fillna("")
    df = feat.add_numeric_features(df)
    return df


def build_column_transformer():
    text_pipe = TfidfVectorizer(max_features=200, ngram_range=(1, 2), min_df=1)
    numeric_pipe = Pipeline([
        ("impute", SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
    ])
    categorical_pipe = Pipeline([
        ("impute", SimpleImputer(strategy="constant", fill_value="missing")),
        ("ohe", OneHotEncoder(handle_unknown="ignore")),
    ])
    return ColumnTransformer([
        ("text", text_pipe, feat.TEXT_FEATURE),
        ("num", numeric_pipe, feat.NUMERIC_FEATURES),
        ("cat", categorical_pipe, feat.CATEGORICAL_FEATURES),
    ])


def get_model_candidates():
    return {
        "LogisticRegression": LogisticRegression(max_iter=2000, random_state=RANDOM_STATE),
        "LinearSVM": LinearSVC(random_state=RANDOM_STATE),
        "RandomForest": RandomForestClassifier(n_estimators=200, random_state=RANDOM_STATE),
    }


def evaluate(pipeline, X_test, y_test, name):
    y_pred = pipeline.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_test, y_pred, average="macro", zero_division=0
    )
    logger.info("--- %s: classification_report ---\n%s", name, classification_report(y_test, y_pred, zero_division=0))
    labels = sorted(EXPECTED_CLASSES)
    cm = confusion_matrix(y_test, y_pred, labels=labels)
    logger.info("--- %s: confusion matrix (rows=true, cols=pred, order=%s) ---\n%s", name, labels, cm)
    return {"accuracy": acc, "precision_macro": precision, "recall_macro": recall, "f1_macro": f1}


def main():
    config.MODELS_DIR.mkdir(parents=True, exist_ok=True)

    df = load_dataset()
    X = df[feat.FEATURE_COLUMNS]
    y = df["CODE_NGP"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=RANDOM_STATE, stratify=y
    )
    logger.info("Train: %d rows, Test: %d rows", len(X_train), len(X_test))
    logger.info("Train class distribution:\n%s", y_train.value_counts().to_string())
    logger.info("Test class distribution:\n%s", y_test.value_counts().to_string())
    logger.warning(
        "Only %d products total (%d train / %d test, ~%d per class in test). "
        "Treat all metrics below as an MVP baseline signal, not a production performance estimate.",
        len(df), len(X_train), len(X_test), len(X_test) // 3,
    )

    results = {}
    fitted_pipelines = {}
    for name, model in get_model_candidates().items():
        pipeline = Pipeline([
            ("preprocess", build_column_transformer()),
            ("model", model),
        ])
        pipeline.fit(X_train, y_train)  # preprocessing learned ONLY on train, inside the pipeline
        fitted_pipelines[name] = pipeline
        results[name] = evaluate(pipeline, X_test, y_test, name)

    comparison = pd.DataFrame(results).T[["accuracy", "precision_macro", "recall_macro", "f1_macro"]]
    logger.info("--- Model comparison (test set) ---\n%s", comparison.to_string())

    best_name = comparison["f1_macro"].idxmax()
    best_pipeline = fitted_pipelines[best_name]
    logger.info("Best model by macro F1: %s (f1_macro=%.3f)", best_name, comparison.loc[best_name, "f1_macro"])

    joblib.dump(best_pipeline, MODEL_PATH)
    logger.info("Saved best pipeline to %s", MODEL_PATH)

    metadata = {
        "model": best_name,
        "classes": sorted(EXPECTED_CLASSES),
        "random_state": RANDOM_STATE,
        "features": {
            "text": feat.TEXT_FEATURE,
            "numeric": feat.NUMERIC_FEATURES,
            "categorical": feat.CATEGORICAL_FEATURES,
            "excluded": ["categorie", "camera", "batterie", "CODE_NGP", "url", "date_scraping", "site_source"],
        },
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "dataset": str(DATASET_PATH.relative_to(config.PROJECT_ROOT)).replace("\\", "/"),
        "dataset_rows": len(df),
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "test_metrics": {k: round(v, 4) for k, v in results[best_name].items()},
        "all_models_test_metrics": {m: {k: round(v, 4) for k, v in r.items()} for m, r in results.items()},
        "sklearn_version": sklearn.__version__,
        "limitations": [
            "3 classes correspond to the 3 scraped categories (Smartphone/PC Portable/Televiseur) at "
            "HS8 granularity - not a 10-digit classification (CKD/SKD, tablette, usage industriel/satellite "
            "are structurally absent from this retail-scraping source, see docs/ml_dataset.md).",
            "Test set has only ~5 examples per class - metrics are high-variance, not a reliable estimate.",
            "The Tarif douanier does not distinguish NGP by brand; this model must not be used to imply "
            "a brand -> NGP relationship.",
        ],
    }
    with open(METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    logger.info("Saved metadata to %s", METADATA_PATH)


if __name__ == "__main__":
    main()
