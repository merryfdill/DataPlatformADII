"""Predict CODE_NGP for a new product using the pipeline saved by
train_model.py (Phase 2.11).

Loads models/ngp_classifier.joblib (preprocessing + model, fitted end to
end) and applies it to a single product description. `categorie` is never
used as input, on purpose - see docs/ml_model.md for why.

IMPORTANT (see docs/ml_model.md "Analyse critique"): this classifier was
trained on 73 products from 3 categories, and largely keys on brand +
category-specific vocabulary rather than the legal criteria in the Tarif
douanier (OS capability, weight/keyboard, color receiver). It reproduces the
category boundary of this specific small sample; it is not a validated
general-purpose NGP classifier and should not be presented as one.

Usage
-----
    python ingestion/ml/predict_ngp.py                  # demo on a real dataset row
    python ingestion/ml/predict_ngp.py --demo-row 10     # demo on a different row
"""

import argparse
import logging
import sys
from pathlib import Path

import joblib
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import features as feat  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("predict_ngp")

MODEL_PATH = config.MODELS_DIR / "ngp_classifier.joblib"


def load_pipeline():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"No trained pipeline at {MODEL_PATH} - run ingestion/ml/train_model.py first.")
    return joblib.load(MODEL_PATH)


def predict_ngp(pipeline, product: dict):
    """product: dict with the same raw fields as data/prix_web.csv rows
    (marque, modele, description, prix, devise, type_prix, ram, stockage,
    reseau, taille_ecran, processeur - `categorie` is ignored even if
    supplied, batterie/camera are ignored, they were excluded from training
    for having almost no data - see features.py).

    Returns {"CODE_NGP": str, "probabilities": {code: float} | None}
    """
    row = {
        "texte_produit": feat.build_texte_produit(
            product.get("marque"), product.get("modele"), product.get("description")
        ),
        "prix": product.get("prix"),
        "marque": product.get("marque"),
        "devise": product.get("devise"),
        "type_prix": product.get("type_prix"),
        "reseau": product.get("reseau"),
        "processeur": product.get("processeur"),
    }
    for col in feat.NUMERIC_SOURCE_COLUMNS:
        row[f"{col}_num"] = feat.extract_leading_number(product.get(col))

    X = pd.DataFrame([row])[feat.FEATURE_COLUMNS]

    predicted = pipeline.predict(X)[0]
    result = {"CODE_NGP": predicted, "probabilities": None}

    if hasattr(pipeline, "predict_proba"):
        proba = pipeline.predict_proba(X)[0]
        result["probabilities"] = {cls: round(float(p), 4) for cls, p in zip(pipeline.classes_, proba)}
    else:
        logger.info("Model has no predict_proba - probabilities not available for this model type.")

    return result


def _demo(row_index):
    pipeline = load_pipeline()
    dataset_path = config.DATA_DIR / "ml_dataset_features.csv"
    df = pd.read_csv(dataset_path, dtype={"CODE_NGP": str}, encoding="utf-8-sig")

    record = df.iloc[row_index]
    product = {
        "marque": record["marque"],
        "modele": record["modele"],
        "description": record["description"],
        "prix": record["prix"],
        "devise": record["devise"],
        "type_prix": record["type_prix"],
        "ram": record["ram"],
        "stockage": record["stockage"],
        "reseau": record["reseau"],
        "taille_ecran": record["taille_ecran"],
        "processeur": record["processeur"],
    }
    actual = record["CODE_NGP"]

    result = predict_ngp(pipeline, product)

    logger.info("Demo product (row %d of %s): %s %s", row_index, dataset_path.name, product["marque"], product["modele"])
    logger.info("Actual CODE_NGP:    %s", actual)
    logger.info("Predicted CODE_NGP: %s", result["CODE_NGP"])
    logger.info("Match: %s", result["CODE_NGP"] == actual)
    if result["probabilities"]:
        logger.info("Class probabilities: %s", result["probabilities"])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demo-row", type=int, default=0, help="Row index from ml_dataset_features.csv to demo on")
    args = parser.parse_args()
    _demo(args.demo_row)


if __name__ == "__main__":
    main()
