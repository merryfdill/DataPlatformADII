"""NGP code normalization reference (Phase 2.13).

BADR (data/badr.db, simulated with Faker) was seeded with a fixed list of
plausible HS/NGP codes per business category
(config.BADR_HS_CODES_BY_CATEGORY), captured before the Phase 2.9 official
Tarif douanier analysis. For the smartphone category this means BADR still
carries `85171200` - the pre-SH2022 code for "téléphones pour réseaux
cellulaires" - while the Tarif ADII actually in force (Édition 1er janvier
2022, verified in Phase 2.9) renumbered that sub-position to `85171300`
("téléphones intelligents"). BADR is a fixed, already-generated dataset and
must never be edited to "fix" this - see docs/ngp_normalization.md for the
full reasoning.

This module is the ONLY place that reconciles the two: a small, explicit,
manually-reviewed reference table (`NGP_NORMALIZATION_TABLE`), applied only
to build a matching view - never written back to Silver BADR or badr.db.

Every row must be independently justified from the Phase 2.9 Tarif analysis.
Do NOT add a row here "because it seems logical" - only for a code that has
actually been checked against the official Tarif. Any BADR code not listed
here is passed through unchanged (identity) by this module: that is an
explicit "out of scope for this MVP", not a claim that it's verified correct.
"""

import pandas as pd

NGP_NORMALIZATION_TABLE = [
    {
        "ancien_code": "85171200",
        "code_normalise": "85171300",
        "raison": (
            "Evolution de nomenclature SH2022 : la sous-position smartphone "
            "a ete renumerotee de 8517.12 (pre-2022, valeur encore utilisee "
            "dans les codes BADR simules) a 8517.13 'Telephones intelligents' "
            "(Tarif ADII actuellement en vigueur)."
        ),
        "source": "Tarif des droits de douane a l'importation, ADII, Edition 1er janvier 2022 (Phase 2.9)",
    },
    {
        "ancien_code": "84713000",
        "code_normalise": "84713000",
        "raison": (
            "Code verifie inchange : position 84.71, sous-position 8471.30 "
            "('machines automatiques de traitement de l'information "
            "portatives...') confirmee exacte dans le Tarif ADII actuel, "
            "aucune evolution de nomenclature identifiee."
        ),
        "source": "Tarif ADII Edition 1er janvier 2022, Chapitre 84 (Phase 2.9)",
    },
    {
        "ancien_code": "85287200",
        "code_normalise": "85287200",
        "raison": (
            "Code verifie inchange : position 85.28, sous-position 8528.72 "
            "('Autres, en couleurs') confirmee exacte dans le Tarif ADII "
            "actuel, aucune evolution de nomenclature identifiee."
        ),
        "source": "Tarif ADII Edition 1er janvier 2022, Chapitre 85 (Phase 2.9)",
    },
]


def get_normalization_table():
    return pd.DataFrame(NGP_NORMALIZATION_TABLE)


def normalize_code(code):
    """Looks up `code` in the reference table; returns the normalized code,
    or `code` unchanged if it isn't in the table (out of scope, not a claim
    of correctness).
    """
    for row in NGP_NORMALIZATION_TABLE:
        if row["ancien_code"] == code:
            return row["code_normalise"]
    return code


def apply_normalization(df, source_column="CODE_NGP"):
    """Returns a copy of df with two new columns:
      - CODE_NGP_ORIGINAL: the untouched source value (audit trail)
      - CODE_NGP_NORMALISE: the value after table lookup (identity if not listed)
    Never mutates df in place, never touches the source column itself.
    """
    df = df.copy()
    df["CODE_NGP_ORIGINAL"] = df[source_column]
    df["CODE_NGP_NORMALISE"] = df[source_column].apply(normalize_code)
    return df
