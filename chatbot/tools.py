"""Controlled, read-only tools the chatbot's LLM can call to fetch real data
from the Gold layer (Phase 2.24). LLM-agnostic - used first with GroqCloud
(Phase 2.24), then Gemini (Phase 2.25) via a different function-calling
wrapper in app.py; this module itself never changes with the LLM provider.

Every tool builds a SELECT statement from strictly validated/enum-constrained
inputs (never raw LLM text interpolated directly) and executes it via
run_select(), which is itself a hard safety net: it refuses anything that
isn't a plain SELECT against one of the 4 allow-listed Gold tables. The LLM
never gets direct/unlimited SQL access - it can only invoke these functions
with structured JSON arguments (tool/function calling), and the arbitrage
verdict itself is never recomputed here, only read back exactly as Spark
(spark/jobs/arbitrage_gold.py, Phase 2.21) and dbt (Phase 2.22) produced it.
"""

import datetime
import decimal
import os
import re

import trino

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

FORBIDDEN_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|MERGE|TRUNCATE|GRANT|REVOKE|COMMIT|ROLLBACK|CALL)\b",
    re.IGNORECASE,
)
ALLOWED_TABLES = (
    "iceberg.gold.fct_arbitrage",
    "iceberg.gold.mart_arbitrage_kpi",
    "iceberg.gold.mart_arbitrage_par_ngp",
    "iceberg.gold.mart_arbitrage_temps",
)


def _to_jsonable(value):
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value


def run_select(sql: str) -> dict:
    """Executes a read-only SELECT against Gold via Trino. Refuses anything
    that isn't a plain SELECT against an allow-listed Gold table.
    """
    stripped = sql.strip()
    if not re.match(r"^SELECT\b", stripped, re.IGNORECASE):
        raise ValueError("Seules les requetes SELECT sont autorisees.")
    if FORBIDDEN_KEYWORDS.search(stripped):
        raise ValueError("Requete refusee : mot-cle non autorise detecte.")
    if not any(t in stripped for t in ALLOWED_TABLES):
        raise ValueError("Requete refusee : table hors perimetre Gold autorise.")

    conn = trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user="chatbot", catalog="iceberg", schema="gold",
    )
    try:
        cur = conn.cursor()
        cur.execute(stripped)
        columns = [d[0] for d in cur.description]
        rows = cur.fetchall()
    finally:
        conn.close()

    return {
        "sql": stripped,
        "rows": [
            {col: _to_jsonable(val) for col, val in zip(columns, row)}
            for row in rows
        ],
    }


# Mirrors ingestion/config.py SCRAPING_CATEGORY_TO_NGP8 (Phase 2.8) - the only
# 3 CODE_NGP values present in Gold (Phase 2.13 normalization). Duplicated
# here (not imported) to keep this service self-contained, independent of
# the ingestion module's scraping dependencies.
CATEGORY_TO_NGP = {
    "smartphone": "85171300",
    "smartphones": "85171300",
    "telephone": "85171300",
    "telephones": "85171300",
    "pc portable": "84713000",
    "pc portables": "84713000",
    "ordinateur portable": "84713000",
    "ordinateurs portables": "84713000",
    "laptop": "84713000",
    "laptops": "84713000",
    "televiseur": "85287200",
    "televiseurs": "85287200",
    "television": "85287200",
    "televisions": "85287200",
    "tele": "85287200",
    "tv": "85287200",
}
VALID_NGP_CODES = {"85171300", "84713000", "85287200"}


def resolve_code_ngp(value):
    if not value:
        return None
    v = str(value).strip().lower()
    if v in CATEGORY_TO_NGP:
        return CATEGORY_TO_NGP[v]
    digits = re.sub(r"\D", "", str(value))
    if digits in VALID_NGP_CODES:
        return digits
    raise ValueError(
        f"CODE_NGP inconnu : '{value}'. Valeurs valides : {sorted(VALID_NGP_CODES)} "
        "ou Smartphone / PC Portable / Televiseur."
    )


def validate_mois(value):
    if not value:
        return None
    v = str(value).strip()
    if re.match(r"^\d{4}-\d{2}$", v):
        v = f"{v}-01"
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
        raise ValueError(f"Format de mois invalide : '{value}' (attendu YYYY-MM).")
    datetime.date.fromisoformat(v)  # raises ValueError if not a real date
    return v


SORT_OPTIONS = {
    "ratio_desc": "ratio_unitaire DESC",
    "ratio_asc": "ratio_unitaire ASC",
    "valeur_desc": "valeur_mad DESC",
    "valeur_asc": "valeur_mad ASC",
    "quantite_desc": "quantite DESC",
    "quantite_asc": "quantite ASC",
    "date_desc": "date_depot DESC",
}


def get_kpi_globaux():
    return run_select("SELECT * FROM iceberg.gold.mart_arbitrage_kpi")


def get_kpi_par_ngp(code_ngp=None):
    resolved = resolve_code_ngp(code_ngp)
    if resolved:
        return run_select(
            f"SELECT * FROM iceberg.gold.mart_arbitrage_par_ngp WHERE code_ngp = '{resolved}'"
        )
    return run_select("SELECT * FROM iceberg.gold.mart_arbitrage_par_ngp ORDER BY code_ngp")


def get_kpi_temporel(mois_debut=None, mois_fin=None):
    conditions = []
    d1 = validate_mois(mois_debut)
    d2 = validate_mois(mois_fin)
    if d1:
        conditions.append(f"mois >= date('{d1}')")
    if d2:
        conditions.append(f"mois <= date('{d2}')")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return run_select(f"SELECT * FROM iceberg.gold.mart_arbitrage_temps {where} ORDER BY mois")


def rechercher_declarations(code_ngp=None, arbitrage=None, tri="ratio_desc", limite=10):
    conditions = []
    resolved = resolve_code_ngp(code_ngp)
    if resolved:
        conditions.append(f"code_ngp = '{resolved}'")
    if arbitrage:
        a = str(arbitrage).strip().upper()
        if a not in {"NORMAL", "MINORE", "MAJORE"}:
            raise ValueError(f"Verdict d'arbitrage invalide : '{arbitrage}'.")
        conditions.append(f"arbitrage = '{a}'")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    order = SORT_OPTIONS.get(tri, SORT_OPTIONS["ratio_desc"])
    lim = max(1, min(int(limite or 10), 50))
    sql = (
        "SELECT badr_id, date_depot, code_ngp, pays, quantite, valeur_mad, "
        "valeur_unitaire_mad, prix_reference, ratio_unitaire, arbitrage "
        f"FROM iceberg.gold.fct_arbitrage {where} ORDER BY {order} LIMIT {lim}"
    )
    return run_select(sql)


def expliquer_declaration(badr_id):
    bid = int(badr_id)
    result = run_select(f"SELECT * FROM iceberg.gold.fct_arbitrage WHERE badr_id = {bid}")
    if not result["rows"]:
        return result
    code_ngp = result["rows"][0]["code_ngp"]
    context = run_select(
        f"SELECT * FROM iceberg.gold.mart_arbitrage_par_ngp WHERE code_ngp = '{code_ngp}'"
    )
    result["contexte_categorie"] = context["rows"][0] if context["rows"] else None
    return result


TOOLS_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "get_kpi_globaux",
            "description": (
                "Recupere les KPI globaux deja calcules (nombre total de declarations, "
                "NORMAL/MINORE/MAJORE, pourcentages, ratio moyen/median, valeur totale MAD, "
                "quantite totale) depuis iceberg.gold.mart_arbitrage_kpi. A utiliser pour toute "
                "question sur des totaux ou statistiques globales."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_kpi_par_ngp",
            "description": (
                "Recupere les KPI (NORMAL/MINORE/MAJORE, taux de minoration/majoration, ratio, "
                "valeur, quantite) par categorie de produit depuis iceberg.gold.mart_arbitrage_par_ngp. "
                "Sans code_ngp, retourne les 3 categories - utile pour comparer Smartphone / "
                "PC Portable / Televiseur."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "code_ngp": {
                        "type": "string",
                        "description": (
                            "Code NGP (85171300, 84713000 ou 85287200) ou nom de categorie "
                            "(Smartphone, PC Portable, Televiseur). Omettre pour les 3 categories."
                        ),
                    }
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_kpi_temporel",
            "description": (
                "Recupere l'evolution mensuelle des arbitrages (nombre de declarations, "
                "NORMAL/MINORE/MAJORE, ratio moyen, valeur totale) depuis "
                "iceberg.gold.mart_arbitrage_temps. A utiliser pour toute question sur une "
                "tendance ou une evolution dans le temps."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "mois_debut": {"type": "string", "description": "Mois de debut, format YYYY-MM (optionnel)."},
                    "mois_fin": {"type": "string", "description": "Mois de fin, format YYYY-MM (optionnel)."},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "rechercher_declarations",
            "description": (
                "Recherche des declarations individuelles dans iceberg.gold.fct_arbitrage, avec "
                "filtres optionnels (categorie, verdict d'arbitrage) et tri. A utiliser pour "
                "'donne-moi les declarations avec le ratio le plus eleve', 'quantite importante et "
                "ratio anormal', etc."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "code_ngp": {"type": "string", "description": "Filtrer par code NGP ou nom de categorie (optionnel)."},
                    "arbitrage": {
                        "type": "string",
                        "enum": ["NORMAL", "MINORE", "MAJORE"],
                        "description": "Filtrer par verdict d'arbitrage (optionnel).",
                    },
                    "tri": {
                        "type": "string",
                        "enum": list(SORT_OPTIONS.keys()),
                        "description": "Critere de tri (defaut: ratio_desc).",
                    },
                    "limite": {"type": "integer", "description": "Nombre maximum de resultats (defaut 10, max 50)."},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "expliquer_declaration",
            "description": (
                "Recupere le detail complet d'UNE declaration precise (par badr_id) pour expliquer "
                "son classement NORMAL/MINORE/MAJORE : CODE_NGP, QUANTITE, VALEUR_MAD, "
                "VALEUR_UNITAIRE_MAD, PRIX_REFERENCE, RATIO_UNITAIRE, ARBITRAGE, plus le ratio "
                "moyen/median de sa categorie pour comparaison."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "badr_id": {"type": "integer", "description": "Identifiant BADR_ID de la declaration."}
                },
                "required": ["badr_id"],
            },
        },
    },
]

TOOL_FUNCTIONS = {
    "get_kpi_globaux": lambda args: get_kpi_globaux(),
    "get_kpi_par_ngp": lambda args: get_kpi_par_ngp(args.get("code_ngp")),
    "get_kpi_temporel": lambda args: get_kpi_temporel(args.get("mois_debut"), args.get("mois_fin")),
    "rechercher_declarations": lambda args: rechercher_declarations(
        args.get("code_ngp"), args.get("arbitrage"), args.get("tri", "ratio_desc"), args.get("limite", 10)
    ),
    "expliquer_declaration": lambda args: expliquer_declaration(args["badr_id"]),
}

TOOL_SOURCE_TABLE = {
    "get_kpi_globaux": "iceberg.gold.mart_arbitrage_kpi",
    "get_kpi_par_ngp": "iceberg.gold.mart_arbitrage_par_ngp",
    "get_kpi_temporel": "iceberg.gold.mart_arbitrage_temps",
    "rechercher_declarations": "iceberg.gold.fct_arbitrage",
    "expliquer_declaration": "iceberg.gold.fct_arbitrage + iceberg.gold.mart_arbitrage_par_ngp",
}
