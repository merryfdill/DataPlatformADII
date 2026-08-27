"""Simulate the BADR customs declaration source as a local SQLite database.

We don't have access to the real BADR system, so this script reproduces the
view handed over by ADII (see the columns below) and fills it with synthetic
but business-coherent data generated with Faker. No columns beyond the ones
in the customs view are added here (no scoring/verdict/ratio/reference price
- that belongs to later Silver/Gold stages), except QUANTITE (Phase 2.19,
see below) which IS part of a customs declaration in reality even though the
earlier version of this simulation omitted it.

Phase 2.19: added QUANTITE (commercial lot size, integer >= 1). Phase 2.18's
diagnostic found VALEUR/POIDS were drawn fully independently of the
declaration's product category, making them physically implausible at the
unit level (e.g. a "1 smartphone" declaration weighing hundreds of kg) and
making VALEUR incomparable to a unit retail price without knowing how many
units it covers. VALEUR_INITIALE and POIDS_INITIAL are now generated as
QUANTITE x a per-unit baseline (config.BADR_QUANTITY_PARAMS_BY_CATEGORY,
keyed by the same 8 business categories already used for CODE_NGP - not a
parallel scheme) x natural noise - this is standard commercial-invoice logic
(total = unit x quantity), not derived from PRIX_REFERENCE/scraping/ML in
any way. The existing "declared vs inspector's assessment" (VALEUR) and
"declared vs actual weighing" (POIDS) discrepancy logic is unchanged.

Regeneration strategy (idempotence)
------------------------------------
Without --run-date: full DROP TABLE + CREATE TABLE before inserting, so
re-running never appends to or duplicates existing rows - it always produces
a clean, self-contained dataset. Generation is deterministic by default
(fixed random seed) so re-running without flags reproduces the same data;
pass --seed to get a different draw. This is the original one-time
historical-generation path, unchanged.

With --run-date (daily append mode, added for the Airflow daily-simulation
phase): INSERT only, never DROP/UPDATE/DELETE. Adds --count (default 15 -
kept in the historical 1-16/day range, see docs/analysis for why: the
730-day history averages 6.85 declarations/day and peaks at 16, so 15 stays
visually consistent on a time-series chart instead of a 7x scale jump) new
declarations dated exactly --run-date to the EXISTING table, alongside
whatever is already there. The seed is derived from --run-date itself
(int(YYYYMMDD)), not from --seed, so replaying the same date always
reproduces the exact same rows. Before inserting, checks whether
DATE_DEPOT=--run-date already has rows; if so, inserts nothing and returns
cleanly (anti-double-insert guard - required because Airflow's own retries
would otherwise turn one logical day into 2-3x the intended row count).

Usage
-----
    python ingestion/badr/generate_badr.py                  # 5000 rows (default), historical, DROP+CREATE
    python ingestion/badr/generate_badr.py --rows 20000      # bigger volume
    python ingestion/badr/generate_badr.py --seed 7           # different draw
    python ingestion/badr/generate_badr.py --db-path data/badr.db
    python ingestion/badr/generate_badr.py --run-date 2026-09-01            # append 15 rows dated 2026-09-01
    python ingestion/badr/generate_badr.py --run-date 2026-09-01 --count 80 # append 80 rows instead
"""

import argparse
import random
import sqlite3
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

from faker import Faker  # noqa: E402

CREATE_TABLE_SQL = f"""
CREATE TABLE {config.BADR_TABLE_NAME} (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    DATE_DEPOT        TEXT    NOT NULL,
    VALEUR_INITIALE   REAL    NOT NULL CHECK (VALEUR_INITIALE > 0),
    VALEUR            REAL    NOT NULL CHECK (VALEUR > 0),
    POIDS             REAL    NOT NULL CHECK (POIDS > 0),
    POIDS_INITIAL     REAL    NOT NULL CHECK (POIDS_INITIAL > 0),
    QUANTITE          INTEGER NOT NULL CHECK (QUANTITE >= 1),
    CODE_NGP          TEXT    NOT NULL,
    CODE_NGP_INITIAL  TEXT    NOT NULL,
    PAYS              TEXT    NOT NULL,
    DEVISE            TEXT    NOT NULL
);
"""

INSERT_SQL = f"""
INSERT INTO {config.BADR_TABLE_NAME} (
    DATE_DEPOT, VALEUR_INITIALE, VALEUR, POIDS, POIDS_INITIAL, QUANTITE,
    CODE_NGP, CODE_NGP_INITIAL, PAYS, DEVISE
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def generate_row(fake, rng, forced_date=None):
    # --- HS/NGP code: declared, then possible reclassification ---
    # (drawn first so QUANTITE/VALEUR/POIDS below can be generated
    # consistently with the declaration's business category)
    category = rng.choice(list(config.BADR_HS_CODES_BY_CATEGORY.keys()))
    code_ngp_initial = rng.choice(config.BADR_HS_CODES_BY_CATEGORY[category])
    r = rng.random()
    if r < 0.90:
        code_ngp = code_ngp_initial
    elif r < 0.97:
        code_ngp = rng.choice(config.BADR_HS_CODES_BY_CATEGORY[category])
    else:
        other_category = rng.choice(
            [c for c in config.BADR_HS_CODES_BY_CATEGORY if c != category]
        )
        code_ngp = rng.choice(config.BADR_HS_CODES_BY_CATEGORY[other_category])

    params = config.BADR_QUANTITY_PARAMS_BY_CATEGORY[category]

    # --- Quantity: commercial lot size, category-dependent (Phase 2.19) ---
    quantite = max(1, round(rng.lognormvariate(params["qty_mu"], params["qty_sigma"])))

    # --- Value: importer-declared base (quantite x a synthetic per-unit
    # value - NOT derived from PRIX_REFERENCE/scraping/ML), then the
    # inspector's assessment ---
    unit_value = rng.lognormvariate(params["unit_value_mu"], params["unit_value_sigma"])
    valeur_initiale = round(quantite * unit_value * rng.uniform(0.9, 1.1), 2)
    r = rng.random()
    if r < 0.60:
        valeur = valeur_initiale  # accepted as declared
    elif r < 0.85:
        valeur = valeur_initiale * rng.uniform(1.05, 1.40)  # revalued up
    else:
        valeur = valeur_initiale * rng.uniform(0.80, 0.95)  # revalued down
    valeur = round(valeur, 2)

    # --- Weight: importer-declared (quantite x typical unit weight for the
    # category), then the actual weighing ---
    poids_initial = round(quantite * params["unit_weight_kg"] * rng.uniform(0.9, 1.1), 2)
    r = rng.random()
    if r < 0.80:
        poids = poids_initial * rng.uniform(0.98, 1.02)  # near-identical
    else:
        poids = poids_initial * rng.uniform(0.85, 1.15)  # discrepancy
    poids = round(max(poids, 0.1), 2)

    # --- Country / currency, kept coherent ---
    pays = rng.choice(list(config.BADR_COUNTRY_CURRENCY.keys()))
    if rng.random() < config.BADR_ALTERNATE_CURRENCY_RATE:
        devise = rng.choice(config.BADR_ALTERNATE_CURRENCIES)
    else:
        devise = config.BADR_COUNTRY_CURRENCY[pays]

    # --- Date: forced_date (daily append mode) takes priority; otherwise
    # spread randomly over the configured history window (original,
    # one-time historical-generation behavior, unchanged) ---
    date_depot = forced_date or fake.date_between(
        start_date=f"-{config.BADR_HISTORY_DAYS}d", end_date="today"
    ).isoformat()

    return (
        date_depot,
        valeur_initiale,
        valeur,
        poids,
        poids_initial,
        quantite,
        code_ngp,
        code_ngp_initial,
        pays,
        devise,
    )


def table_exists(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (config.BADR_TABLE_NAME,),
    )
    return cur.fetchone() is not None


def append_daily(conn, run_date_str, count):
    """INSERT-only: adds `count` new declarations dated exactly
    `run_date_str` to the existing table. Never touches existing rows
    (no DROP/UPDATE/DELETE). Anti-double-insert guard: if that date
    already has rows, does nothing and returns - so an Airflow retry (or
    a manual replay) can never turn one logical day into 2x/3x rows.
    """
    if not table_exists(conn):
        raise RuntimeError(
            f"Table '{config.BADR_TABLE_NAME}' introuvable dans le fichier cible - "
            "le mode append suppose que la generation historique initiale "
            "(sans --run-date) a deja ete faite au moins une fois."
        )

    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM {config.BADR_TABLE_NAME} WHERE DATE_DEPOT = ?",
        (run_date_str,),
    )
    existing = cur.fetchone()[0]
    if existing > 0:
        print(
            f"DATE_DEPOT={run_date_str} a deja {existing} declaration(s) en base - "
            "aucun INSERT (garde anti-doublon), ce run est considere comme deja fait."
        )
        return existing

    # Seed derived from the date itself (not --seed): replaying the same
    # run_date always regenerates the exact same rows, independently of
    # how many times or in what order other dates were appended.
    seed = int(run_date_str.replace("-", ""))
    fake = Faker()
    Faker.seed(seed)
    rng = random.Random(seed)

    rows = [generate_row(fake, rng, forced_date=run_date_str) for _ in range(count)]
    cur.executemany(INSERT_SQL, rows)
    conn.commit()

    total = cur.execute(f"SELECT COUNT(*) FROM {config.BADR_TABLE_NAME}").fetchone()[0]
    print(f"Append: {count} nouvelle(s) declaration(s) pour DATE_DEPOT={run_date_str} (total table: {total})")
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=config.BADR_DEFAULT_NUM_ROWS)
    parser.add_argument("--seed", type=int, default=config.BADR_DEFAULT_SEED)
    parser.add_argument("--db-path", type=Path, default=config.BADR_DB_PATH)
    parser.add_argument(
        "--run-date",
        type=str,
        default=None,
        help=(
            "YYYY-MM-DD - switches to daily APPEND mode: adds --count new "
            "declarations dated exactly --run-date to the existing table, "
            "INSERT-only, never touching existing rows. Airflow passes "
            "{{ ds }} here. Omit entirely for the original one-time "
            "historical generation (DROP+CREATE, --rows/--seed)."
        ),
    )
    parser.add_argument(
        "--count",
        type=int,
        default=15,
        help="Number of declarations to append. Only used together with --run-date.",
    )
    args = parser.parse_args()

    args.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(args.db_path)
    try:
        if args.run_date:
            date.fromisoformat(args.run_date)  # raises ValueError on a malformed date
            append_daily(conn, args.run_date, args.count)
        else:
            fake = Faker()
            Faker.seed(args.seed)
            rng = random.Random(args.seed)

            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {config.BADR_TABLE_NAME}")
            cur.execute(CREATE_TABLE_SQL)

            rows = [generate_row(fake, rng) for _ in range(args.rows)]
            cur.executemany(INSERT_SQL, rows)
            conn.commit()

            count = cur.execute(f"SELECT COUNT(*) FROM {config.BADR_TABLE_NAME}").fetchone()[0]
            print(f"Generated {count} rows into {args.db_path} (table: {config.BADR_TABLE_NAME})")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
