"""Simulate the BADR customs declaration source as a local SQLite database.

We don't have access to the real BADR system, so this script reproduces the
view handed over by ADII (see the columns below) and fills it with synthetic
but business-coherent data generated with Faker. No columns beyond the ones
in the customs view are added here (no scoring/verdict/ratio/reference price
- that belongs to later Silver/Gold stages).

Regeneration strategy (idempotence)
------------------------------------
Every run does a full DROP TABLE + CREATE TABLE before inserting, so
re-running never appends to or duplicates existing rows - it always produces
a clean, self-contained dataset. Generation is deterministic by default
(fixed random seed) so re-running without flags reproduces the same data;
pass --seed to get a different draw.

Usage
-----
    python ingestion/badr/generate_badr.py                  # 5000 rows (default)
    python ingestion/badr/generate_badr.py --rows 20000      # bigger volume
    python ingestion/badr/generate_badr.py --seed 7           # different draw
    python ingestion/badr/generate_badr.py --db-path data/badr.db
"""

import argparse
import random
import sqlite3
import sys
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
    CODE_NGP          TEXT    NOT NULL,
    CODE_NGP_INITIAL  TEXT    NOT NULL,
    PAYS              TEXT    NOT NULL,
    DEVISE            TEXT    NOT NULL
);
"""

INSERT_SQL = f"""
INSERT INTO {config.BADR_TABLE_NAME} (
    DATE_DEPOT, VALEUR_INITIALE, VALEUR, POIDS, POIDS_INITIAL,
    CODE_NGP, CODE_NGP_INITIAL, PAYS, DEVISE
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def generate_row(fake, rng):
    # --- Value: importer-declared base, then the inspector's assessment ---
    valeur_initiale = round(rng.lognormvariate(8.6, 0.9), 2)
    r = rng.random()
    if r < 0.60:
        valeur = valeur_initiale  # accepted as declared
    elif r < 0.85:
        valeur = valeur_initiale * rng.uniform(1.05, 1.40)  # revalued up
    else:
        valeur = valeur_initiale * rng.uniform(0.80, 0.95)  # revalued down
    valeur = round(valeur, 2)

    # --- Weight: importer-declared, then the actual weighing ---
    poids_initial = round(rng.lognormvariate(6.5, 1.1), 2)
    r = rng.random()
    if r < 0.80:
        poids = poids_initial * rng.uniform(0.98, 1.02)  # near-identical
    else:
        poids = poids_initial * rng.uniform(0.85, 1.15)  # discrepancy
    poids = round(max(poids, 0.1), 2)

    # --- HS/NGP code: declared, then possible reclassification ---
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

    # --- Country / currency, kept coherent ---
    pays = rng.choice(list(config.BADR_COUNTRY_CURRENCY.keys()))
    if rng.random() < config.BADR_ALTERNATE_CURRENCY_RATE:
        devise = rng.choice(config.BADR_ALTERNATE_CURRENCIES)
    else:
        devise = config.BADR_COUNTRY_CURRENCY[pays]

    # --- Date, spread over the configured history window ---
    date_depot = fake.date_between(
        start_date=f"-{config.BADR_HISTORY_DAYS}d", end_date="today"
    ).isoformat()

    return (
        date_depot,
        valeur_initiale,
        valeur,
        poids,
        poids_initial,
        code_ngp,
        code_ngp_initial,
        pays,
        devise,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=config.BADR_DEFAULT_NUM_ROWS)
    parser.add_argument("--seed", type=int, default=config.BADR_DEFAULT_SEED)
    parser.add_argument("--db-path", type=Path, default=config.BADR_DB_PATH)
    args = parser.parse_args()

    fake = Faker()
    Faker.seed(args.seed)
    rng = random.Random(args.seed)

    args.db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db_path)
    try:
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
