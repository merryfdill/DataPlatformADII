{{ config(enabled=false) }}

-- Disabled, not deleted (Phase 2.22).
--
-- This stub was part of the original project scaffold, anticipating a
-- separate dbt staging model for scraped market prices. That step is no
-- longer needed here: PRIX_REFERENCE (Phase 2.14 scraping) is already
-- resolved and joined by Spark into the single Gold arbitrage table
-- (spark/jobs/arbitrage_gold.py, Phase 2.21) before dbt ever runs - see
-- stg_declarations.sql and docs/dbt_gold.md ("Spark = calcul metier,
-- DBT = modelisation analytique"). Re-deriving market prices separately in
-- dbt would duplicate Spark's already-validated work, which this phase
-- explicitly avoids.
select 1
