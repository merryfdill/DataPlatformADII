{{ config(enabled=false) }}

-- Disabled, not deleted (Phase 2.22).
--
-- This stub was part of the original project scaffold, anticipating a
-- separate dbt staging model for the NGP product matching. That step is no
-- longer needed here: NGP classification is IA 1's job
-- (models/ngp_classifier.joblib, Phase 2.11) and the CODE_NGP normalization
-- (Phase 2.13) is already applied upstream, before Spark writes the Gold
-- arbitrage table - see stg_declarations.sql and docs/dbt_gold.md. Nothing
-- in dbt re-runs or duplicates the matching/classification logic.
select 1
