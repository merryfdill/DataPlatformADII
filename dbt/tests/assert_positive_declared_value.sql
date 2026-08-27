-- Singular test: VALEUR_MAD must be strictly positive for every analyzed
-- declaration (verified true for all 338 real rows, Phase 2.22 - guarded
-- here so a future refresh can't silently regress). A dbt singular test
-- passes when the query returns zero rows.

select *
from {{ ref('fct_arbitrage') }}
where VALEUR_MAD <= 0
