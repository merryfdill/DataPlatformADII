-- Singular test: PRIX_REFERENCE must be strictly positive (verified true
-- for all 338 real rows, Phase 2.22 - required for RATIO_UNITAIRE to be
-- defined, cf. docs/ratio_unitaire.md).

select *
from {{ ref('fct_arbitrage') }}
where PRIX_REFERENCE <= 0
