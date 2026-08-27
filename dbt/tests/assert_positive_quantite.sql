-- Singular test: QUANTITE must be strictly positive (verified true for all
-- 338 real rows, Phase 2.22 - required for VALEUR_UNITAIRE_MAD to be
-- meaningful, cf. docs/ratio_unitaire.md).

select *
from {{ ref('fct_arbitrage') }}
where QUANTITE <= 0
