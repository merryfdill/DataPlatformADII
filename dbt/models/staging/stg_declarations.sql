-- Phase 2.22: thin staging view over the Spark Gold arbitrage source.
-- No business logic recomputed here - VALEUR_MAD, PRIX_REFERENCE,
-- VALEUR_UNITAIRE_MAD, RATIO_UNITAIRE and ARBITRAGE are taken as-is from
-- Spark (Phases 2.17-2.21). This is a 1:1 pass-through per dbt staging
-- convention (a stable naming layer between the raw source and the marts);
-- NULL/domain/uniqueness checks live in schema.yml, not here.

select
    BADR_ID,
    DATE_DEPOT,
    CODE_NGP,
    CODE_NGP_INITIAL,
    PAYS,
    DEVISE,
    QUANTITE,
    VALEUR,
    VALEUR_MAD,
    PRIX_REFERENCE,
    VALEUR_UNITAIRE_MAD,
    RATIO_UNITAIRE,
    ARBITRAGE
from {{ source('gold', 'arbitrage') }}
