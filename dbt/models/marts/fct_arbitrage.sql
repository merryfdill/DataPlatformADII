-- Fact table: one row per BADR declaration analyzed by the Spark
-- arbitrage pipeline (Phases 2.17-2.21). VALEUR_MAD, PRIX_REFERENCE,
-- VALEUR_UNITAIRE_MAD, RATIO_UNITAIRE and ARBITRAGE are Spark's
-- business-computed results, taken as-is - not recomputed here.
--
-- The only addition is EST_RECLASSIFIE, a derived flag (CODE_NGP <>
-- CODE_NGP_INITIAL) useful for downstream analysis/chatbot explanations
-- ("this declaration's HS code was normalized/reclassified") - it does not
-- alter or duplicate any business value.

select
    BADR_ID,
    DATE_DEPOT,
    CODE_NGP,
    CODE_NGP_INITIAL,
    (CODE_NGP <> CODE_NGP_INITIAL) as EST_RECLASSIFIE,
    PAYS,
    DEVISE,
    QUANTITE,
    VALEUR,
    VALEUR_MAD,
    PRIX_REFERENCE,
    VALEUR_UNITAIRE_MAD,
    RATIO_UNITAIRE,
    ARBITRAGE
from {{ ref('stg_declarations') }}
