-- Global KPIs across all analyzed declarations.
-- Pure aggregation of fct_arbitrage - the ARBITRAGE verdict (absolute
-- threshold rule in spark/jobs/arbitrage_gold.py) and RATIO_UNITAIRE are
-- taken as-is, nothing is recalculated here.

select
    count(*) as nb_declarations,
    count(*) filter (where ARBITRAGE = 'NORMAL') as nb_normal,
    count(*) filter (where ARBITRAGE = 'MINORE') as nb_minore,
    count(*) filter (where ARBITRAGE = 'MAJORE') as nb_majore,
    100.0 * count(*) filter (where ARBITRAGE = 'NORMAL') / count(*) as pct_normal,
    100.0 * count(*) filter (where ARBITRAGE = 'MINORE') / count(*) as pct_minore,
    100.0 * count(*) filter (where ARBITRAGE = 'MAJORE') / count(*) as pct_majore,
    avg(RATIO_UNITAIRE) as ratio_moyen,
    approx_percentile(RATIO_UNITAIRE, 0.5) as ratio_median,
    sum(VALEUR_MAD) as valeur_totale_mad,
    sum(QUANTITE) as quantite_totale
from {{ ref('fct_arbitrage') }}
