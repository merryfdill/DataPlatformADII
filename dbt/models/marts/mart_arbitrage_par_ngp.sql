-- KPIs by CODE_NGP. Aggregation only - the ARBITRAGE verdict (absolute
-- threshold rule, spark/jobs/arbitrage_gold.py) is NOT recalculated here;
-- this mart only summarizes the already-produced ARBITRAGE/RATIO_UNITAIRE.
-- approx_percentile below is a descriptive median, not a threshold.

select
    CODE_NGP,
    count(*) as nb_declarations,
    count(*) filter (where ARBITRAGE = 'NORMAL') as nb_normal,
    count(*) filter (where ARBITRAGE = 'MINORE') as nb_minore,
    count(*) filter (where ARBITRAGE = 'MAJORE') as nb_majore,
    100.0 * count(*) filter (where ARBITRAGE = 'MINORE') / count(*) as taux_minoration,
    100.0 * count(*) filter (where ARBITRAGE = 'MAJORE') / count(*) as taux_majoration,
    approx_percentile(RATIO_UNITAIRE, 0.5) as ratio_median,
    avg(RATIO_UNITAIRE) as ratio_moyen,
    sum(VALEUR_MAD) as valeur_totale_mad,
    sum(QUANTITE) as quantite_totale
from {{ ref('fct_arbitrage') }}
group by CODE_NGP
order by CODE_NGP
