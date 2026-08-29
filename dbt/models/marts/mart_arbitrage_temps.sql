-- Temporal KPIs.
--
-- Granularity: MONTH, not day. DATE_DEPOT spans ~2 years but fct_arbitrage
-- has only a few hundred rows over almost as many distinct days - a daily
-- grain would be almost entirely single-declaration days (no real trend to
-- show). Grouping by month (~11-25 declarations/month) is the coarsest
-- grain that still shows a genuine distribution per period - checked
-- against the real data, per the instruction not to build a temporal mart
-- the data can't actually support.

select
    date_trunc('month', DATE_DEPOT) as mois,
    count(*) as nb_declarations,
    count(*) filter (where ARBITRAGE = 'NORMAL') as nb_normal,
    count(*) filter (where ARBITRAGE = 'MINORE') as nb_minore,
    count(*) filter (where ARBITRAGE = 'MAJORE') as nb_majore,
    avg(RATIO_UNITAIRE) as ratio_moyen,
    sum(VALEUR_MAD) as valeur_totale_mad
from {{ ref('fct_arbitrage') }}
group by 1
order by 1
