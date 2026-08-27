# Prix de référence par CODE_NGP (Phase 2.14)

Complète [`docs/ml_prediction.md`](ml_prediction.md) (Phase 2.12) et [`docs/ngp_normalization.md`](ngp_normalization.md) (Phase 2.13).

## Pourquoi calculer un prix de référence

Avant tout arbitrage (comparaison avec `BADR.VALEUR`), il faut d'abord établir, indépendamment de BADR, ce que le marché retail (Jumia) pratique réellement comme prix pour chaque catégorie de produit identifiée par son `CODE_NGP`. Cette phase construit uniquement cette référence de marché — elle ne la compare à rien d'autre.

## Pourquoi Spark

Cohérent avec la convention déjà en place pour la couche Silver (`spark/jobs/bronze_to_silver.py`) : même style de job (`SparkSession`, rapport imprimé, écriture `s3a://` en `overwrite` coalescée à 1 fichier vu le faible volume), plutôt que de faire cette agrégation en pandas côté `ingestion/ml/`.

## Données utilisées

`s3://datalake/silver/scraping_ml/scraping_predictions.parquet` (Phase 2.12, 73 produits) — jamais BADR, jamais le scraping brut, jamais un recalcul des prédictions NGP (`CODE_NGP_PREDIT` est lu tel quel).

## Comment la médiane est calculée

Spark SQL `percentile(prix, 0.5)` (fonction **exacte**, pas `percentile_approx`). Avec seulement 23 à 25 lignes par groupe `CODE_NGP`, le calcul exact est trivialement peu coûteux — aucune raison d'accepter une approximation sur un volume aussi petit.

## Pourquoi les devises BADR ne sont pas encore utilisées

Cette phase ne lit que le côté scraping. Le `devise` du scraping est vérifié dans les données réelles avant tout calcul (pas supposé) : une seule valeur trouvée, `MAD`. Le résultat est explicitement tagué `DEVISE_REFERENCE = "MAD"`. Aucune conversion EUR/USD/GBP → MAD n'est faite ; les devises BADR (qui peuvent être EUR/USD/GBP/MAD, voir `ingestion/config.py BADR_COUNTRY_CURRENCY`) ne sont ni lues ni mélangées ici.

## Pourquoi cette phase ne fait pas encore l'arbitrage

`PRIX_REFERENCE` est une mesure du marché retail seule. Comparer ce prix à `BADR.VALEUR`, calculer un ratio, ou classer une déclaration NORMAL/MINORÉ/MAJORÉ nécessite d'abord une conversion de devise cohérente et une décision méthodologique sur la comparabilité (prix retail TTC vs valeur déclarée à l'import) — hors périmètre de cette phase, traité plus tard.

## Structure de la table PRIX_REFERENCE

`s3://datalake/silver/reference/prix_reference/` (emplacement recommandé, vérifié inexistant avant création — pas de doublon avec une table équivalente).

| Colonne | Type | Contenu |
|---|---|---|
| `CODE_NGP` | string | code HS8 (normalisé, Phase 2.13) |
| `NB_PRODUITS` | int | nombre de produits scraping utilisés |
| `PRIX_MIN` | decimal | prix minimum |
| `PRIX_MEDIAN` | double | médiane exacte |
| `PRIX_MOYEN` | decimal | moyenne |
| `PRIX_MAX` | decimal | prix maximum |
| `DEVISE_REFERENCE` | string | toujours `"MAD"` à ce stade |

## Résultat (réel, exécuté)

| CODE_NGP | Catégorie | NB_PRODUITS | PRIX_MIN | PRIX_MEDIAN | PRIX_MOYEN | PRIX_MAX |
|---|---|---|---|---|---|---|
| 85171300 | Smartphone | 23 | 949,00 | 1549,0 | 2618,87 | 15499,00 |
| 84713000 | PC Portable | 25 | 1899,00 | 2899,0 | 3919,68 | 20490,00 |
| 85287200 | Televiseur | 25 | 899,00 | 2040,0 | 2747,12 | 9899,00 |

Pour chaque code : `PRIX_MIN ≤ PRIX_MEDIAN ≤ PRIX_MAX` et `PRIX_MIN ≤ PRIX_MOYEN ≤ PRIX_MAX` — vérifiés programmatiquement, aucune relation médiane/moyenne supposée a priori.

## Limites

- Aucune comparaison BADR, aucune conversion de devise, aucun ratio, aucune classification NORMAL/MINORÉ/MAJORÉ à ce stade.
- `NB_PRODUITS` par code reste petit (23–25) : ce prix de référence est un signal MVP, pas une estimation de marché statistiquement robuste.
- Aucune valeur extrême n'a été retirée ni de normalisation statistique appliquée (min/max bruts conservés tels quels, comme demandé).
