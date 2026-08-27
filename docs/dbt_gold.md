# DBT / Gold analytique (Phase 2.22)

Complète [`docs/arbitrage_gold.md`](arbitrage_gold.md) (Phase 2.21). Couche de modélisation analytique au-dessus de la Gold Spark, exploitable par Grafana, un futur chatbot LLM, des analyses SQL et de futurs DAG Airflow.

## Architecture confirmée

```
BADR + SCRAPING → BRONZE → SILVER → IA 1 (NGP) → PRIX_REFERENCE
                                                          ↓
                                          VALEUR_UNITAIRE → RATIO_UNITAIRE
                                                          ↓
                                          Regle metier ARBITRAGE (Phase 2.21)
                                                          ↓
                                    GOLD Spark (Parquet, s3://datalake/gold/arbitrage/)
                                                          ↓
                        register_gold_iceberg.py : Parquet → table Iceberg (Phase 2.22)
                                                          ↓
                              DBT (Trino) : sources → staging → fct → marts
                                                          ↓
                                    Grafana / SQL / futur chatbot LLM
```

- **Spark** = calcul métier et préparation (classification NGP, prix de référence, ratio, arbitrage). Rien de tout cela n'est recalculé par dbt.
- **DBT** = transformation/modélisation analytique (staging, fact, agrégats/KPI) au-dessus du résultat déjà calculé.
- **Grafana** = visualisation (phase ultérieure).
- **LLM** = interrogation et explication des données Gold (phase ultérieure) — ne remplace jamais le calcul métier, n'invente jamais de résultat.

## Infrastructure existante réutilisée (rien recréé)

- Service `dbt` (`docker-compose.yml`) : déjà présent, `./dbt` monté dans le conteneur, connecté à Trino via `dbt-trino`.
- `dbt/profiles.yml` : déjà configuré — catalogue `iceberg`, schéma `gold`, host `trino:8080`. Non modifié.
- `dbt/dbt_project.yml` : déjà configuré (staging = view, marts = table). Non modifié.
- Scaffold de modèles déjà présent dans le commit initial du projet (`git log` : `52cde97 initial project structure`), mais **entièrement vide** (0 octet) : `stg_declarations.sql`, `stg_market_prices.sql`, `stg_product_matches.sql`, `fct_declarations_valuation.sql`, `mart_daily_kpis.sql`, `mart_risk_by_hs_code.sql`, `sources.yml`, `schema.yml`, `tests/assert_positive_declared_value.sql`. Cette phase **peuple** ces fichiers plutôt que d'en créer des parallèles :
  - `fct_declarations_valuation.sql` → renommé `fct_arbitrage.sql` (même concept : fait des déclarations valorisées)
  - `mart_daily_kpis.sql` → renommé `mart_arbitrage_temps.sql`
  - `mart_risk_by_hs_code.sql` → renommé `mart_arbitrage_par_ngp.sql`
  - `mart_arbitrage_kpi.sql` : nouveau (aucun équivalent dans le scaffold)
- Catalogue Trino `iceberg` (`infrastructure/trino/catalog/iceberg.properties`) : REST catalog déjà pointé vers `iceberg-rest` + MinIO. Non modifié.
- `spark/conf/spark-defaults.conf` : catalogue Iceberg déjà entièrement configuré côté Spark (même REST catalog, même warehouse). Non modifié — jamais exploité par les phases précédentes (toutes en Parquet brut), exploité pour la première fois dans cette phase.

## Pourquoi une étape technique supplémentaire était nécessaire

Le catalogue Trino `iceberg` est un **REST catalog Iceberg strict** : il ne voit que les tables qu'il gère lui-même via métadonnées Iceberg, pas un dossier Parquet brut arbitraire. Or `s3://datalake/gold/arbitrage/` (Phase 2.21) a été écrit en Parquet brut par Spark — invisible pour Trino/dbt tel quel.

**`spark/jobs/register_gold_iceberg.py`** (nouveau, Phase 2.22) résout cela : il relit le Parquet Gold **déjà calculé et déjà validé** (aucune recomputation, aucun changement de valeur) et l'écrit tel quel comme table Iceberg `iceberg.gold.arbitrage`, via le catalogue Iceberg déjà configuré côté Spark. C'est une étape de **branchement technique**, pas une étape métier.

Un correctif mineur a été nécessaire au lancement (`--conf spark.sql.catalog.iceberg.s3.path-style-access=true`, passé en ligne de commande, aucun fichier image modifié) : le client S3 d'Iceberg utilisait par défaut l'adressage virtual-hosted-style (`datalake.minio`, non résolvable) au lieu du path-style déjà utilisé pour les lectures S3A classiques.

## Sources DBT

`dbt/models/sources.yml` : source unique `gold.arbitrage` → `iceberg.gold.arbitrage` (338 lignes, schéma vérifié réellement — voir [`docs/arbitrage_gold.md`](arbitrage_gold.md) pour le détail des colonnes). Aucune donnée dupliquée : dbt interroge directement cette table via Trino, sans copie physique supplémentaire.

## Modèles créés

| Modèle | Type | Rôle |
|---|---|---|
| `stg_declarations.sql` | view | Passage 1:1 sur la source, renommage/convention dbt uniquement |
| `fct_arbitrage.sql` | table | Fait principal : une ligne par déclaration analysée + `EST_RECLASSIFIE` (flag dérivé `CODE_NGP <> CODE_NGP_INITIAL`, seule transformation analytique ajoutée) |
| `mart_arbitrage_kpi.sql` | table | KPI globaux (1 ligne) |
| `mart_arbitrage_par_ngp.sql` | table | KPI par CODE_NGP (3 lignes) |
| `mart_arbitrage_temps.sql` | table | KPI mensuels (25 lignes) |

**Modèles désactivés (pas supprimés)** : `stg_market_prices.sql` et `stg_product_matches.sql` — leur rôle prévu à l'origine (rapprochement prix/matching NGP séparé) est déjà réalisé en amont par Spark (Phases 2.13/2.14/2.21) et fusionné dans la Gold ; les recréer en dbt dupliquerait ce que Spark a déjà fait, ce que cette phase interdit explicitement. Chaque fichier contient `{{ config(enabled=false) }}` et un commentaire expliquant pourquoi.

### Pourquoi une granularité mensuelle pour `mart_arbitrage_temps`

`DATE_DEPOT` couvre ~2 ans (2024-08-21 à 2026-08-10) mais seulement 270 dates distinctes pour 338 lignes — un regroupement journalier donnerait presque exclusivement des jours à une seule déclaration, sans tendance exploitable. Vérifié réellement avant de construire le modèle (pas supposé) : 25 mois distincts, 11 à 25 déclarations/mois — une granularité mensuelle est la plus fine qui reste réellement analysable.

## Seuils d'arbitrage : non retouchés

Conformément à la consigne, dbt **n'recalcule pas** les seuils P10/P90 de la Phase 2.21 — `ARBITRAGE` est lu tel quel depuis la source Gold. Aucun seuil n'est redéfini dans dbt.

## Tests

**Structure** (`schema.yml`, tests génériques dbt) :
- `BADR_ID` : `not_null`, `unique` — **vérifié réellement avant de déclarer le test** (338 valeurs distinctes pour 338 lignes, requête Trino directe)
- `CODE_NGP` : `not_null`, `accepted_values` (3 codes du périmètre)
- `ARBITRAGE` : `not_null`, `accepted_values` (`NORMAL`/`MINORE`/`MAJORE`)
- `RATIO_UNITAIRE` : `not_null`
- `mart_arbitrage_par_ngp.CODE_NGP` : `not_null`, `unique`, `accepted_values`

**Domaine** : tous vérifiés réellement sur les 338 lignes avant d'écrire les tests (0 NULL, 0 valeur hors domaine).

**Tests singuliers** (`dbt/tests/*.sql`, complètent le stub `assert_positive_declared_value.sql` déjà présent) :
- `assert_positive_declared_value.sql` : `VALEUR_MAD > 0`
- `assert_positive_quantite.sql` : `QUANTITE > 0`
- `assert_positive_prix_reference.sql` : `PRIX_REFERENCE > 0`

**Résultat : 13/13 tests PASS.**

## Résultats obtenus (Trino, après `dbt run`)

**KPI globaux** (`mart_arbitrage_kpi`) : 338 déclarations · NORMAL 270 (79,88%) · MINORE 34 (10,06%) · MAJORE 34 (10,06%) · ratio moyen 1,3304 · ratio médian ≈1,033 (`approx_percentile`, voir limite ci-dessous) · valeur totale 44 590 579,73 MAD · quantité totale 18 861

**Par CODE_NGP** (`mart_arbitrage_par_ngp`) :

| CODE_NGP | n | NORMAL | MINORE | MAJORE | Taux minoration | Taux majoration | Ratio médian |
|---|---|---|---|---|---|---|---|
| 84713000 | 109 | 87 | 11 | 11 | 10,09% | 10,09% | 1,449 |
| 85171300 | 118 | 94 | 12 | 12 | 10,17% | 10,17% | 1,091 |
| 85287200 | 111 | 89 | 11 | 11 | 9,91% | 9,91% | 0,737 |

Ces chiffres correspondent **exactement** aux totaux/décomptes de la Gold Spark (Phase 2.21) : 338 lignes, 270/34/34, même répartition par CODE_NGP — aucune déclaration perdue entre Spark et dbt.

## Limites

- `ratio_median` (`mart_arbitrage_kpi`/marts) utilise `approx_percentile` (Trino), un algorithme **approché** — légère différence avec la médiane exacte calculée par Spark en Phase 2.20/2.21 (~1,033 vs 1,045). Documenté ici, pas un défaut de calcul.
- Le catalogue Iceberg REST (`iceberg-rest`) utilise un backend SQLite mono-écrivain (limite connue de l'image de référence Tabular) — des écritures concurrentes rapprochées peuvent produire une erreur `ICEBERG_COMMIT_ERROR` transitoire ; observé et résolu dans cette phase par un redémarrage simple du conteneur (aucune perte de données, le catalogue est reconstruit depuis les métadonnées déjà persistées sur S3/MinIO). À surveiller si le pipeline est industrialisé (DAG Airflow futur).
- `register_gold_iceberg.py` doit être ré-exécuté après chaque rafraîchissement de `arbitrage_gold.py` pour que dbt voie les données à jour (pas encore orchestré automatiquement — tâche pour la phase Airflow).
- La table Iceberg `iceberg.gold.arbitrage` est un `createOrReplace()` (aucun historique de versions Iceberg conservé au-delà de l'écriture courante).

## Utilisation future

- **Grafana** : pourra se connecter à Trino/Iceberg et lire directement `mart_arbitrage_kpi`, `mart_arbitrage_par_ngp`, `mart_arbitrage_temps` pour un dashboard, sans recalcul.
- **Chatbot LLM** (phase ultérieure) : interrogera ces marts (et `fct_arbitrage` pour le détail) pour répondre en langage naturel et expliquer un résultat déjà calculé — jamais recalculer le ratio, jamais réinventer un arbitrage, jamais halluciner une déclaration absente de Gold.
