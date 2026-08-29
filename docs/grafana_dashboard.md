# Dashboard Grafana (Phase 2.23)

Complète [`docs/dbt_gold.md`](dbt_gold.md). Dashboard d'arbitrage douanier construit uniquement sur la Gold analytique dbt — aucun recalcul métier.

> ⚠️ Les décomptes de validation cités plus bas (`338/270/34/34`, répartitions par CODE_NGP, `% MINORE`/`% MAJORE` = 10,06 %) datent de l'ancienne règle P10/P90. Le changement de règle du 2026-08-28 (seuil absolu 10 %) les rend caducs. Les **requêtes** des panels sont inchangées (elles lisent la colonne `ARBITRAGE` et agrègent) ; seuls les nombres attendus changent. Nouvelle référence : voir le run `adii_arbitrage` le plus récent.

## Infrastructure existante réutilisée (rien recréé)

Le service `grafana` (`docker-compose.yml`) était **déjà entièrement préconfiguré** pour ce cas d'usage, avant cette phase :
- `GF_INSTALL_PLUGINS: trino-datasource` — installe déjà le plugin Trino au démarrage (installé et persistant depuis le 8 août dans le volume nommé `grafana-data`, confirmé via les logs du conteneur).
- `./monitoring/grafana/provisioning` et `./monitoring/grafana/dashboards` déjà montés en lecture seule dans le conteneur — mécanisme de provisioning fichier déjà actif.
- `monitoring/grafana/provisioning/datasources/trino.yml` : datasource "Trino" **déjà provisionnée** (catalogue `iceberg`, schéma `gold`, `http://trino:8080`) — présente depuis le commit initial du projet, jamais modifiée dans cette phase (voir incident ci-dessous).
- `monitoring/grafana/provisioning/dashboards/dashboards.yml` : provider de dashboards déjà configuré, relit `/var/lib/grafana/dashboards` toutes les 30s.
- `monitoring/grafana/dashboards/adii_overview.json` : **squelette vide** (`"panels": []`) déjà présent dans le commit initial — cette phase le **peuple**, ne crée pas de fichier parallèle.

Aucune modification de `docker-compose.yml`, d'Airflow, de MinIO ou d'Iceberg n'a été nécessaire.

## Datasource Grafana utilisée

**Trino** (`type: trino-datasource`, uid `PD73EC80E50767796`, déjà provisionnée) — `catalog: iceberg`, `schema: gold` par défaut. Toutes les requêtes du dashboard qualifient néanmoins explicitement `iceberg.gold.<table>` (plus robuste, indépendant du schéma par défaut de la session).

### Incident et correction (limité, documenté)

Une tentative de fixer un `uid` explicite (`trino`) dans `trino.yml`, pour rendre le dashboard indépendant de l'UID auto-généré, a fait échouer le démarrage complet de Grafana ("Datasource provisioning error: data source not found" — le provisioner ne migre pas proprement un changement d'UID sur une datasource déjà existante dans ce conteneur). **Annulé immédiatement** : `trino.yml` restauré à son état d'origine (aucune différence git), conteneur redémarré, état sain confirmé. Le dashboard référence donc l'UID auto-généré existant (`PD73EC80E50767796`), stable tant que la datasource n'est pas supprimée/recréée.

Le mot de passe admin Grafana stocké (volume persistant depuis le 8 août) ne correspondait plus à `.env` (`admin`/`admin`) ; réinitialisé via `grafana cli admin reset-admin-password admin` (commande standard, ne touche qu'à l'authentification interne de Grafana, aucune donnée de projet).

## Tables dbt utilisées

Toutes en lecture seule, via `iceberg.gold.*` (aucun recalcul, aucune duplication physique) :
- `mart_arbitrage_kpi` (1 ligne) — KPI globaux
- `fct_arbitrage` (338 lignes) — détail filtrable (période, CODE_NGP, verdict)
- `mart_arbitrage_temps` (25 lignes) — série mensuelle, utilisée telle quelle pour le panel d'évolution temporelle (pas de recalcul en dbt ni en Grafana)

## Dashboard créé

**"ADII Overview"** (uid `adii-overview`, fichier `monitoring/grafana/dashboards/adii_overview.json`), 15 panels.

### KPI principaux (9 panels stat, ligne 1-3, données globales non filtrées, source `mart_arbitrage_kpi`)

| Panel | Requête |
|---|---|
| Nombre total de déclarations | `SELECT nb_declarations AS value FROM iceberg.gold.mart_arbitrage_kpi` |
| NORMAL | `SELECT nb_normal AS value FROM iceberg.gold.mart_arbitrage_kpi` |
| MINORE | `SELECT nb_minore AS value FROM iceberg.gold.mart_arbitrage_kpi` |
| MAJORE | `SELECT nb_majore AS value FROM iceberg.gold.mart_arbitrage_kpi` |
| % MINORE | `SELECT pct_minore AS value FROM iceberg.gold.mart_arbitrage_kpi` |
| % MAJORE | `SELECT pct_majore AS value FROM iceberg.gold.mart_arbitrage_kpi` |
| Ratio unitaire moyen | `SELECT ratio_moyen AS value FROM iceberg.gold.mart_arbitrage_kpi` |
| Valeur totale (MAD) | `SELECT valeur_totale_mad AS value FROM iceberg.gold.mart_arbitrage_kpi` |
| Quantité totale | `SELECT quantite_totale AS value FROM iceberg.gold.mart_arbitrage_kpi` |

Volontairement **non filtrés** par les variables de filtre : ce sont les totaux de référence validés en Phase 2.22 (338/270/34/34), affichés tels quels pour servir de repère fixe pendant qu'on explore les visualisations filtrées en dessous.

### Visualisations (6 panels, filtrables sauf mention contraire, source `fct_arbitrage`)

1. **Répartition NORMAL/MINORE/MAJORE** (piechart) — `SELECT ARBITRAGE, count(*) AS nb FROM iceberg.gold.fct_arbitrage WHERE DATE_DEPOT BETWEEN date($__timeFrom()) AND date($__timeTo()) AND CODE_NGP IN (${code_ngp:sqlstring}) AND ARBITRAGE IN (${arbitrage:sqlstring}) GROUP BY ARBITRAGE`
2. **Répartition des arbitrages par CODE_NGP** (barchart groupé) — même filtre, agrégation pivotée avec `count(*) FILTER (WHERE ARBITRAGE = '...')` par code
3. **Ratio unitaire moyen par CODE_NGP** (barchart) — `avg(RATIO_UNITAIRE)` groupé par `CODE_NGP`, même filtre
4. **Valeur déclarée totale par CODE_NGP (MAD)** (barchart) — `sum(VALEUR_MAD)` groupé, même filtre
5. **Quantité totale par CODE_NGP** (barchart) — `sum(QUANTITE)` groupé, même filtre
6. **Évolution temporelle des arbitrages** (timeseries, empilé) — **depuis `mart_arbitrage_temps`** (pas de recalcul), filtré uniquement par période (`mois BETWEEN date($__timeFrom()) AND date($__timeTo())`) ; pas de filtre CODE_NGP possible ici car ce mart n'a pas cette dimension (choix cohérent avec la structure dbt existante, pas contourné).

## Filtres

- **Période** : sélecteur de plage temporelle natif de Grafana (haut-droite du dashboard), appliqué via les macros `$__timeFrom()`/`$__timeTo()` — **vérifiées fonctionnelles** avec ce plugin (`trino-datasource` v1.0.11) avant utilisation. Plage par défaut du dashboard : 2024-08-01 → 2026-08-31 (couvre l'intégralité de `DATE_DEPOT`, 2024-08-21 à 2026-08-10).
- **CODE_NGP** : variable de dashboard `$code_ngp` (multi-sélection, requête `SELECT DISTINCT CODE_NGP FROM iceberg.gold.fct_arbitrage ORDER BY 1`, "All" par défaut).
- **Verdict d'arbitrage** : variable `$arbitrage` (multi-sélection, requête `SELECT DISTINCT ARBITRAGE FROM iceberg.gold.fct_arbitrage ORDER BY 1`, "All" par défaut).

## Validations effectuées

Chaque requête a été testée à **trois niveaux** avant intégration au dashboard :
1. Directement dans Trino (CLI, `docker exec trino trino --execute ...`)
2. À travers le moteur de requête réel de Grafana (`POST /api/ds/query`, datasource Trino, macros de temps réellement résolues)
3. Le dashboard complet rechargé par le provisioning Grafana et son contenu relu via l'API (`GET /api/dashboards/uid/adii-overview`) pour confirmer que les 15 panels sont bien chargés avec la bonne configuration

| Vérification | Résultat |
|---|---|
| Total déclarations | 338 ✓ |
| NORMAL | 270 ✓ |
| MINORE | 34 ✓ |
| MAJORE | 34 ✓ |
| Répartition par CODE_NGP (NORMAL/MINORE/MAJORE) | 87/11/11, 94/12/12, 89/11/11 — identique à la Phase 2.22 ✓ |
| Ratio moyen par CODE_NGP | 1,6565 / 1,4290 / 0,9054 — identique à la Phase 2.22 ✓ |
| Valeur totale MAD par CODE_NGP | 14 463 523,72 / 17 549 326,37 / 12 577 729,64 — identique ✓ |
| Quantité totale par CODE_NGP | 3001 / 8228 / 7632 — identique ✓ |
| Filtre CODE_NGP+ARBITRAGE fonctionnel | testé avec `CODE_NGP=85171300, ARBITRAGE=MINORE` → 12 lignes via Grafana **et** via Trino direct — identique ✓ |
| Panel temporel (`mart_arbitrage_temps`) | colonne `mois` correctement détectée comme champ temporel par le plugin (format `timeseries`) ✓ |
| Aucune ligne perdue | source = `fct_arbitrage` (338 lignes, Phase 2.22), aucune jointure/filtre supplémentaire dans les requêtes Grafana |
| Aucun KPI recalculé différemment de la Gold | tous les KPI globaux lisent `mart_arbitrage_kpi` tel quel ; les agrégations par CODE_NGP dans les visualisations utilisent les mêmes colonnes (`RATIO_UNITAIRE`, `VALEUR_MAD`, `QUANTITE`, `ARBITRAGE`) que `mart_arbitrage_par_ngp`, sans changer la logique de calcul |

## Accès au dashboard

`http://localhost:3000` (identifiants `.env` : `admin`/`admin`) → dashboard **"ADII Overview"**.

## Correction Phase 2.23.1 — panels `% MINORE` / `% MAJORE`

**Cause du "No data"** : `pct_minore`/`pct_majore` (`mart_arbitrage_kpi`, dbt, non modifié) sont calculés comme `100.0 * count(*) / count(*)` — Trino renvoie ce résultat en `DECIMAL` haute précision (ex. `10.0591715976331361`), que le plugin `trino-datasource` sérialise en champ **string** plutôt que numérique. Un panel `stat` avec `reduceOptions.calcs: ["lastNotNull"]` ne peut pas réduire un champ non numérique → "No data". Les 7 autres KPI n'ont pas ce problème car leurs colonnes source sont déjà `BIGINT`/`DOUBLE` côté Trino.

**Correction** (uniquement dans les requêtes des 2 panels concernés, `monitoring/grafana/dashboards/adii_overview.json`, aucun modèle dbt touché) :
```sql
-- avant
SELECT pct_minore AS value FROM iceberg.gold.mart_arbitrage_kpi
-- apres
SELECT CAST(pct_minore AS DOUBLE) AS value FROM iceberg.gold.mart_arbitrage_kpi
```
Idem pour `pct_majore`. Revalidé via le moteur de requête réel de Grafana : `% MINORE` = 10,06 %, `% MAJORE` = 10,06 %, type de champ `number`/`float64`.

**Période par défaut resserrée** : la vraie plage de `DATE_DEPOT` dans Gold a été revérifiée (`2024-08-21` à `2026-08-10`, inchangée depuis la Phase 2.22) ; la période par défaut du dashboard est passée de `2024-08-01 → 2026-08-31` à exactement `2024-08-21 → 2026-08-10`. Revalidé : les 3 totaux ARBITRAGE restent 270/34/34 avec cette plage resserrée (aucune donnée coupée), et les filtres CODE_NGP/verdict restent fonctionnels (testé `CODE_NGP=84713000 + MAJORE` → 11, identique à Trino direct).

## Limites

- Le dashboard référence l'UID auto-généré de la datasource (`PD73EC80E50767796`) plutôt qu'un UID fixe : si la datasource est un jour supprimée puis recréée (volume `grafana-data` effacé), l'UID changera et les références dans `adii_overview.json` devront être mises à jour (limite documentée, pas contournée pour éviter le risque de casse rencontré lors de la tentative de fixation).
- Les 9 KPI globaux ne sont pas filtrables par `$code_ngp`/`$arbitrage`/période (choix assumé, voir plus haut) — seules les 6 visualisations le sont.
- Le panel temporel n'est filtrable que par période, pas par CODE_NGP (limite structurelle de `mart_arbitrage_temps`, qui n'a pas cette dimension — non contournée pour éviter de dupliquer un calcul déjà fait par dbt).
- Pas de rafraîchissement automatique configuré (`refresh: ""`) — cohérent avec des données Gold rafraîchies manuellement (Spark → `register_gold_iceberg.py` → dbt), pas un flux temps réel.
