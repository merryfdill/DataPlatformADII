# DataPlatformADII

MVP Data Engineering platform for customs declaration valuation
and risk detection.

## Architecture

BADR + Web Scraping
        ↓
     Airflow
        ↓
      Bronze
        ↓
      MinIO
        ↓
      Spark
        ↓
     Silver
        ↓
     Iceberg
        ↓
      dbt
        ↓
      Gold
        ↓
     Trino
        ↓
     Grafana

## ⚠️ Dépannage — `register_gold_iceberg` / lecture Gold Iceberg

Deux pannes distinctes ont existé sur `iceberg.gold.arbitrage`. La première
est **corrigée**, la seconde est une **limite structurelle du catalogue
SQLite** avec un contournement simple.

### Cas 1 — `NotFoundException: Location does not exist` (CORRIGÉ le 2026-08-29)

`arbitrage_gold.py` écrivait son Parquet avec `.write.mode("overwrite")`
directement dans `s3://datalake/gold/arbitrage/` — qui est la *location* de
la table Iceberg. `overwrite` supprime le préfixe cible avant d'écrire → à
**chaque run** il effaçait `data/` + `metadata/` de la table, et le
catalogue pointait vers un `metadata.json` disparu (reproduit 4 fois).
**Corrigé** : `arbitrage_gold.py` écrit dans `gold/arbitrage_staging/`,
`register_gold_iceberg.py` lit de là ; le préfixe de la table n'est plus
touché que par les commits atomiques d'Iceberg. Les 2 fichiers orphelins
au root de `gold/arbitrage/` (~50 Ko) sont inertes. Si ça se reproduisait
pour une autre raison, la procédure de récupération plus bas s'applique.

### Cas 2 — `SQLITE_BUSY` / `ServiceFailureException: 500: Unknown failure` (LIMITE CONNUE)

**Symptôme** : `register_gold_iceberg` (ou une tâche dbt) échoue ; les logs
d'`iceberg-rest` montrent `org.sqlite.SQLiteException: [SQLITE_BUSY] The
database file is locked`.

**Cause** : le catalogue est un **SQLite mono-écrivain**
(`iceberg-rest`, `busy_timeout=30000`). Il tient un run séquentiel normal
(`arbitrage → register → dbt_run → dbt_test`, un écrivain à la fois), mais
une connexion peut rester **verrouillée dans le process `iceberg-rest`**
après un spark-submit tué, un retry en échec, ou une transaction étirée par
la pression mémoire. `busy_timeout` ne récupère pas d'une connexion coincée
dans le même process, et il n'y a pas d'auto-guérison.

**Contournement** (30 s, aucune perte — le catalogue est sur volume) :
```bash
docker restart dataplatformadii-iceberg-rest-1
# puis relancer la tâche en echec (UI "Clear", ou re-trigger du DAG)
```

**Non fait, volontairement** : migration du catalogue vers Postgres (élimine
le mono-écrivain). Hors périmètre à ce stade — le contournement suffit pour
la démo, à condition de ne pas empiler les runs ni tuer de spark-submit.

### Procédure de récupération (cas 1, si jamais il récidive)

Testée le 23/08/2026 :

1. Supprimer l'entrée de catalogue cassée directement via l'API REST du
   catalogue — **ne pas** utiliser `DROP TABLE` via Spark/Trino, ça échoue
   avec la même erreur (ces clients chargent l'état actuel avant de
   supprimer) :
   ```bash
   docker exec dataplatformadii-trino-1 curl -s -X DELETE \
     "http://iceberg-rest:8181/v1/namespaces/gold/tables/arbitrage" \
     -w "\nHTTP_STATUS=%{http_code}\n"
   ```
   (`HTTP_STATUS=200` confirme la suppression. Remplacer `arbitrage` par le
   nom de la table concernée si ce n'est pas celle-ci.)

2. Relancer le job qui écrit cette table — aucune donnée source n'est
   perdue, il ré-écrit juste la table Iceberg à partir du Parquet déjà
   calculé :
   ```bash
   docker exec dataplatformadii-spark-iceberg-1 spark-submit \
     --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
     --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
     --conf spark.hadoop.fs.s3a.access.key=minioadmin \
     --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
     --conf spark.hadoop.fs.s3a.path.style.access=true \
     --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
     --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
     --conf spark.sql.catalog.iceberg.s3.path-style-access=true \
     /home/iceberg/jobs/register_gold_iceberg.py
   ```

3. Vérifier :
   ```bash
   docker exec dataplatformadii-trino-1 trino --catalog iceberg --schema gold --execute "SELECT count(*) FROM arbitrage"
   ```
   Doit retourner le volume du dernier run `adii_arbitrage` (~345, 2026-08-29 ;
   ce nombre bouge à chaque run — la population BADR grandit via
   `adii_daily_ingestion` et `date_fin` par défaut = aujourd'hui).

**Si c'est une table dbt** (`fct_arbitrage`, `mart_arbitrage_kpi`, etc.)
plutôt que la table source `arbitrage` : même étape 1 (adapter le nom),
puis à l'étape 2 relancer dbt au lieu du spark-submit :
```bash
docker exec dataplatformadii-dbt-1 dbt run --threads 1
```

**Risque résiduel** : le SQLite du catalogue reste mono-écrivain (d'où
`busy_timeout=30000`). Migration vers un backend Postgres : non faite, non
prioritaire. La cause principale (le job qui écrasait sa propre table) est
éliminée — `adii_arbitrage` doit désormais passer `register_gold_iceberg`
du premier coup, sans `DELETE` manuel.

## ⚠️ Dépannage — les tâches Airflow meurent après « Pre Execute » sans erreur

**Symptôme** : une tâche (ex. `check_environment`) passe en `Up For Retry`
puis `Failed` après 3 essais. Le log de la tâche s'arrête net après
`::group::Pre Execute` — aucun traceback, aucune erreur. Le vrai message
n'est **pas** dans le log de la tâche mais dans `docker logs
dataplatformadii-airflow-scheduler-1` :
`httpx.ConnectError: [Errno 111] Connection refused` ou
`airflow.sdk.api.client.ServerResponseError: Invalid auth token`, suivi de
`Process exited ... signal_sent=SIGKILL`.

**Ce n'est PAS un OOM** (`docker inspect` du scheduler :
`OOMKilled=false`, `RestartCount=0` ; `dmesg` de la VM WSL : aucune ligne
`oom-killer` ; l'API server répond en ~4 ms). C'est purement de la
config.

**Cause** : depuis la séparation `airflow-webserver` / `airflow-scheduler`
/ `airflow-dag-processor` (13/08/2026), le scheduler d'Airflow 3
(LocalExecutor) fork des workers dont le superviseur doit appeler
l'**Execution API** de l'`api-server` pour enregistrer chaque tâche. Deux
variables, non définies par défaut, cassent ce dialogue :

| Variable | Défaut cassé | Effet |
|---|---|---|
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | `http://localhost:8080/execution/` — rien n'écoute sur `:8080` dans le conteneur scheduler | `Connection refused` |
| `AIRFLOW__API_AUTH__JWT_SECRET` | clé aléatoire **régénérée par process** (`get_signing_key`, `tokens.py:558`) → scheduler et api-server ont des clés différentes | `Invalid auth token` |

Les deux sont **latentes** : elles ne se manifestent qu'à la **recréation
des conteneurs** (`down`+`up`, `--force-recreate`, changement de compose)
et réapparaîtront si les variables disparaissent du `docker-compose.yml`
ou du `.env`.

**Correctif** (déjà appliqué dans `docker-compose.yml`, bloc
`x-airflow-common → environment`) :

```yaml
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-webserver:8080/execution/
AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET}   # défini dans .env, identique pour TOUS les conteneurs airflow
AIRFLOW__API__BASE_URL: http://localhost:8082          # port hôte publié par airflow-webserver (8080 = Trino, 8083 = Spark)
```

Après modif : `docker compose up -d` (recrée les conteneurs airflow),
attendre que `docker exec dataplatformadii-airflow-scheduler-1 curl -s
http://airflow-webserver:8080/execution/health` renvoie
`{"status":"healthy"}`, puis relancer le DAG.

**Vérification rapide de l'état sain** :
```bash
docker exec dataplatformadii-airflow-scheduler-1 \
  python -c "from airflow.executors.base_executor import get_execution_api_server_url as g; print(g())"
# -> http://airflow-webserver:8080/execution/   (et NON http://localhost:8080/...)
```

### Troisième bug, même famille — `KeyError: 'logical_date'` / `'ds'` sur un déclenchement manuel

Une fois les deux variables ci-dessus corrigées, la tâche s'exécute enfin
pour de vrai — et échoue alors avec `KeyError: 'logical_date'` (ou `'ds'`)
dans le log **de la tâche** cette fois.

**Cause** : Airflow 3 n'ajoute `logical_date` / `ds` / `ts` /
`data_interval_*` au contexte de tâche **que si `dag_run.logical_date`
n'est pas None** (`task_runner.py` :
`if logical_date := coerce_datetime(dag_run.logical_date):`). Un run lancé
par le bouton **Trigger** de l'UI (sans config) ou par
`airflow dags trigger` sans `--logical-date` a `logical_date = NULL` →
`context["logical_date"]` et `context["ds"]` lèvent `KeyError`. Les runs
**planifiés** (cron) ne sont pas touchés. Même profil que les deux
précédents : latent, invisible jusqu'à ce qu'on atteigne la ligne.

**Correctif** (appliqué) : `airflow/dags/utils/run_context.py` —
`run_logical_date(context)` renvoie `context.get("logical_date") or
context["dag_run"].run_after` (jamais `now()` : `run_after` est traçable
au run), et `run_ds(context)` formate **le même** timestamp pour que
`logical_date` et `ds` ne puissent pas diverger. Points remplacés :
`daily_ingestion.py` (6 : `check_environment` + les 5 tâches qui lisaient
`context["ds"]`), `arbitrage.py` (2 : `check_environment`,
`collect_metrics`). `daily_report.py` n'utilise aucune clé d'intervalle.

### Quatrième bug, même famille — `metrics.py` ne peut plus joindre Postgres

Symptôme : `ProgrammingError: invalid dsn: missing "=" after
"airflow-db-not-allowed:///"` dans `utils/metrics.py`. L'erreur est
capturée (le run continue) mais **aucune ligne n'est écrite dans
`reporting.*`** → le rapport Slack de 07:00 n'a rien à lire.

**Cause** : Airflow 3 remplace volontairement
`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` par `airflow-db-not-allowed:///`
dans l'environnement des sous-processus de tâche (une tâche ne doit pas
toucher la base de métadonnées directement). `metrics.py` dérivait sa DSN
de cette variable — impossible désormais.

**Correctif** (appliqué) : `_get_connection()` construit une connexion
explicite depuis `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB`
(host `postgres`, ou `REPORTING_DB_HOST` si défini). Ces trois variables
ont été ajoutées au bloc `x-airflow-common → environment` du
`docker-compose.yml` (elles n'y étaient pas — seule la DSN complète l'était,
via substitution `${...}` au parse). `reporting.*` vit dans la même base
Postgres (`airflow`), schéma `reporting`.

### Limite connue (NON corrigée) — un run qui échoue tôt ne laisse aucune métrique

`reporting.task_runs`, `business_metrics`, `data_quality_runs` et
`scraping_counts` ont toutes une FK `run_id → reporting.pipeline_runs(run_id)`.
La ligne parente dans `pipeline_runs` n'est créée que par `start_run()`,
appelée uniquement par la tâche `check_environment`. Si `check_environment`
meurt **avant** d'avoir exécuté `start_run()` (timeout, SIGKILL, Postgres
injoignable à cet instant précis, ou une tâche aval relancée isolément),
alors :

- chaque tâche suivante fait un INSERT dans `task_runs` → `ForeignKeyViolation`,
  capturée et logguée dans `_record_task_result()`, **métrique perdue** ;
- `get_latest_run()` lit `pipeline_runs` → le run **n'apparaît pas du tout**
  dans le rapport Slack de 07:00. Un pipeline entièrement en échec devient
  invisible — exactement le cas où on voudrait un rapport.

Vérifié : le run échoué `manual__2026-08-26T18:00` a 0 ligne dans les
5 tables `reporting.*`.

**Correctif recommandé (option A, ~10 lignes, pas de migration de schéma)** :
`record_task_run()` et `finish_run()` créent la ligne parente si absente —
`INSERT INTO pipeline_runs (run_id, logical_date, global_status)
VALUES (%s, %s, 'UNKNOWN') ON CONFLICT (run_id) DO NOTHING` — avec
`logical_date = run_logical_date(context)` et `dag_id` passés depuis
`_record_task_result()`. Le `ON CONFLICT (run_id) DO UPDATE` déjà présent
dans `start_run()` promeut ensuite la ligne stub si `check_environment`
finit par tourner ; `finish_run()` pose le statut final. FK conservée,
rapport voit tous les runs. Écarté : dropper la FK (le run reste invisible
au rapport, seul le bruit disparaît) ; sortir `start_run()` dans une tâche
dédiée (change la structure du DAG pour un gain marginal).

## Main technologies

- Apache Airflow
- Apache Spark / PySpark
- MinIO
- Apache Iceberg
- dbt
- Trino
- Grafana
- Docker

## Structure

- airflow/          Orchestration
- ingestion/        BADR and web ingestion
- spark/            Data processing
- dbt/              Transformations and tests
- infrastructure/   MinIO, Iceberg and Trino
- monitoring/       Grafana
- data/             Local temporary data
- notebooks/        Exploration and ML

## Règle d'arbitrage NORMAL / MINORÉ / MAJORÉ

Seuil **absolu et symétrique** autour du prix de référence, calculé par
`spark/jobs/arbitrage_gold.py` (règle métier `if/else`, pas d'IA) :

```
RATIO_UNITAIRE < 1 − SEUIL_MINORE_PCT/100   → MINORÉ
bande centrale                              → NORMAL
RATIO_UNITAIRE > 1 + SEUIL_MAJORE_PCT/100   → MAJORÉ
```

où `RATIO_UNITAIRE = (VALEUR_MAD / QUANTITE) / PRIX_REFERENCE`.

Le seuil est **isolé dans deux variables d'environnement** — par défaut 10 %
de chaque côté (bande NORMAL `[0,90 ; 1,10]`) :

```
.env                ARBITRAGE_SEUIL_MINORE_PCT=10
                    ARBITRAGE_SEUIL_MAJORE_PCT=10
docker-compose.yml  passées au service spark-iceberg
```

Changer le seuil = éditer `.env`, `docker compose up -d spark-iceberg`,
relancer le DAG `adii_arbitrage`. Aucun code, aucune image à reconstruire.
Les bornes effectives sont loguées au démarrage de la tâche `arbitrage`.

Détail et justification (remplace l'ancienne règle P10/P90) :
[`docs/arbitrage_gold.md`](docs/arbitrage_gold.md).

### ⚠️ Limite à connaître pour la soutenance — référence non calibrée

Avec la règle absolue ±10 %, la distribution obtenue est de l'ordre de
**NORMAL ~11 % · MINORÉ ~45 % · MAJORÉ ~44 %** (~345 déclarations ; les
décomptes exacts bougent à chaque run — `PRIX_REFERENCE` est recalculé).
Seulement ~11 % en NORMAL, c'est économiquement surprenant. Ce n'est **pas
un défaut de la règle** :

- **Globalement** la référence est à peu près centrée : médiane du ratio
  ≈ **0,98** (≈ 1), ~51 % des déclarations sous la référence / ~49 %
  au-dessus. L'hypothèse « prix retail Jumia systématiquement trop haut »
  n'apparaît pas dans l'agrégat et n'est pas directionnelle.
- **Par catégorie**, les échelles ne correspondent pas : médianes du ratio
  à ≈ **0,5** (téléviseurs) / ≈ **1,3** (smartphones) / ≈ **1,5** (PC
  portables) — loin de 1, dans des sens opposés. BADR (Faker) et les prix
  Jumia sont générés indépendamment ; les deux biais opposés se compensent
  à l'agrégat.
- **Retail vs CIF** : un prix Jumia inclut marge + TVA + transport local,
  une valeur en douane est un prix CIF plus bas. Réel en production,
  mais ici secondaire par rapport au décalage d'échelle ci-dessus.
- **P10/P90 masquait tout ça** en re-centrant sur chaque catégorie à
  chaque run (toujours 10/80/10). La règle absolue le **révèle**.

Sur données simulées, les proportions démontrent que le mécanisme
fonctionne, ce ne sont pas des taux de fraude. Sur données réelles,
`PRIX_REFERENCE` devrait d'abord être calibré sur la valeur en douane
(par catégorie). Analyse complète : [`docs/arbitrage_gold.md`](docs/arbitrage_gold.md).
