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

## ⚠️ Dépannage — Catalogue Iceberg illisible (`Failed to load table`)

**Symptôme** : Trino / dbt / le chatbot renvoient une erreur du type
`Failed to load table: arbitrage in gold namespace`, alors que
`SHOW TABLES` liste bien la table.

**Cause** : le catalogue REST Iceberg (service `iceberg-rest`) stocke ses
métadonnées dans un fichier SQLite **sans aucun volume Docker**
(`/tmp/iceberg_rest_mode=memory` à l'intérieur du conteneur — voir
`docker-compose.yml`, service `iceberg-rest`, pas de bloc `volumes:`).
Ce fichier survit à un simple `docker restart` mais est **perdu à chaque
recréation du conteneur** (`docker-compose down` puis `up`,
`--force-recreate`, etc.). Si une écriture Iceberg est interrompue à ce
moment-là, le catalogue peut garder un pointeur vers un fichier de
métadonnées qui n'existe plus sur MinIO/S3.

**Procédure de récupération** (testée et confirmée le 23/08/2026) :

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
   Doit retourner `338`.

**Si c'est une table dbt** (`fct_arbitrage`, `mart_arbitrage_kpi`, etc.)
plutôt que la table source `arbitrage` : même étape 1 (adapter le nom),
puis à l'étape 2 relancer dbt au lieu du spark-submit :
```bash
docker exec dataplatformadii-dbt-1 dbt run --threads 1
```

**Cause racine — pas encore corrigée** : `iceberg-rest` n'a aucun volume
Docker. Son catalogue est perdu à **chaque recréation complète de la
stack**, pas seulement lors d'un incident isolé — un `docker-compose down`
puis `up` oblige à tout reconstruire (`register_gold_iceberg.py` + `dbt
run`) avant que Trino/Grafana/le chatbot ne revoient les données. Piste de
correction durable (non implémentée) : ajouter un volume nommé sur
`iceberg-rest`, ou migrer vers un backend de catalogue avec une base
persistée.

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
