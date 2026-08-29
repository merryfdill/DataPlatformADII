# Arbitrage et couche Gold

Complète [`docs/ratio_unitaire.md`](ratio_unitaire.md). Première couche Gold du projet — résultat métier final, qui alimente ensuite dbt, le dashboard Grafana et le chatbot.

> **Changement de règle (2026-08-28).** La règle P10/P90 par CODE_NGP (prototype des phases 2.20/2.21) est remplacée par un **seuil absolu symétrique** fourni par l'encadrant Douane. Toutes les déclarations historiques sont reclassées — attendu et voulu. L'ancien test de non-régression `338/270/34/34` est caduc.

## Architecture

- **IA 1** (`models/ngp_classifier.joblib`) : classification du CODE_NGP des produits scrapés. Rôle figé — non concernée par ce changement.
- **RATIO_UNITAIRE** : rapport entre la valeur déclarée BADR ramenée à l'unité et le prix de référence retail. Voir [`docs/ratio_unitaire.md`](ratio_unitaire.md).
- **Arbitrage NORMAL/MINORE/MAJORE** : **règle métier explicable**, pas une IA. Aucun Isolation Forest, aucun classifieur d'anomalie, aucun `.joblib`.
- **Chatbot** : interroge et explique les résultats Gold, ne recalcule jamais le verdict.

```
Silver badr_ratio_unitaire  ─┐
                              ├─► arbitrage_gold.py ─► gold/arbitrage_staging/ (Parquet)
Silver badr (PAYS, CODE_NGP_INITIAL, VALEUR) ─┘         │
                                                        ▼
                              register_gold_iceberg.py ─► iceberg.gold.arbitrage (table Iceberg, location gold/arbitrage/)
                                                        │
                                                        ▼
                                                     dbt (fct_arbitrage, marts) ─► Trino ─► Grafana / chatbot
```

**Chemin de staging (correctif 2026-08-29).** `arbitrage_gold.py` écrit un Parquet brut dans `gold/arbitrage_staging/`, PAS dans `gold/arbitrage/` qui est la *location* de la table Iceberg. Raison : `.write.mode("overwrite")` supprime le préfixe cible avant d'écrire — pointé sur `gold/arbitrage/` il effaçait `data/` + `metadata/` de la table Iceberg à chaque run, laissant le catalogue vers un `metadata.json` disparu (« NotFoundException: Location does not exist », 4 occurrences). Seul `register_gold_iceberg.py` lit le Parquet de staging ; tout l'aval (dbt, Grafana, chatbot) lit la table Iceberg via Trino.

## Formule du ratio (inchangée)

```
VALEUR_UNITAIRE_MAD = VALEUR_MAD / QUANTITE
RATIO_UNITAIRE      = VALEUR_UNITAIRE_MAD / PRIX_REFERENCE
```

`RATIO_UNITAIRE = 1` → la valeur déclarée par unité est exactement le prix de référence retail de la catégorie. `< 1` → déclaré **sous** la référence. `> 1` → déclaré **au-dessus**.

## Règle d'arbitrage (seuil absolu)

```
RATIO_UNITAIRE < BORNE_BASSE               → MINORE   (déclaré à plus de X % sous la référence)
BORNE_BASSE ≤ RATIO_UNITAIRE ≤ BORNE_HAUTE → NORMAL
RATIO_UNITAIRE > BORNE_HAUTE               → MAJORE   (déclaré à plus de X % au-dessus)

BORNE_BASSE = 1 − ARBITRAGE_SEUIL_MINORE_PCT / 100
BORNE_HAUTE = 1 + ARBITRAGE_SEUIL_MAJORE_PCT / 100
```

**Par défaut : 10 % de chaque côté → bande NORMAL `[0,90 ; 1,10]`.**

Chaque déclaration est jugée **par rapport à son propre prix de référence**, jamais par rapport aux autres déclarations. Contrairement aux percentiles P10/P90, ce seuil ne se déplace pas quand la population grandit : une déclaration à `ratio = 0,85` est MINORÉ aujourd'hui et le restera, quels que soient les volumes ajoutés ensuite.

### Isolation du seuil

`ARBITRAGE_SEUIL_MINORE_PCT` et `ARBITRAGE_SEUIL_MAJORE_PCT` sont, du plus externe au plus interne :

| Niveau | Fichier | Contenu |
|---|---|---|
| Valeur | `.env` (et `.env.example`) | `ARBITRAGE_SEUIL_MINORE_PCT=10` / `ARBITRAGE_SEUIL_MAJORE_PCT=10` |
| Injection | `docker-compose.yml`, service `spark-iceberg`, bloc `environment:` | `${ARBITRAGE_SEUIL_MINORE_PCT:-10}` / `${ARBITRAGE_SEUIL_MAJORE_PCT:-10}` |
| Lecture | `spark/jobs/arbitrage_gold.py` | `float(os.environ.get("ARBITRAGE_SEUIL_MINORE_PCT", "10")) / 100` → `BORNE_BASSE = 1 - …` |

**Passer de 10 à 15 %** = éditer une ligne de `.env`, `docker compose up -d spark-iceberg`, relancer `adii_arbitrage`. Aucun code touché, aucune image reconstruite (`spark/jobs/` est bind-mounté).

**Deux variables** même si elles valent 10 aujourd'hui : l'encadrant peut vouloir un seuil de minoration plus strict que celui de majoration (ou l'inverse). Défaut identique des deux côtés.

Les bornes effectives sont **loguées au démarrage du job** (visibles dans le log de la tâche `arbitrage` d'Airflow) :

```
Regle d'arbitrage appliquee (seuil absolu, isole dans ARBITRAGE_SEUIL_*_PCT) :
  ARBITRAGE_SEUIL_MINORE_PCT = 10.0 %  ->  borne basse NORMAL = 0.900000
  ARBITRAGE_SEUIL_MAJORE_PCT = 10.0 %  ->  borne haute NORMAL = 1.100000
    RATIO_UNITAIRE < 0.900000                    -> MINORE
    0.900000 <= RATIO_UNITAIRE <= 1.100000 -> NORMAL
    RATIO_UNITAIRE > 1.100000                    -> MAJORE
```

### ⚠️ Ce n'est pas un barème douanier officiel de production

Le chiffre de 10 % est la règle énoncée par l'encadrant, mais l'ensemble du pipeline reste une **démonstration sur données simulées** : `data/badr.db` est généré avec Faker et `PRIX_REFERENCE` provient d'un scraping Jumia. Les résultats illustrent une méthode, pas un contrôle douanier réel.

### Pourquoi ce changement (P10/P90 → absolu)

| | P10/P90 par CODE_NGP (ancien) | Seuil absolu ±X % (nouveau) |
|---|---|---|
| Référence du jugement | la population elle-même | le prix de référence propre à chaque déclaration |
| Proportion signalée | toujours ~10 % / ~10 % par construction | varie réellement selon les données |
| Stabilité dans le temps | le seuil bouge à chaque ajout de déclarations → reclasse des lots déjà jugés | le seuil ne bouge pas |
| Modifiable | non (recalculé) | oui, une variable d'environnement |

Ré-exécuter `adii_arbitrage` reclasse quand même d'anciens lots — non plus à cause du percentile, mais parce que `PRIX_REFERENCE` (médiane des prix scrapés **de la période**) varie d'un run à l'autre. C'est la raison pour laquelle l'arbitrage reste **manuel et borné par une période** : on ne rejuge pas un lot déjà arbitré.

## Périmètre : déclarations sans prix de référence

Seuls les 3 codes NGP scrapés ont un `PRIX_REFERENCE`. Les ~4 700 déclarations BADR des 31 autres codes n'ont **pas de ratio** → elles sont écartées en amont par `ratio_unitaire.py`, **n'obtiennent aucun verdict** et **restent hors Gold**. Le rapport et les tâches `data_quality_*` les comptent explicitement comme `rows_out_of_scope = population − gold_rows`. Ce changement de règle ne crée pas de verdict « hors périmètre » — statu quo.

## Couche Gold

Parquet de staging : `s3://datalake/gold/arbitrage_staging/` (écrit par `arbitrage_gold.py`, lu par `register_gold_iceberg.py`).
Table Iceberg exposée : `iceberg.gold.arbitrage`, *location* `s3://datalake/gold/arbitrage/` (gérée par Iceberg seul).

| Colonne | Source |
|---|---|
| BADR_ID, DATE_DEPOT, CODE_NGP (normalisé), QUANTITE, DEVISE | `silver/badr_ratio_unitaire/` |
| CODE_NGP_INITIAL, PAYS, VALEUR (brute) | `silver/badr/` (jointure) |
| VALEUR_MAD, PRIX_REFERENCE, VALEUR_UNITAIRE_MAD, RATIO_UNITAIRE | `silver/badr_ratio_unitaire/` |
| ARBITRAGE | calculé par `arbitrage_gold.py` (règle absolue ci-dessus) |

Le schéma Gold est **inchangé** : aucune colonne de seuil n'y est ni n'y était stockée.

## Validations exécutées par le job

- Lignes en entrée = lignes en sortie (0 perdue)
- 0 NULL sur toutes les colonnes importantes
- Aucun CODE_NGP hors `{85171300, 84713000, 85287200}`
- **Exactitude de la règle** : `max(RATIO_UNITAIRE)` des MINORÉ `< BORNE_BASSE`, `min` des MAJORÉ `> BORNE_HAUTE`, NORMAL entièrement dans `[BORNE_BASSE, BORNE_HAUTE]`
- Cohérence des médianes : `MINORE < NORMAL < MAJORE`
- Vérification manuelle de 5 lignes (ratio comparé aux bornes)
- Gold relu après écriture, puis sources amont recomptées (aucune modifiée)

## Règle métier vs IA

| | IA 1 (classification NGP) | Arbitrage |
|---|---|---|
| Méthode | Modèle scikit-learn entraîné | Règle `if/else` sur 2 constantes |
| Reproductibilité | Dépend des poids appris | 100 % déterministe |
| Explicabilité | Coefficients/probabilités | Comparaison directe à un seuil chiffré, lisible |
| Nouveau `.joblib` | Oui | **Non — aucun** |

## Valeurs ASCII de la colonne ARBITRAGE

`NORMAL`, `MINORE`, `MAJORE` — sans accent (formes correctes : « MINORÉ »/« MAJORÉ ») pour éviter tout problème d'encodage dans Parquet/Trino/dbt. Choix documenté.

## Limites

- BADR simulé (Faker), `PRIX_REFERENCE` issu du scraping — valeur démonstrative, pas probante.
- Couverture ≈ 3 codes NGP sur 34 ; le reste hors Gold.
- Le résultat d'un run dépend de `PRIX_REFERENCE`, qui dépend des prix scrapés de la période arbitrée — deux runs sur des périodes différentes ne sont pas directement comparables.

## ⚠️ Limite majeure — la référence n'est pas calibrée sur la valeur en douane

**Distribution obtenue avec la règle absolue ±10 %** (ordre de grandeur — bouge à chaque run avec `PRIX_REFERENCE` et la population BADR ; deux runs 2026-08-29 : `156/38/149` sur 343, puis `154/38/153` sur 345) :

| | ~n | ~% |
|---|---:|---:|
| **MINORÉ** | ~155 | ~45 % |
| **NORMAL** | ~38 | **~11 %** |
| **MAJORÉ** | ~150 | ~44 % |
| Global | ~345 | médiane ratio ≈ **0,98** · moyenne ≈ **1,3** |

Seules **~11 %** des déclarations tombent dans la bande NORMAL, et **~44 %** sont MAJORÉ (déclarées au-dessus du prix de référence) — économiquement contre-intuitif. Analyse :

**1. Globalement la référence est à peu près centrée.** Médiane du ratio ≈ 0,98 (quasi 1) ; ~51 % des déclarations sous la référence, ~49 % au-dessus — presque symétrique. La moyenne (≈ 1,3) est tirée par une queue droite longue (max ~7,4), les moyennes ne sont pas robustes ici. Donc **l'hypothèse « référence retail systématiquement trop haute » n'est PAS visible dans l'agrégat** et n'est pas directionnelle.

**2. Le vrai problème est par catégorie — les échelles ne correspondent pas :**

| CODE_NGP | `PRIX_REFERENCE`* | val. unitaire déclarée médiane | médiane ratio | verdict dominant |
|---|---:|---:|---:|---|
| 85171300 (Smartphone) | ~1 319 MAD | ~1 690 MAD | **≈ 1,29** (+29 %) | MAJORÉ ~59 % |
| 84713000 (PC Portable) | ~2 800 MAD | ~4 250 MAD | **≈ 1,52** (+52 %) | MAJORÉ ~65 % |
| 85287200 (Téléviseur) | ~3 000 MAD | ~1 520 MAD | **≈ 0,50** (−50 %) | MINORÉ ~81 % |

\* `PRIX_REFERENCE` = médiane des prix Jumia scrapés, recalculée à chaque run — d'où le fait que les décomptes exacts changent.

Les médianes par catégorie sont **loin de 1 et dans des directions opposées** (~0,5 pour les TV, ~1,5 pour les PC portables). BADR (Faker) et les prix Jumia sont générés indépendamment : rien ne garantit que la valeur unitaire tirée pour un téléviseur soit proche du prix Jumia d'un téléviseur. Le gros du « MAJORÉ » global est porté par smartphones + PC portables ; le gros du « MINORÉ » par les téléviseurs. Les deux biais opposés se compensent à l'agrégat, d'où la médiane globale trompeusement proche de 1.

**3. Retail vs CIF (concern réel pour la production, secondaire ici).** Un prix Jumia = prix CIF + marge distributeur + TVA + transport local. Une valeur en douane est un prix CIF à l'import, structurellement plus bas. Sur données réelles, `PRIX_REFERENCE` devrait subir un ajustement retail→CIF (coefficient, ou table de valeurs douanières de référence) avant qu'une bande ±10 % ait un sens. Mais sur CE jeu de données, cet effet est dominé par le décalage d'échelle Faker/scraping du point 2 — et il n'explique pas pourquoi les PC portables sont à 1,48.

**Pourquoi P10/P90 « marchait » : il ne mesurait rien d'absolu.** En prenant les percentiles *à l'intérieur de chaque catégorie*, il coupait toujours ~10 % de chaque côté, que la catégorie soit centrée sur 0,48 ou sur 1,48. Il produisait une distribution stable (10/80/10) en masquant complètement la non-calibration de la référence. La règle absolue la **révèle** — ce qui est correct sur le plan méthodologique, mais rend les proportions NORMAL/MINORÉ/MAJORÉ **non interprétables comme un taux de fraude** sur ces données synthétiques.

**Conséquence pratique :** la règle est juste et ne change pas. Sur données simulées, les décomptes démontrent que le mécanisme fonctionne, ce ne sont pas des taux réels. Sur données réelles, il faudrait d'abord calibrer `PRIX_REFERENCE` sur la valeur en douane (par catégorie).
