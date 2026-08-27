# Analyse de VALEUR BADR et conversion en MAD (Phase 2.15 + 2.16)

Complète [`docs/prix_reference.md`](prix_reference.md) (Phase 2.14). La Phase 2.15 a analysé `VALEUR`/`VALEUR_INITIALE` et préparé la structure de conversion (taux `NULL`). La Phase 2.16 a intégré les taux de change officiels et calculé `VALEUR_MAD` pour les 5000 lignes. **Aucune des deux phases ne compare encore `VALEUR_MAD` à `PRIX_REFERENCE`** — ni ratio, ni NORMAL/MINORÉ/MAJORÉ, ni Gold.

> ⚠️ **Mise à jour Phase 2.18/2.19** : le diagnostic Phase 2.18 a montré que le ratio `VALEUR_MAD / PRIX_REFERENCE` n'était pas interprétable (valeur globale de déclaration vs prix unitaire retail). La Phase 2.19 a ajouté `QUANTITE` et **régénéré `data/badr.db`** (voir [`docs/badr_quantity.md`](badr_quantity.md)) — les valeurs `VALEUR`/`VALEUR_INITIALE`/`POIDS`/`POIDS_INITIAL` citées ci-dessous correspondent au générateur **avant** cette régénération et sont conservées ici à titre historique. `s3://datalake/silver/badr_valeur/`, `s3://datalake/silver/badr_ratio/` et `s3://datalake/silver/badr_ratio_stats/` sont **obsolètes** depuis la régénération (calculés sur l'ancien BADR) et devront être recalculés dans une phase future à partir du nouveau Silver BADR (avec `QUANTITE`).

## Structure de VALEUR

D'après le générateur ([`ingestion/badr/generate_badr.py`](../ingestion/badr/generate_badr.py), lignes 60-69) — la source de vérité la plus fiable puisque BADR est simulé :

```python
# --- Value: importer-declared base, then the inspector's assessment ---
valeur_initiale = round(rng.lognormvariate(8.6, 0.9), 2)
r = rng.random()
if r < 0.60: valeur = valeur_initiale            # accepté tel que déclaré
elif r < 0.85: valeur = valeur_initiale * uniform(1.05, 1.40)  # réévalué à la hausse
else: valeur = valeur_initiale * uniform(0.80, 0.95)           # réévalué à la baisse
```

`VALEUR_INITIALE` = valeur déclarée par l'importateur. `VALEUR` = valeur après évaluation par l'inspecteur douanier (acceptée telle quelle, ou réévaluée à la hausse/baisse). **Ce n'est pas une distinction unitaire/total** — c'est une distinction temporelle/procédurale (déclaration initiale vs évaluation finale), exactement le même schéma que `CODE_NGP_INITIAL` → `CODE_NGP` (déclaration initiale vs reclassification) et `POIDS_INITIAL` → `POIDS` (poids déclaré vs pesée réelle).

## Devises présentes (vérifiées dans les données réelles, 5000 lignes)

| DEVISE | Lignes | % |
|---|---|---|
| EUR | 2373 | 47,5% |
| USD | 2324 | 46,5% |
| GBP | 303 | 6,1% |

0 NULL, 0 devise inattendue — conforme à `config.BADR_COUNTRY_CURRENCY` / `BADR_ALTERNATE_CURRENCIES` (jamais MAD dans BADR).

## VALEUR vs VALEUR_INITIALE (statistiques réelles)

| | VALEUR | VALEUR_INITIALE |
|---|---|---|
| count | 5000 | 5000 |
| NULL | 0 | 0 |
| min | 116,60 | 116,60 |
| médiane | 5577,34 | 5380,56 |
| moyenne | 8517,19 | 8204,17 |
| max | 124017,49 | 124017,49 |

Par devise (médiane VALEUR) : EUR 5462,74 · USD 5653,69 · GBP 5730,31 — ordres de grandeur comparables entre devises (cohérent avec la génération : le tirage lognormal ne dépend pas de la devise).

**Relation VALEUR/VALEUR_INITIALE** : identiques dans 59,9% des cas (2995/5000) ; différentes dans 40,1% des cas (2005/5000), dont 64% réévaluées à la hausse et 36% à la baisse — conforme exactement aux probabilités du générateur (60% / 25% / 15%).

## VALEUR : totale ou unitaire ?

**Constat historique (Phase 2.15/2.18), résolu en Phase 2.19 :** à l'époque de cette analyse, BADR ne possédait aucune colonne `QUANTITE` — recherché explicitement dans le schéma Silver BADR (`id, DATE_DEPOT, VALEUR_INITIALE, VALEUR, POIDS, POIDS_INITIAL, CODE_NGP, CODE_NGP_INITIAL, PAYS, DEVISE`) et dans le script générateur : absente des deux. Aucune quantité n'avait été fabriquée pour combler ce vide — la conclusion était "non déterminable avec certitude". **`QUANTITE` a depuis été ajoutée et `data/badr.db` régénéré (Phase 2.19, voir [`docs/badr_quantity.md`](badr_quantity.md))** ; le raisonnement ci-dessous, qui a motivé cet ajout, reste documenté tel quel.

Deux indices, présents dans les données et le code, penchent vers une **valeur agrégée de déclaration/expédition plutôt qu'un prix unitaire** — mais restent circonstanciels, pas une preuve :

1. **Le générateur ne fait varier `VALEUR` par aucune caractéristique produit.** La médiane de `VALEUR` par `CODE_NGP` va de 4624 à 6757 sur les 34 codes (écart-type 551) — un smartphone, un véhicule et un meuble ont statistiquement la même échelle de valeur. Un prix *unitaire* réel varierait sur plusieurs ordres de grandeur entre ces catégories ; une valeur *totale* de déclaration (dont l'ampleur dépend surtout de la taille de l'envoi, pas du type de produit) est bien plus cohérente avec cette absence de variation par catégorie.
2. **Magnitude de `POIDS`** : exemples réels observés — 2598 kg (id=2, textile), 3719 kg (id=3, électronique), 988 kg (id=4, `85287200`/téléviseurs). Ce sont des poids d'expédition, pas d'un article unique.

Ces deux indices sont cohérents entre eux mais restent des inférences sur des données simulées, pas une confirmation documentée du système BADR réel — d'où la conclusion prudente ci-dessus.

## POIDS vs POIDS_INITIAL

Cohérent avec le code générateur (« near-identical » 80% du temps, écart plus large 20% du temps) : 82,4% des lignes ont un ratio `POIDS/POIDS_INITIAL` dans ±2%, 17,6% en dehors. Aucune incohérence structurelle détectée (aucun poids négatif, aucun écart aberrant au-delà de ce que le générateur prévoit lui-même) ; aucune relation quantité/prix n'a été déduite de cette analyse.

## Stratégie de conversion des devises

**Phase 2.15** : aucune source de taux de change n'existait dans le projet — recherché explicitement dans `ingestion/config.py`, `.env.example`, `ingestion/badr/generate_badr.py` : le seul champ lié à la devise est `BADR_ALTERNATE_CURRENCY_RATE = 0.08`, qui est une **probabilité** de tirage d'une devise alternative (8%), pas un taux de change. La structure de la table a été préparée, taux laissés `NULL`, aucun taux inventé.

**Phase 2.16** : taux officiels intégrés. Source retenue : **Bank Al-Maghrib**, "Cours de référence" — le taux de change officiel quotidien de la banque centrale marocaine ([bkam.ma/.../Cours-de-reference](https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-des-changes/Cours-de-change/Cours-de-reference)). Consulté le **2026-08-14**, taux datés par BAM du **2026-08-13** (dernière publication disponible ; BAM publie un nouveau cours de référence chaque jour ouvré à 16h15). Vérifiés par deux extractions indépendantes (résumé WebFetch + parsing du tableau HTML brut), parfaitement concordantes.

`s3://datalake/silver/reference/taux_change.parquet` ([`ingestion/build_taux_change.py`](../ingestion/build_taux_change.py)) :

| DEVISE | TAUX_MAD | SOURCE | DATE_TAUX |
|---|---|---|---|
| EUR | 10,7272 | Bank Al-Maghrib, Cours de référence officiel | 2026-08-13 |
| USD | 9,3019 | Bank Al-Maghrib, Cours de référence officiel | 2026-08-13 |
| GBP | 12,550 | Bank Al-Maghrib, Cours de référence officiel | 2026-08-13 |

`VALEUR_MAD = VALEUR × TAUX_MAD` — calculé pour les 5000 lignes (voir section Validations).

## Pourquoi les données BADR originales ne sont pas modifiées

`data/badr.db` reste la source de vérité pour les déclarations simulées ; le convertir en MAD dans les données sources supprimerait l'information de devise d'origine et casserait la traçabilité. La conversion est donc préparée dans une **vue de travail séparée**, jamais dans Silver BADR ni `data/badr.db`.

## Pourquoi aucune décision NORMAL/MINORÉ/MAJORÉ n'est prise

`VALEUR_MAD` est maintenant disponible pour les 5000 lignes, mais cette phase (2.16) s'arrête volontairement à la conversion : comparer `VALEUR_MAD` à `PRIX_REFERENCE`, calculer un ratio, ou classer NORMAL/MINORÉ/MAJORÉ est explicitement hors périmètre et traité dans une phase ultérieure.

## Dataset de travail

`s3://datalake/silver/badr_valeur/` (Spark, [`spark/jobs/badr_valeur_prep.py`](../spark/jobs/badr_valeur_prep.py)) — jointure `Silver BADR ⋈ taux_change` sur `DEVISE` :

| Colonne | Contenu |
|---|---|
| id, DATE_DEPOT, CODE_NGP, CODE_NGP_INITIAL, POIDS | copiés de Silver BADR |
| VALEUR, DEVISE | copiés de Silver BADR |
| TAUX_MAD | taux officiel BAM (Phase 2.16) |
| VALEUR_MAD | `VALEUR × TAUX_MAD`, calculé pour les 5000 lignes |

5000 lignes en entrée, 5000 en sortie (aucune perte). Silver BADR relu et recompté après écriture pour confirmer qu'il est resté à 5000 lignes, intact.

## Validations (Phase 2.16, réelles)

- Lignes Silver BADR lues : **5000**
- Lignes dans `badr_valeur` (sortie) : **5000** (aucune perte)
- `TAUX_MAD` NULL : **0** / 5000
- `VALEUR_MAD` NULL : **0** / 5000
- Silver BADR recompté après le job : toujours **5000** lignes, intact

**Statistiques VALEUR_MAD (MAD) :**

| | Global | EUR | USD | GBP |
|---|---|---|---|---|
| count | 5000 | 2373 | 2324 | 303 |
| min | 1 084,60 | 2 866,42 | 1 084,60 | 8 249,49 |
| médiane | 56 480,10 | 58 599,90 | 52 590,01 | 71 915,39 |
| moyenne | 86 646,13 | 89 905,95 | 80 061,11 | 111 623,15 |
| max | 1 153 597,64 | 1 145 821,74 | 1 153 597,64 | 658 606,68 |

Exemple de calcul vérifié manuellement : ligne id=1, `VALEUR`=6773,64 EUR × `TAUX_MAD`=10,7272 = 72 662,19 MAD ✓ (correspond exactement à la sortie Spark).

## Problèmes / informations manquantes

- Absence de `QUANTITE` dans BADR : bloque toujours toute conclusion définitive totale/unitaire (voir section dédiée) — non résolu par la conversion de devise, question orthogonale.
- Taux de change désormais intégrés (Phase 2.16) — plus un problème ouvert. Point de vigilance pour une phase future : ces taux sont figés à la date du 2026-08-13 (pas de mise à jour automatique quotidienne dans cette implémentation).
