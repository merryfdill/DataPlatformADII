# Arbitrage et couche Gold (Phase 2.21)

Complète [`docs/ratio_unitaire.md`](ratio_unitaire.md) (Phase 2.20). Première couche Gold du projet — résultat métier final, destiné à alimenter ensuite dbt, un dashboard, et un futur chatbot LLM.

## Architecture confirmée du projet

- **IA 1** (`models/ngp_classifier.joblib`, Phase 2.11) : classification du CODE_NGP des produits scrapés. Rôle achevé et figé — non modifiée dans cette phase.
- **RATIO_UNITAIRE** : indicateur de comparaison entre la valeur déclarée BADR (ramenée à l'unité) et le prix de référence retail.
- **Arbitrage NORMAL/MINORE/MAJORE** : **règle métier explicable**, pas une deuxième IA. Aucun Isolation Forest, aucun classifieur d'anomalie, aucun fichier `.joblib` supplémentaire créé.
- **Futur chatbot LLM** (hors périmètre de cette phase) : interrogera et expliquera les résultats Gold, sans jamais remplacer ce calcul métier ni inventer de résultat.

```
Silver badr_ratio_unitaire  ─┐
                              ├─► Spark (arbitrage_gold.py) ─► Gold (s3://datalake/gold/arbitrage/)
Silver badr (PAYS, CODE_NGP_INITIAL, VALEUR) ─┘
```

## Pourquoi le ratio est utilisé

`RATIO_UNITAIRE` compare la valeur déclarée par unité à ce que le marché retail pratique réellement pour ce type de produit (`PRIX_REFERENCE`, Phase 2.14) — un signal de cohérence économique, calculé sans aucune IA.

## Formule

```
VALEUR_UNITAIRE_MAD = VALEUR_MAD / QUANTITE          (Phase 2.20)
RATIO_UNITAIRE       = VALEUR_UNITAIRE_MAD / PRIX_REFERENCE
```

## Pourquoi QUANTITE est nécessaire

Sans elle (Phase 2.17/2.18), `VALEUR_MAD` (valeur globale de déclaration) et `PRIX_REFERENCE` (prix unitaire) n'étaient pas comparables — voir le diagnostic Phase 2.18 et sa résolution Phase 2.19/2.20.

## Analyse de la distribution réelle (avant tout choix de seuil)

Percentiles de `RATIO_UNITAIRE`, lus directement dans `s3://datalake/silver/badr_ratio_unitaire/` (338 lignes) :

| | Global (n=338) | Smartphone (n=118) | PC Portable (n=109) | Televiseur (n=111) |
|---|---|---|---|---|
| P5 | 0,273 | 0,267 | 0,582 | 0,218 |
| P10 | 0,400 | 0,467 | 0,629 | 0,276 |
| P25 | 0,636 | 0,705 | 0,894 | 0,448 |
| P50 (médiane) | 1,045 | 1,079 | 1,464 | 0,735 |
| P75 | 1,609 | 1,736 | 2,055 | 1,090 |
| P90 | 2,440 | 2,772 | 2,819 | 1,552 |
| P95 | 3,233 | 3,450 | 4,169 | 1,738 |
| P99 | 6,021 | 6,059 | 5,037 | 3,446 |
| Moyenne | 1,330 | 1,429 | 1,656 | 0,905 |

**Constat déterminant : les médianes diffèrent nettement par catégorie** (Televiseur 0,74 vs Smartphone 1,08 vs PC Portable 1,46 — écart ×2). **Un seuil global serait trompeur** : appliqué uniformément, il sur-signalerait systématiquement les Televiseur en MINORE et sous-signalerait les PC Portable en MAJORE, pour une raison qui ne reflète qu'une différence structurelle entre catégories, pas une anomalie réelle. **Conclusion : seuils par CODE_NGP, pas de seuil global.**

## Définition des seuils

```
RATIO_UNITAIRE < P10(CODE_NGP)              → MINORE
P10(CODE_NGP) ≤ RATIO_UNITAIRE ≤ P90(CODE_NGP) → NORMAL
RATIO_UNITAIRE > P90(CODE_NGP)              → MAJORE
```

**Pourquoi P10/P90 (déciles) plutôt que P25/P75 (quartiles) :** un système de ciblage douanier vise à signaler une **minorité** de déclarations pour contrôle, pas la moitié du trafic (ce que des quartiles feraient par construction — 50% hors de la zone NORMAL). Les déciles ciblent ~10% bas + ~10% haut, une proportion opérationnellement réaliste, tout en restant une règle de percentile simple, transparente et non arbitraire (pas de constante inventée comme "0,8/1,2").

**Calcul dynamique, pas de constante codée en dur** : les seuils sont recalculés par Spark à chaque exécution à partir des données réelles (`spark/jobs/arbitrage_gold.py`), donc reproductibles et vérifiables — pas des nombres choisis a priori.

### Seuils obtenus (calculés, Phase 2.21)

| CODE_NGP | n | SEUIL_MINORE (P10) | SEUIL_MAJORE (P90) | Médiane |
|---|---|---|---|---|
| 85171300 (Smartphone) | 118 | 0,4670 | 2,7719 | 1,0794 |
| 84713000 (PC Portable) | 109 | 0,6291 | 2,8187 | 1,4639 |
| 85287200 (Televiseur) | 111 | 0,2756 | 1,5518 | 0,7354 |

### ⚠️ Ce ne sont PAS des seuils douaniers officiels

**Ces seuils sont des seuils de simulation/prototype**, dérivés statistiquement de `data/badr.db`, qui est un jeu de données **simulé avec Faker** (Phase 1, régénéré Phase 2.19). Aucune source officielle de l'ADII ou de la douane marocaine n'établit ces valeurs. Ils ne doivent **jamais** être présentés comme une règle douanière réelle — uniquement comme une méthode reproductible de démonstration sur données synthétiques.

## Valeurs ASCII de la colonne ARBITRAGE

`NORMAL`, `MINORE`, `MAJORE` — volontairement sans accent (formes adjectivales correctes seraient "MINORÉ"/"MAJORÉ") pour éviter tout problème d'encodage dans Parquet/Trino/dbt en aval. Choix documenté, pas un oubli.

## Différence règle métier vs IA

| | IA 1 (classification NGP) | Arbitrage (cette phase) |
|---|---|---|
| Méthode | Modèle scikit-learn entraîné (`ngp_classifier.joblib`) | Règle `if/else` sur percentiles, aucun apprentissage |
| Reproductibilité | Dépend des poids appris | 100% déterministe à partir des données |
| Explicabilité | Coefficients/probabilités du modèle | Comparaison directe à un seuil chiffré, lisible par un humain |
| Nouveau `.joblib` | Oui (Phase 2.11) | **Non — aucun** |

Aucun Isolation Forest, aucun second classifieur, aucun score d'anomalie ML n'a été créé dans cette phase, conformément à l'architecture validée.

## Couche Gold

`s3://datalake/gold/arbitrage/` (emplacement vérifié vide avant création — pas de structure Gold préexistante en conflit).

| Colonne | Source |
|---|---|
| BADR_ID, DATE_DEPOT, QUANTITE, DEVISE | `silver/badr_ratio_unitaire/` |
| CODE_NGP (normalisé) | `silver/badr_ratio_unitaire/` |
| CODE_NGP_INITIAL, PAYS, VALEUR (brute) | `silver/badr/` (jointure ajoutée dans cette phase — ces colonnes n'étaient pas exposées par `badr_valeur_prep.py`/`ratio_unitaire.py`, aucun de ces deux jobs n'a été modifié) |
| VALEUR_MAD, PRIX_REFERENCE, VALEUR_UNITAIRE_MAD, RATIO_UNITAIRE | `silver/badr_ratio_unitaire/` |
| ARBITRAGE | calculé dans cette phase |

## Validations (réelles, exécutées)

- 338 lignes en entrée → 338 en sortie (**0 perdue**)
- 0 NULL sur toutes les colonnes importantes
- Aucun CODE_NGP hors `{85171300, 84713000, 85287200}` (vérifié, aucune invention)
- `MINORE` médiane (0,2706) < `NORMAL` médiane (1,0450) < `MAJORE` médiane (3,2981) — cohérence confirmée
- 5 vérifications manuelles de l'assignation ARBITRAGE, toutes exactes
- Gold relu après écriture (338 lignes confirmées), puis relu une seconde fois indépendamment via pandas
- Sources amont recomptées après le job : `badr_ratio_unitaire` toujours 338, Silver BADR toujours 5000 — aucune n'a été modifiée

## Distribution obtenue

**Globale :** NORMAL 270 (79,9%) · MINORE 34 (10,1%) · MAJORE 34 (10,1%)

**Par CODE_NGP :** répartition quasi identique dans les 3 catégories (~10%/80%/10%), par construction du seuil décile par catégorie.

## Limites

- BADR est simulé (Faker) — les seuils n'ont aucune valeur probante réelle, uniquement démonstrative/méthodologique.
- 338/5000 déclarations BADR (6,76%) couvertes — seules celles avec un `CODE_NGP` normalisé dans le périmètre scrapé.
- Les seuils sont recalculés dynamiquement à chaque exécution à partir de l'échantillon courant (338 lignes) — pas encore figés dans une table de référence stable ; à surveiller si le volume de données évolue significativement.
- Le rôle futur du chatbot LLM (phase ultérieure) sera d'interroger et d'expliquer ces résultats Gold en langage naturel, jamais de recalculer ou de remplacer cette règle métier ni d'inventer un résultat non présent dans Gold.
