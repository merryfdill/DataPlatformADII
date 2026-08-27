# Dataset ML — Prédiction du CODE_NGP (Phase 2.10)

## Source des données

- `data/prix_web.csv` : 73 produits scrapés sur Jumia Maroc (Phase 2.8), 3 catégories — Smartphone (24), PC Portable (24), Televiseur (25). `CODE_NGP` y est NULL pour toutes les lignes.
- Script de préparation : [`ingestion/ml/prepare_dataset.py`](../ingestion/ml/prepare_dataset.py). Lecture seule sur `data/prix_web.csv` — ce fichier n'est jamais modifié.
- Mapping catégorie → NGP : [`ingestion/config.py`](../ingestion/config.py) (`SCRAPING_CATEGORY_TO_NGP8`), documenté comme connaissance métier issue du Tarif douanier officiel ADII (Phase 2.9), pas de BADR, pas de Faker.

## Label — CODE_NGP (8 chiffres)

| Catégorie | CODE_NGP | Source officielle |
|---|---|---|
| Smartphone | `85171300` | ADII Tarif, position 85.17, sous-position 8517.13 "Téléphones intelligents" |
| PC Portable | `84713000` | ADII Tarif, position 84.71, sous-position 8471.30 |
| Televiseur | `85287200` | ADII Tarif, position 85.28, sous-position 8528.72 "Autres, en couleurs" |

Les subdivisions nationales à 10 chiffres applicables au périmètre retail (`8517130090`, `8471300090`, `8528720099` — toutes "assemblé/fini") ne sont **pas** utilisées comme label ici : elles ne dépendent pas de la marque ni des caractéristiques produit dans ce périmètre, et seront traitées comme une règle métier fixe, pas comme une sortie du modèle.

## Datasets produits

| Fichier | Colonnes | Contient `categorie` ? |
|---|---|---|
| `data/ml_dataset_baseline.csv` | marque, modele, description, prix, devise, type_prix, categorie, ram, stockage, reseau, taille_ecran, processeur, batterie, camera, texte_produit, CODE_NGP | Oui |
| `data/ml_dataset_features.csv` | idem sans `categorie` | Non |

`texte_produit` = concaténation `marque + modele + description` (aucune valeur inventée — ces 3 colonnes n'ont aucun NULL dans la source).

Colonnes explicitement exclues des features : `url`, `date_scraping`, `site_source` (métadonnées de scraping) et `CODE_NGP` (label).

## Dataset A vs Dataset B — risque de data leakage

`categorie` détermine `CODE_NGP` de façon déterministe (1:1, cf. tableau ci-dessus) : un modèle entraîné sur le Dataset A peut donc apprendre trivialement `categorie → NGP` sans aucun signal produit réel.

- **Dataset A (baseline)** : sert uniquement à valider le pipeline de classification de bout en bout. Ses performances ne mesurent rien d'intéressant sur le plan ML.
- **Dataset B (features)** : sans `categorie`, le modèle doit trouver un signal dans le texte et les caractéristiques techniques. C'est le seul des deux qui teste une vraie capacité de généralisation.

## Qualité des données

- 73 lignes, 0 doublon, 0 NULL sur marque/modele/description/prix/devise/type_prix/texte_produit/CODE_NGP.
- Colonnes techniques très creuses (NULL réels, non comblés) : `ram` 45/73, `stockage` 25/73, `reseau` 69/73, `taille_ecran` 42/73, `processeur` 56/73, `batterie` 72/73, `camera` 73/73 (100% NULL).
- `devise` (MAD) et `type_prix` (RETAIL_TTC) sont constants sur les 73 lignes — conservés dans la structure car demandés, mais sans variance donc sans signal.
- **Type de `CODE_NGP` après écriture/lecture CSV** : pandas relit la colonne comme `int64` (les 3 codes ne comportent pas de zéro non significatif, donc aucune valeur n'est corrompue), mais un NGP est un identifiant, pas une quantité. La Phase 2.11 doit charger cette colonne en forçant `dtype={'CODE_NGP': str}` pour éviter de la traiter comme une variable ordinale/numérique.

## Distribution des classes

| CODE_NGP | Catégorie | n | % |
|---|---|---|---|
| 85171300 | Smartphone | 24 | 32,9% |
| 84713000 | PC Portable | 24 | 32,9% |
| 85287200 | Televiseur | 25 | 34,2% |

Quasi équilibré (écart max 1 produit) — suffisant pour un MVP, aucun rééchantillonnage (SMOTE etc.) nécessaire ni appliqué.

## Stratégie train/test proposée (Phase 2.11, non exécutée ici)

`sklearn.model_selection.train_test_split(..., test_size=0.2, stratify=CODE_NGP, random_state=42)`

| Classe | n | train (~80%) | test (~20%) |
|---|---|---|---|
| 85171300 | 24 | 19 | 5 |
| 84713000 | 24 | 19 | 5 |
| 85287200 | 25 | 20 | 5 |

## Limites

- Ce dataset ne permet de classer qu'au niveau **catégorie / HS8** (3 classes). Il ne permet **pas** de classification fine à 10 chiffres (CKD/SKD, tablette électronique, usage industriel/satellite — structurellement absents du scraping retail, cf. Phase 2.9).
- Le Tarif officiel ne distingue **jamais** les NGP par marque : aucune relation Samsung/Oppo/Apple → NGP différent n'existe ni ne doit être apprise.
- Le Dataset A est un baseline de pipeline, pas un test de signal ML réel — voir Dataset B pour ça.

## Prochaine étape

Phase 2.11 — entraînement et évaluation d'un premier modèle ML sur `data/ml_dataset_features.csv` (et, à titre de comparaison de pipeline, `data/ml_dataset_baseline.csv`), avec le split stratifié décrit ci-dessus. Pas d'entraînement effectué dans cette phase.
