# Modèle ML — Prédiction du CODE_NGP (Phase 2.11)

Complète [`docs/ml_dataset.md`](ml_dataset.md) (Phase 2.10). Ce document couvre l'entraînement, l'évaluation et les limites du premier modèle.

## Dataset utilisé

`data/ml_dataset_features.csv` (Dataset B, Phase 2.10) — **`categorie` n'est PAS une feature** : elle détermine `CODE_NGP` de façon déterministe (1:1) et créerait une fuite de données triviale. Chargé avec `dtype={"CODE_NGP": str}` pour garder le label catégoriel (voir la limitation de typage documentée dans `ml_dataset.md`).

73 produits, 3 classes : `85171300` (Smartphone, 24) · `84713000` (PC Portable, 24) · `85287200` (Televiseur, 25).

## Features

Construites dans [`ingestion/ml/features.py`](../ingestion/ml/features.py), partagé entre l'entraînement et la prédiction pour garantir un traitement identique.

| Type | Colonnes | Preprocessing |
|---|---|---|
| Texte | `texte_produit` (marque+modele+description, déjà construit en Phase 2.10) | `TfidfVectorizer(max_features=200, ngram_range=(1,2))` |
| Numérique | `prix`, `ram_num`, `stockage_num`, `taille_ecran_num` | `SimpleImputer(median)` + `StandardScaler`, appris uniquement sur train |
| Catégoriel | `marque`, `devise`, `type_prix`, `reseau`, `processeur` | `SimpleImputer(constant="missing")` + `OneHotEncoder(handle_unknown="ignore")`, appris uniquement sur train |
| **Exclues** | `categorie`, `camera`, `batterie`, `CODE_NGP`, `url`, `date_scraping`, `site_source` | — |

`camera` (100% NULL) et `batterie` (72/73 NULL, une seule valeur réelle) sont exclues : les imputer produirait une colonne constante, sans aucun signal par ligne — ce n'est pas de la fabrication de données, juste l'absence de tout signal exploitable. `ram`/`stockage`/`taille_ecran` sont stockées en texte composite (`"8 Go"`, `"6.7\""`) ; `extract_leading_number` en extrait la valeur numérique déjà présente (du parsing, pas de l'invention) — les valeurs toujours manquantes après parsing restent NULL et sont gérées par l'imputer, jamais comblées à la main.

Tout le preprocessing est encapsulé dans un `ColumnTransformer` à l'intérieur du `Pipeline` scikit-learn : il est **appris uniquement sur le train**, jamais sur le test.

## Split train/test

Stratifié 80/20, `random_state=42`, `stratify=CODE_NGP`.

| | Total | 85171300 | 84713000 | 85287200 |
|---|---|---|---|---|
| Train | 58 | 19 | 19 | 20 |
| Test | 15 | 5 | 5 | 5 |

⚠️ **73 produits est un échantillon très petit.** Avec ~5 exemples de test par classe, chaque métrique se déplace de ±6,7 points de pourcentage par exemple mal classé — les résultats ci-dessous sont un signal MVP, pas une estimation de performance fiable.

## Modèles testés et résultats (test set)

| Model | Accuracy | Precision (macro) | Recall (macro) | F1 (macro) |
|---|---|---|---|---|
| Logistic Regression | 1.000 | 1.000 | 1.000 | 1.000 |
| Linear SVM | 1.000 | 1.000 | 1.000 | 1.000 |
| Random Forest | 1.000 | 1.000 | 1.000 | 1.000 |

Les 3 modèles classent parfaitement les 15 exemples de test (matrices de confusion diagonales, 5/5/5). **Meilleur modèle retenu (F1 macro) : Logistic Regression** (choisi arbitrairement parmi 3 ex-æquo parfaits — voir analyse critique).

## Analyse critique — pourquoi 100% n'est pas une victoire

Un score parfait sur un jeu de test de 15 exemples et 3 classes n'est **pas** la preuve que le modèle a appris une règle de classification douanière généralisable. Inspection des coefficients de Logistic Regression (les plus fortes contributions positives par classe) :

- **85287200 (Televiseur)** : `taille_ecran_num` (+0.89), `tv` (+0.78), `smart tv` (+0.46), `marque_MIIO` (+0.37), `led` (+0.32)
- **84713000 (PC Portable)** : `marque_Hp` (+0.68), `marque_Apple` (+0.54), `stockage_num` (+0.48), `ssd` (+0.39), `marque_Lenovo` (+0.27)
- **85171300 (Smartphone)** : `marque_Samsung` (+0.92), `marque_Itel` (+0.51), `ans de garantie` (+0.40, boilerplate marketing), `go` (+0.37)

Le modèle s'appuie massivement sur des **dummies de marque** (`marque_Hp`, `marque_Samsung`, `marque_MIIO`...) et du **vocabulaire quasi-synonyme de la catégorie** ("tv", "smart tv", "ssd") — pas sur les critères légaux du Tarif (capacité OS multitâche pour 8517.13, poids ≤10kg+clavier intégré pour 8471.30, écran couleur intégré pour 8528.72).

**Effet indirect de la catégorie confirmé**, bien qu'elle ne soit jamais utilisée directement : dans cet échantillon de 73 produits, la plupart des marques n'apparaissent que dans une seule catégorie (Hp/Lenovo/Acer → PC uniquement ; MIIO/Sharp/AZATECH → TV uniquement), donc la marque agit comme un **proxy quasi parfait de la catégorie**. Apple, Samsung et XIAOMI font exception (présents dans 2 catégories chacun) — le modèle doit alors combiner marque + vocabulaire de gamme produit pour trancher, ce qui reste un raisonnement "à quoi ressemble ce texte" plutôt qu'un raisonnement tarifaire.

**Test de robustesse** (produit synthétique, hors dataset) : un "Sharp [smartphone-like specs]" — Sharp n'étant vu qu'en tant que marque Televiseur à l'entraînement — a été correctement classé Smartphone, mais avec des probabilités quasi uniformes (`85171300: 0.39, 84713000: 0.34, 85287200: 0.28`), contre 0,88–0,95 sur les exemples "normaux" (marque cohérente avec sa catégorie habituelle). La confiance s'effondre dès que l'association marque↔catégorie habituelle est rompue — signe clair de pattern-matching sur la co-occurrence marque/catégorie de cet échantillon, pas d'une règle généralisable.

**Autres facteurs à garder en tête :**
- Risque de surapprentissage difficile à détecter classiquement ici (train et test sont tous deux quasi parfaitement séparables dans cet espace de features) plutôt qu'un vrai signe de généralisation.
- 3 classes, vocabulaire de catégorie très disjoint (téléphone/laptop/TV ne partagent presque aucun mot-clé métier) → tâche BEAUCOUP plus facile qu'une vraie classification douanière fine.
- Équilibre des classes : quasi parfait (24/24/25), ce n'est pas un facteur de biais ici.

**Conclusion :** ce modèle ne doit **pas** être présenté comme capable de prédire un NGP réel pour un produit quelconque. Il démontre que le pipeline de bout en bout fonctionne, et que le texte produit contient *un* signal de séparation catégorie — pas plus.

## Fichiers sauvegardés

- [`models/ngp_classifier.joblib`](../models/ngp_classifier.joblib) — pipeline complet (preprocessing + Logistic Regression), rechargeable directement avec `joblib.load`.
- [`models/ngp_classifier_metadata.json`](../models/ngp_classifier_metadata.json) — modèle retenu, classes, `random_state`, features, date d'entraînement, métriques test (du meilleur modèle et des 3 modèles), dataset utilisé, limites.

## Script de prédiction

[`ingestion/ml/predict_ngp.py`](../ingestion/ml/predict_ngp.py) charge le pipeline sauvegardé et prédit un `CODE_NGP` (+ probabilités par classe quand le modèle les fournit) pour un nouveau produit décrit avec les mêmes champs que `data/prix_web.csv`. `categorie` n'est jamais utilisée, même si fournie. Testé avec succès sur 3 vrais exemples du dataset (un par catégorie) + 2 produits synthétiques hors dataset pour sonder la robustesse (voir ci-dessus).

## Limites (rappel)

- **3 classes HS8 seulement.** Aucune classification à 10 chiffres (CKD/SKD, tablette électronique, usage industriel/satellite) : ces subdivisions sont structurellement absentes du scraping retail Jumia (Phase 2.9) et n'ont **pas** été apprises ici.
- **Aucune relation marque → NGP n'existe dans le Tarif douanier officiel.** Le fait que le modèle s'appuie sur la marque est une limite du dataset d'entraînement (échantillon trop petit, marques peu diversifiées par catégorie), pas une règle à retenir ou à réutiliser.
- 73 produits, ~5 exemples de test par classe : les métriques ne sont pas une estimation fiable de performance en production.
- Pas de nouvelle donnée artificielle, pas de correspondance forcée, pas de modification de BADR pour améliorer les résultats.

## Prochaine étape (hors périmètre de cette phase)

Élargir la diversité de marques par catégorie (plusieurs marques vendant dans plusieurs catégories) avant de considérer ce signal comme robuste ; envisager une validation croisée plutôt qu'un split unique vu la petite taille du dataset.
