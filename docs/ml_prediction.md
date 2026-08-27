# Application du modèle et rapprochement BADR (Phase 2.12)

Complète [`docs/ml_dataset.md`](ml_dataset.md) (Phase 2.10) et [`docs/ml_model.md`](ml_model.md) (Phase 2.11). Ce document couvre l'application du modèle entraîné à Silver scraping et la préparation (pas encore l'arbitrage) du rapprochement avec BADR.

## Constat initial important : Silver était périmé

Avant d'exécuter quoi que ce soit, l'inspection de `s3://datalake/silver/scraping/` a montré un jeu de données **daté du 12/08, 58 lignes, 9 colonnes** (`marque, modele, prix, devise, type_prix, site_source, url, date_scraping, categorie`) — antérieur au rescraping 3-catégories de la Phase 2.8 (73 lignes, 18 colonnes, incluant `description/ram/stockage/reseau/taille_ecran/processeur`, exactement les colonnes utilisées par le modèle). Après confirmation utilisateur, `ingestion/bronze_ingestion.py` puis le job Spark existant `spark/jobs/bronze_to_silver.py` (ni l'un ni l'autre modifiés) ont été **réexécutés tels quels** pour rafraîchir Bronze et Silver à partir du `data/prix_web.csv` actuel. Résultat : Silver scraping = 73 lignes / 18 colonnes, cohérent avec le modèle. `data/badr.db` n'a pas été touché (seule sa copie technique Bronze/Silver a été régénérée à l'identique).

## Modèle utilisé

`models/ngp_classifier.joblib` — pipeline complet (preprocessing + Logistic Regression) sauvegardé en Phase 2.11, chargé tel quel via `joblib.load`. **Aucun réentraînement.** `categorie` n'est jamais une feature.

## Source d'entrée

`s3://datalake/silver/scraping/` (73 produits, 18 colonnes) — jamais Bronze directement.

## Features

Réutilisées sans duplication depuis [`ingestion/ml/features.py`](../ingestion/ml/features.py) (Phase 2.11) : `texte_produit` (TF-IDF), `prix`/`ram_num`/`stockage_num`/`taille_ecran_num` (numériques), `marque`/`devise`/`type_prix`/`reseau`/`processeur` (catégorielles). Deux petits ajouts additifs (aucune logique dupliquée) :
- `build_texte_produit_series` : version vectorisée de `build_texte_produit`, car Silver ne fournit pas `texte_produit` pré-construit (c'était une colonne du dataset ML de la Phase 2.10, pas du schéma Silver).
- `ensure_raw_columns` : ajoute en NaN toute colonne brute absente de la source, pour que le pipeline gère l'absence via ses imputers déjà appris — filet de sécurité conservé même après le rafraîchissement de Silver, comme demandé.

## Prédiction

[`ingestion/ml/apply_model.py`](../ingestion/ml/apply_model.py) — exécuté réellement sur les 73 produits.

Colonnes ajoutées (le `CODE_NGP` original reste NULL, jamais écrasé) :

| Colonne | Contenu |
|---|---|
| `CODE_NGP_PREDIT` | code HS8 prédit |
| `NGP_PROBA` | JSON `{code: proba}` pour les 3 classes |
| `NGP_CONFIANCE` | probabilité maximale (= probabilité de la classe prédite) |
| `NGP_CONFIDENCE_LEVEL` | HIGH (≥0,80) / MEDIUM (0,60–0,80) / LOW (<0,60) — **indicateur ML uniquement, pas une règle douanière** |

## Contrôles de cohérence (réels, sur les 73 lignes)

- Lignes avant = lignes après = **73** (aucune perte)
- `CODE_NGP_PREDIT` NULL : **0**
- Classes prédites : **3** (`84713000`, `85171300`, `85287200`)
- Distribution : `84713000`→25, `85287200`→25, `85171300`→23
- `NGP_CONFIANCE` : min **0,412**, moyenne **0,776**, max **0,991**
- `NGP_CONFIDENCE_LEVEL` : HIGH 42, MEDIUM 13, LOW 18
- `CODE_NGP` original non-NULL : **0** (vérifié, distinct de `CODE_NGP_PREDIT`)

## Contrôle `categorie` vs `CODE_NGP_PREDIT` (diagnostic seulement)

| categorie | CODE_NGP_PREDIT | nombre |
|---|---|---|
| PC Portable | 84713000 | 24 |
| **Smartphone** | **84713000** | **1** |
| Smartphone | 85171300 | 23 |
| Televiseur | 85287200 | 25 |

**Le mapping catégorie→NGP n'est PAS reproduit à 100% sur l'ensemble des 73 produits** (contrairement au test set de 15 produits en Phase 2.11, qui était parfait). Un iPhone 17 Pro Max est mal classé PC Portable (confiance 0,59 vs 0,40 pour Smartphone — un score serré, pas une erreur confiante). Cause identifiable : le titre du produit contient *"A19 Pro **Hexa-Core**"* (décrivant la puce), et **Apple vend à la fois des iPhones et des MacBooks dans l'échantillon d'entraînement** — le token "Core", fortement associé aux PC Portable ailleurs dans les données ("Core i5", "Core i3"), a fait pencher la balance. C'est exactement la fragilité anticipée dans l'analyse critique de la Phase 2.11 (dépendance à la marque + vocabulaire de gamme plutôt qu'aux critères légaux du Tarif), désormais observée sur données réelles plutôt qu'un cas synthétique.

**Confirmation explicite demandée** : même quand le modèle reproduit catégorie→NGP (ce qui reste vrai pour PC Portable et Televiseur ici), cela **ne prouve pas** une classification douanière indépendante de la catégorie — voir `docs/ml_model.md` pour l'analyse complète des coefficients.

## Sortie Silver ML

`s3://datalake/silver/scraping_ml/scraping_predictions.parquet` — 73 lignes, colonnes Silver originales + les 4 colonnes de prédiction. Silver scraping original (`s3://datalake/silver/scraping/`) **non écrasé**. Lecture de vérification (round-trip) effectuée avec succès.

## Préparation du rapprochement BADR

[`ingestion/ml/prepare_matching.py`](../ingestion/ml/prepare_matching.py) — clé de jointure **`BADR.CODE_NGP` ↔ `SCRAPING.CODE_NGP_PREDIT`** uniquement (jamais marque/modèle, jamais de matching produit individuel). Pas d'arbitrage : ni prix de référence, ni ratio, ni NORMAL/MINORE/MAJORE.

### Analyse du matching (réelle, exécutée)

- Total produits scraping : **73** — tous avec `CODE_NGP_PREDIT` (0 NULL)
- Codes NGP distincts côté scraping : **3**
- Codes NGP distincts côté BADR : **34**
- Codes communs : **2** (`84713000`, `85287200`)
- Produits scraping avec ≥1 correspondance BADR : **50**
- Produits scraping **sans** correspondance BADR : **23** (tous `85171300`)

| CODE_NGP | BADR_COUNT | SCRAPING_COUNT |
|---|---|---|
| 84713000 | 146 | 25 |
| 85287200 | 120 | 25 |
| 85171300 | **0** | 23 |
| *(31 autres codes BADR, ex. 85171200: 105, 94036000: 225, ...)* | >0 | 0 |

**Constat majeur, confirmé sur données réelles (rejoint l'analyse Phase 2.9)** : BADR utilise le code `85171200` (nomenclature pré-2022, 105 déclarations), jamais `85171300` (le code actuel, utilisé ici). C'est pourquoi **0 des 23 smartphones prédits n'a de correspondance BADR** — pas un défaut du modèle ni du matching, mais un vrai écart de nomenclature entre les données BADR simulées et le Tarif douanier actuellement en vigueur. Cet écart n'a pas été corrigé ici (BADR ne doit pas être modifié) ; il est seulement montré, comme demandé.

Sortie : `s3://datalake/silver/matching/ngp_matching_summary.parquet` (35 lignes = union des codes des deux côtés). Les données détaillées ligne-à-ligne restent dans `silver/scraping_ml/` et `silver/badr/` pour la future phase d'arbitrage (jointure sur CODE_NGP, pas de duplication ici).

## Limites

- Les 3 classes restent au niveau HS8 (pas de 10 chiffres) — inchangé depuis la Phase 2.9/2.10.
- Le modèle garde les limites documentées en Phase 2.11 (petit échantillon, dépendance marque/vocabulaire) — désormais illustrées par une vraie erreur (iPhone → PC Portable) plutôt qu'un seul cas synthétique.
- L'écart de nomenclature `85171200` (BADR) vs `85171300` (Tarif actuel) bloque tout rapprochement smartphone tant que BADR n'est pas mis à jour — hors périmètre de cette phase.
- `NGP_CONFIDENCE_LEVEL` est un indicateur ML, pas une règle douanière ; aucune ligne n'a été supprimée pour faible confiance.

## Prochaine étape (hors périmètre de cette phase)

Arbitrage : prix de référence par CODE_NGP, conversion de devise, ratio, classification NORMAL/MINORE/MAJORE, Gold final.
