# Normalisation NGP et préparation du matching BADR ↔ Scraping (Phase 2.13)

Complète [`docs/ml_prediction.md`](ml_prediction.md) (Phase 2.12), qui avait déjà identifié l'écart de nomenclature `85171200` (BADR) vs `85171300` (Tarif actuel) sans le résoudre.

## Pourquoi BADR contient `85171200`

`data/badr.db` est simulé avec Faker à partir d'une liste de codes NGP plausibles définie une fois pour toutes dans [`ingestion/config.py`](../ingestion/config.py) (`BADR_HS_CODES_BY_CATEGORY["electronique"]`). Cette liste a été fixée **avant** l'analyse officielle du Tarif douanier (Phase 2.9) et contient donc `85171200` — le code de la nomenclature SH **pré-2022** pour les téléphones ("téléphones pour réseaux cellulaires").

## Pourquoi le Tarif actuel utilise `85171300`

L'analyse officielle du Tarif ADII (Édition 1er janvier 2022, Phase 2.9) a montré que la révision SH2022 a renuméroté cette sous-position : `8517.12` (pré-2022) a été remplacée par `8517.13` "Téléphones intelligents" + `8517.14` "Autres téléphones pour réseaux cellulaires". C'est ce code actuel (`85171300`) que le modèle ML (Phase 2.11) prédit pour nos smartphones scrapés.

## Pourquoi BADR n'est pas modifié

BADR est un jeu de données déjà généré, simulant des déclarations douanières historiques réelles. Le corriger reviendrait à falsifier a posteriori des données censées représenter un état réel du passé, et masquerait un écart de nomenclature qui est lui-même une information utile (BADR peut légitimement contenir des déclarations anciennes classées sous un code aujourd'hui obsolète). La consigne du projet est explicite : ne jamais modifier BADR pour "arranger" un résultat.

## Pourquoi la normalisation est faite dans une couche de référence séparée

La réconciliation appartient à la couche de **rapprochement**, pas aux données sources. [`ingestion/ml/ngp_normalization.py`](../ingestion/ml/ngp_normalization.py) contient une petite table de référence, indépendante de BADR et du scraping, appliquée **uniquement en mémoire** lors de la construction du dataset de matching — jamais écrite dans `s3://datalake/silver/badr/` ni dans `data/badr.db`.

## Table de référence

| ancien_code | code_normalise | raison | source |
|---|---|---|---|
| 85171200 | **85171300** | Évolution de nomenclature SH2022 : 8517.12 (pré-2022) → 8517.13 "Téléphones intelligents" | Tarif ADII, Édition 1er janvier 2022 (Phase 2.9) |
| 84713000 | 84713000 (inchangé) | Vérifié exact : position 84.71 / sous-position 8471.30, aucune évolution identifiée | Tarif ADII, Chapitre 84 (Phase 2.9) |
| 85287200 | 85287200 (inchangé) | Vérifié exact : position 85.28 / sous-position 8528.72, aucune évolution identifiée | Tarif ADII, Chapitre 85 (Phase 2.9) |

Persistée séparément à `s3://datalake/silver/reference/ngp_code_normalization.parquet`.

## Quelles correspondances sont autorisées

**Seuls ces 3 codes** sont couverts par la table, parce que ce sont les 3 seuls codes explicitement vérifiés contre le Tarif officiel en Phase 2.9 (ceux de notre périmètre de scraping). `ingestion/ml/ngp_normalization.apply_normalization()` applique la table par correspondance exacte (`ancien_code` → `code_normalise`) ; **tout autre code BADR passe inchangé** (identité) — ce n'est pas une affirmation qu'il est correct, seulement qu'il est hors périmètre de cette analyse. Aucune transformation n'a été inventée ou déduite "parce que ça semblait logique" : chaque ligne cite sa source Phase 2.9.

## Dataset de matching (après normalisation)

`ingestion/ml/prepare_matching.py` (Phase 2.12, étendu ici) rejoint sur **`BADR.CODE_NGP_NORMALISE` ↔ `SCRAPING.CODE_NGP_PREDIT`**. Sortie : `s3://datalake/silver/matching/ngp_matching_summary.parquet` (remplace la version Phase 2.12, même emplacement, convention inchangée) :

| Colonne | Contenu |
|---|---|
| `CODE_NGP` | code commun (normalisé) |
| `CODE_NGP_ORIGINAL_BADR` | code(s) BADR d'origine ayant produit ce code normalisé (traçabilité) |
| `nb_badr` | nombre de déclarations BADR sur ce code normalisé |
| `nb_scraping` | nombre de produits scraping prédits sur ce code |
| `matching_possible` | `nb_badr > 0` et `nb_scraping > 0` |

## Résultat (réel, vérifié)

| Catégorie | BADR original | → normalisé | nb_badr | nb_scraping | matching_possible |
|---|---|---|---|---|---|
| Smartphone | 85171200 | 85171300 | 105 | 23 | **True** (était False avant normalisation) |
| PC Portable | 84713000 | 84713000 | 146 | 25 | True |
| Televiseur | 85287200 | 85287200 | 120 | 25 | True |

**Avant normalisation (Phase 2.12)** : 50/73 produits scraping avaient une correspondance BADR (23 smartphones sans correspondance).
**Après normalisation (Phase 2.13)** : **73/73** produits scraping ont désormais une correspondance BADR possible.

105 des 5000 lignes BADR (celles taguées `85171200`) changent de code dans cette vue de matching uniquement — jamais dans Silver BADR ni `data/badr.db`.

## Limites

- Cette normalisation ne concerne que 3 codes sur les 34 présents dans BADR — les 31 autres (textile, alimentaire, véhicules, etc.) restent non examinés, hors périmètre du scraping actuel.
- Le matching reste au niveau CODE_NGP (agrégat), pas produit-à-produit — aucun rapprochement individuel marque/modèle n'est fait.
- Aucun prix de référence, taux de change, ratio ou classification NORMAL/MINORÉ/MAJORÉ n'est calculé à ce stade.
