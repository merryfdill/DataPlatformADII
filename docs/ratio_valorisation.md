# Ratio de valorisation BADR (Phase 2.17)

Complète [`docs/valeur_badr.md`](valeur_badr.md) (Phase 2.15/2.16) et [`docs/prix_reference.md`](prix_reference.md) (Phase 2.14). Cette phase calcule uniquement `RATIO = VALEUR_MAD / PRIX_REFERENCE` et l'analyse statistiquement — **aucune classification NORMAL/MINORÉ/MAJORÉ, aucun seuil métier, pas de Gold.**

## Sources (toutes en lecture seule)

- `s3://datalake/silver/badr_valeur/` (Phase 2.16, 5000 lignes) — `CODE_NGP` y est encore le code **brut** BADR (non normalisé)
- `s3://datalake/silver/reference/ngp_code_normalization.parquet` (Phase 2.13) — table de normalisation
- `s3://datalake/silver/reference/prix_reference/` (Phase 2.14) — prix de référence par code normalisé

## Rapprochement sur le CODE_NGP normalisé

Le job [`spark/jobs/ratio_valorisation.py`](../spark/jobs/ratio_valorisation.py) réutilise **la table de normalisation déjà persistée en Phase 2.13** (pas de nouvelle règle inventée, pas de duplication de la logique) : `badr_valeur.CODE_NGP` est d'abord normalisé (`85171200 → 85171300`, les autres codes inchangés par défaut), puis la jointure vers `prix_reference` se fait sur ce code normalisé.

## PRIX_REFERENCE utilisé

`PRIX_REFERENCE = PRIX_MEDIAN` de la table Phase 2.14 — la médiane est la statistique de référence standard, robuste aux valeurs extrêmes, déjà mise en avant dans le calcul du prix de référence. Choix documenté explicitement, pas une hypothèse silencieuse.

## Validations avant calcul (réelles, exécutées)

| Vérification | Résultat |
|---|---|
| Lignes `badr_valeur` lues | 5000 |
| Lignes après jointure (avant filtre) | 5000 (aucune perte à la jointure) |
| `VALEUR_MAD` NULL | 0 |
| `PRIX_REFERENCE` NULL | 4629 |
| `PRIX_REFERENCE` ≤ 0 | 0 |
| Lignes retenues pour le calcul | **371** / 5000 |

**Aucune ligne supprimée sans justification** : les 4629 lignes exclues le sont uniquement parce que leur `CODE_NGP` normalisé ne fait pas partie des 3 catégories scrapées (85171300/84713000/85287200) — donc aucun `PRIX_REFERENCE` n'existe pour elles. C'est exactement le même constat que la Phase 2.13 (371 = 105 + 146 + 120, matching_possible). Rien n'a été deviné pour combler ces 31 autres codes NGP.

## Vérification manuelle (réelle)

| BADR_ID | CODE_NGP | VALEUR_MAD | PRIX_REFERENCE | RATIO attendu | RATIO obtenu |
|---|---|---|---|---|---|
| 4 | 85287200 | 218818,467384 | 2040,0 | 107,263955 | 107,263955 ✓ |
| 14 | 85287200 | 18438,505237 | 2040,0 | 9,038483 | 9,038483 ✓ |
| 20 | 85287200 | 37753,161 | 2040,0 | 18,506451 | 18,506451 ✓ |

## Table Silver produite

`s3://datalake/silver/badr_ratio/` (371 lignes) :

| Colonne | Contenu |
|---|---|
| BADR_ID | identifiant BADR (`id`) |
| CODE_NGP | code normalisé (85171300/84713000/85287200) |
| VALEUR_MAD | valeur de la déclaration convertie en MAD (Phase 2.16) |
| PRIX_REFERENCE | médiane du prix retail scrapé pour ce code (Phase 2.14) |
| RATIO | VALEUR_MAD / PRIX_REFERENCE |
| DEVISE | devise d'origine de la déclaration (EUR/USD/GBP) |
| DATE_DEPOT | date de dépôt de la déclaration |

0 NULL sur toutes les colonnes (vérifié indépendamment).

## Statistiques du RATIO

**Global (371 lignes) :**

| NB_LIGNES | MIN | Q1 | MÉDIANE | MOYENNE | Q3 | MAX | Valeurs aberrantes* |
|---|---|---|---|---|---|---|---|
| 371 | 1,514 | 14,588 | 25,475 | 41,389 | 51,428 | 302,731 | 33 |

**Par CODE_NGP :**

| CODE_NGP | NB_LIGNES | MIN | Q1 | MÉDIANE | MOYENNE | Q3 | MAX | Aberrantes* |
|---|---|---|---|---|---|---|---|---|
| 85171300 (Smartphone) | 105 | 4,624 | 20,178 | 33,503 | 53,219 | 62,013 | 245,855 | 11 |
| 84713000 (PC Portable) | 146 | 1,514 | 10,612 | 17,733 | 27,478 | 33,068 | 252,684 | 11 |
| 85287200 (Televiseur) | 120 | 4,703 | 16,422 | 31,605 | 47,963 | 59,669 | 302,731 | 7 |

*\*Méthode de Tukey (borne = Q1 − 1,5×IQR / Q3 + 1,5×IQR) — une convention statistique descriptive standard, pas un seuil métier douanier. Aucun seuil NORMAL/MINORÉ/MAJORÉ n'est défini ici.*

Sortie : `s3://datalake/silver/badr_ratio_stats/` (4 lignes : 1 globale "ALL" + 1 par CODE_NGP).

## Lecture (sans classification)

Un `RATIO` > 1 signifie que `VALEUR_MAD` dépasse le prix médian retail scrapé pour cette catégorie ; toutes les médianes de ratio observées sont nettement > 1 (17,7 à 33,5). Ceci reflète très probablement le constat déjà documenté en Phase 2.15 : `VALEUR` semble représenter une valeur agrégée de déclaration/expédition plutôt qu'un prix unitaire (absence de `QUANTITE` dans BADR), donc comparer directement à un prix retail unitaire scrapé donne mécaniquement un ratio élevé. **Cette phase ne tire aucune conclusion de classification** — l'interprétation métier (NORMAL/MINORÉ/MAJORÉ) est explicitement hors périmètre.

## Limites

- `RATIO` compare une valeur potentiellement agrégée (BADR) à un prix unitaire (scraping retail) — voir `docs/valeur_badr.md`, question totale/unitaire non résolue.
- Seuls 371/5000 déclarations BADR (7,4%) sont couvertes, celles dont le NGP normalisé correspond à l'une des 3 catégories scrapées.
- Les valeurs aberrantes signalées (méthode de Tukey) ne sont qu'une description statistique, pas un verdict de fraude ou d'erreur.
