# Ratio de valorisation unitaire (Phase 2.20)

Complète [`docs/ratio_valorisation.md`](ratio_valorisation.md) (Phase 2.17), [`ingestion/badr/generate_badr.py`](../ingestion/badr/generate_badr.py) (diagnostic Phase 2.18) et [`docs/badr_quantity.md`](badr_quantity.md) (Phase 2.19).

## Pourquoi l'ancien ratio était invalide

Phase 2.17 : `RATIO = VALEUR_MAD / PRIX_REFERENCE`. Phase 2.18 a montré que `VALEUR_MAD` est une valeur **globale de déclaration** (plusieurs unités), alors que `PRIX_REFERENCE` est un prix **unitaire** de détail — division mathématiquement incohérente, conflatant taille du lot et éventuelle sur/sous-évaluation. Résultat observé en Phase 2.17 : médiane ≈ 25,5, moyenne ≈ 41,4, max ≈ 302,7 — des ordres de grandeur non interprétables économiquement.

## Comment QUANTITE résout le problème de granularité

Phase 2.19 a ajouté `QUANTITE` (nombre d'unités par déclaration) au générateur BADR et régénéré `data/badr.db`. Il devient possible de ramener `VALEUR_MAD` à une base unitaire comparable à `PRIX_REFERENCE`.

## Formules

```
VALEUR_UNITAIRE_MAD = VALEUR_MAD / QUANTITE
RATIO_UNITAIRE      = VALEUR_UNITAIRE_MAD / PRIX_REFERENCE
                     = (VALEUR_MAD / QUANTITE) / PRIX_REFERENCE
```

Calculées en pleine précision décimale (`DecimalType(28,10)`), aucun arrondi avant le calcul du ratio.

## Sources (toutes en lecture seule)

- `s3://datalake/silver/badr_valeur/` — **rafraîchi dans cette phase** avant le calcul : la version existante (Phase 2.16) ne contenait pas `QUANTITE` (elle n'existait pas encore à l'époque) et datait d'avant la régénération BADR de la Phase 2.19. [`spark/jobs/badr_valeur_prep.py`](../spark/jobs/badr_valeur_prep.py) a été mis à jour a minima (ajout de `QUANTITE` à la liste `.select()`, aucune autre logique modifiée) et réexécuté.
- `s3://datalake/silver/reference/ngp_code_normalization.parquet` (Phase 2.13, même table, même règle que Phase 2.17)
- `s3://datalake/silver/reference/prix_reference/` (Phase 2.14, **non recalculé**, Jumia non requêté dans cette phase)

## Vérifications avant calcul (réelles)

| Vérification | Résultat |
|---|---|
| Lignes après jointure | 5000 (= source, aucune perte) |
| `VALEUR_MAD` NULL | 0 |
| `QUANTITE` NULL | 0 |
| `QUANTITE` ≤ 0 | 0 |
| `PRIX_REFERENCE` NULL | 4662 |
| `PRIX_REFERENCE` ≤ 0 | 0 |
| `CODE_NGP` NULL | 0 |
| **Lignes retenues** | **338 / 5000** |

**Rapprochement NGP** : uniquement sur les codes normalisés (85171300/84713000/85287200) — aucune association artificielle. Les 4662 lignes exclues le sont **uniquement** parce que leur `CODE_NGP` normalisé est hors du périmètre des 3 catégories scrapées (aucun `PRIX_REFERENCE` n'existe pour elles) — c'est une conséquence directe et déjà documentée du périmètre du scraping (Phase 2.13), pas une perte de données arbitraire.

Répartition des 338 lignes retenues : `84713000`→109 · `85171300`→118 · `85287200`→111.

## Vérification manuelle (5 lignes, toutes exactes)

| BADR_ID | CODE_NGP | VALEUR_MAD | QUANTITE | VALEUR_UNITAIRE_MAD | PRIX_REFERENCE | RATIO_UNITAIRE |
|---|---|---|---|---|---|---|
| 1 | 85171300 | 33995,37 | 24 | 1416,47 | 1549,0 | 0,9144 |
| 7 | 85287200 | 70416,03 | 95 | 741,22 | 2040,0 | 0,3633 |
| 32 | 85287200 | 26065,13 | 32 | 814,54 | 2040,0 | 0,3993 |
| 36 | 85171300 | 10493,47 | 6 | 1748,91 | 1549,0 | 1,1291 |
| 48 | 85171300 | 23415,44 | 11 | 2128,68 | 1549,0 | 1,3742 |

Calcul de contrôle (exemple de l'énoncé) : 500 000 / 100 = 5000 MAD ✓ (logique identique appliquée).

## Statistiques RATIO_UNITAIRE (338 lignes)

**Global :** min 0,101 · Q1 0,636 · médiane **1,045** · moyenne **1,330** · Q3 1,609 · max 8,115 · écart-type 1,100 · 20 valeurs aberrantes (Tukey 1,5×IQR)

**Par CODE_NGP :**

| CODE_NGP | n | Ratio min | Q1 | médiane | moyenne | Q3 | max | aberrantes |
|---|---|---|---|---|---|---|---|---|
| 85171300 (Smartphone) | 118 | 0,101 | 0,705 | 1,079 | 1,429 | 1,736 | 6,309 | 7 |
| 84713000 (PC Portable) | 109 | 0,384 | 0,894 | 1,464 | 1,656 | 2,055 | 6,826 | 6 |
| 85287200 (Televiseur) | 111 | 0,157 | 0,448 | 0,735 | 0,905 | 1,090 | 8,115 | 3 |

**VALEUR_UNITAIRE_MAD par CODE_NGP :** Smartphone (min 155,81 / médiane 1672,02 / max 9772,12), PC Portable (min 1113,79 / médiane 4243,87 / max 19789,84), Televiseur (min 320,58 / médiane 1500,13 / max 16554,96) — comparables en ordre de grandeur à `PRIX_REFERENCE` (1549 / 2899 / 2040 MAD), signe que le calcul est désormais dimensionnellement cohérent.

## Comparaison avec l'ancien ratio (Phase 2.17)

25 lignes comparables (présentes dans les deux calculs — l'ancien `badr_ratio` datant d'avant la régénération BADR Phase 2.19, la comparaison est illustrative, pas une réconciliation stricte ligne à ligne). Écarts les plus marqués :

| BADR_ID | CODE_NGP | ANCIEN_RATIO | RATIO_UNITAIRE |
|---|---|---|---|
| 4807 | 85287200 | 121,24 | **0,99** |
| 4462 | 85171300 | 109,43 | **1,70** |
| 637 | 85287200 | 64,43 | **1,03** |
| 2100 | 84713000 | 59,53 | **2,51** |
| 4688 | 85171300 | 51,53 | **1,44** |

Le nouveau `RATIO_UNITAIRE` (médiane globale 1,045, cohérent avec "prix déclaré ≈ prix de référence") est **radicalement plus interprétable** que l'ancien (médiane 25,5) — confirmation empirique du diagnostic de la Phase 2.18.

## Destination

- `s3://datalake/silver/badr_ratio_unitaire/` (338 lignes) : `BADR_ID, CODE_NGP, DEVISE, DATE_DEPOT, QUANTITE, VALEUR_MAD, VALEUR_UNITAIRE_MAD, PRIX_REFERENCE, RATIO_UNITAIRE, POIDS` — 0 NULL sur toutes les colonnes
- `s3://datalake/silver/badr_ratio_unitaire_stats/` (4 lignes : global + 3 par CODE_NGP)
- `s3://datalake/silver/badr_ratio/` (Phase 2.17) **non écrasé** — reste disponible comme historique, vérifié inchangé après ce job (371 lignes)

## Limites

- 338/5000 déclarations BADR (6,76%) couvertes — seules celles dont le NGP normalisé correspond aux 3 catégories scrapées.
- 20 valeurs aberrantes (Tukey) signalées, non supprimées — probablement liées aux ~3% de déclarations reclassifiées (Phase 2.19) dont `QUANTITE`/`POIDS` restent cohérents avec la catégorie d'origine, pas le code final.
- `VALEUR`/`QUANTITE` restent des tirages synthétiques indépendants du prix retail réel (Phase 2.19) — la cohérence dimensionnelle est désormais assurée, sans garantie que `RATIO_UNITAIRE` reflète une réalité économique précise.
- Aucun seuil NORMAL/MINORÉ/MAJORÉ n'est défini ici — uniquement la distribution du ratio.
