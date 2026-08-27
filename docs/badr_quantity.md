# Ajout de QUANTITE au générateur BADR (Phase 2.19)

Complète [`docs/valeur_badr.md`](valeur_badr.md) (Phase 2.15/2.16) et le diagnostic [Phase 2.18](../ingestion/badr/generate_badr.py). Cette phase modifie `data/badr.db` (avec sauvegarde préalable) — c'est la seule phase à ce jour à le faire.

## Pourquoi QUANTITE a été ajoutée

La Phase 2.18 a démontré que `VALEUR`/`POIDS` étaient générés indépendamment de toute notion de quantité, rendant `RATIO = VALEUR_MAD / PRIX_REFERENCE` mathématiquement incomparable (valeur globale de déclaration ÷ prix unitaire retail) et physiquement incohérent (des déclarations pesant plusieurs centaines de kg sans savoir combien d'unités elles contiennent). `QUANTITE` comble ce vide pour permettre, dans une phase future, `VALEUR_UNITAIRE_MAD = VALEUR_MAD / QUANTITE`, comparable à `PRIX_REFERENCE`.

## Rôle métier

`QUANTITE` = taille du lot commercial de la déclaration (nombre d'unités), entier ≥ 1. C'est une information qui existe réellement dans une déclaration douanière réelle — son absence dans la version précédente de la simulation était une lacune du simulateur, pas une caractéristique voulue de BADR.

## Comment elle est générée

Dans [`ingestion/badr/generate_badr.py`](../ingestion/badr/generate_badr.py), `QUANTITE` est tirée d'une loi log-normale dont les paramètres dépendent de la **catégorie métier** déjà utilisée pour choisir `CODE_NGP` (les 8 catégories de `config.BADR_HS_CODES_BY_CATEGORY` — réutilisation de ce qui existait déjà, aucune architecture parallèle) :

```python
quantite = max(1, round(rng.lognormvariate(params["qty_mu"], params["qty_sigma"])))
```

Reproductible via le même mécanisme de seed déjà présent (`random.Random(args.seed)`, défaut 42) — aucun nouveau système de graine introduit.

Paramètres par catégorie (`config.BADR_QUANTITY_PARAMS_BY_CATEGORY`), jugement d'ordre de grandeur documenté, pas dérivé du scraping/ML :

| Catégorie | Poids unitaire typique | Quantité médiane visée |
|---|---|---|
| electronique (smartphones, TV, ...) | 2,0 kg | ~40 |
| machines (PC portables, électroménager, ...) | 20,0 kg | ~15 |
| textile | 0,3 kg | ~300 |
| vehicules | 150,0 kg | ~5 |
| alimentaire | 0,8 kg | ~250 |
| plastique | 1,5 kg | ~150 |
| meubles | 30,0 kg | ~10 |
| jouets | 0,4 kg | ~200 |

## Cohérence avec POIDS

`POIDS_INITIAL` (puis `POIDS` via la même logique de "pesée réelle" déjà existante, inchangée) est maintenant généré comme `QUANTITE × poids_unitaire_typique × bruit(0.9–1.1)`, au lieu d'un tirage totalement indépendant. La logique "déclaré → pesée effective" (80% quasi identique, 20% écart) n'a pas été modifiée.

## Cohérence avec VALEUR

Même principe : `VALEUR_INITIALE` = `QUANTITE × valeur_unitaire_synthétique × bruit(0.9–1.1)`, où la valeur unitaire est un **nouveau tirage aléatoire synthétique indépendant** (log-normal, paramétré par catégorie), **jamais lu depuis `PRIX_REFERENCE` ni le scraping/ML**. Ceci reflète la logique standard d'une facture commerciale (total = prix unitaire × quantité) — ce n'est pas la relation interdite `VALEUR = QUANTITE × PRIX_REFERENCE` (qui aurait fait fuiter le prix retail réel dans BADR). La logique "déclaré → évaluation de l'inspecteur" (60/25/15%) n'a pas été modifiée.

Aucune relation artificielle `CODE_NGP → VALEUR` ou `MARQUE → VALEUR` n'a été créée : la seule dépendance est `catégorie métier (8 groupes) → paramètres de quantité/poids/valeur unitaire`, qui existait déjà implicitement pour le choix du `CODE_NGP` lui-même.

## Statistiques (réelles, sur les 5000 lignes régénérées)

**QUANTITE globale :** min 1 · Q1 12 · médiane 61 · moyenne 201,9 · Q3 235 · max 7732 · 0 NULL · 0 valeur ≤ 0

**POIDS globale :** min 2,44 kg · médiane 189,69 kg · moyenne 384,22 kg · max 9692,19 kg

**POIDS/QUANTITE (kg/unité) globale :** min 0,236 · Q1 0,435 · médiane 1,67 · moyenne 26,31 · Q3 24,15 · max 181,88 (l'écart mean/médiane global reflète le mélange des 8 catégories très hétérogènes, de 0,3 à 150 kg/unité — attendu, pas une anomalie)

**Par CODE_NGP (34 codes) :** disponible en intégralité dans les logs d'exécution ; exemples représentatifs :

| CODE_NGP | n | QUANTITE médiane | QUANTITE max |
|---|---|---|---|
| 85171200 (Smartphone) | 118 | 40 | 544 |
| 84713000 (PC Portable) | 109 | 16 | 207 |
| 85287200 (Televiseur) | 111 | 44 | 421 |
| 87032319 (vehicules) | 131 | 5 | 601 |
| 95030030 (jouets) | 339 | 209 | 4472 |

### Statistiques pour les 3 catégories du périmètre ML

| CODE_NGP | n | QUANTITE (min/médiane/max) | POIDS (min/médiane/max) | POIDS_PAR_UNITE (min/médiane/max) |
|---|---|---|---|---|
| 85171300 (Smartphone) | 118 | 6 / 40 / 544 | 12,07 / 78,26 / 1074,45 kg | 0,86 / 1,99 / 2,45 kg |
| 84713000 (PC Portable) | 109 | 1 / 16 / 207 | 17,93 / 328,43 / 3799,80 kg | 16,07 / 20,21 / 31,33 kg |
| 85287200 (Televiseur) | 111 | 3 / 44 / 421 | 6,19 / 95,09 / 2331,25 kg | 0,37 / 2,01 / **169,02** kg |

### Exemples réels (lignes typiques, non reclassifiées)

| CODE_NGP | QUANTITE | POIDS | POIDS_PAR_UNITE | VALEUR | DEVISE |
|---|---|---|---|---|---|
| 85171200 (id=1) | 24 | 45,54 kg | 1,90 kg | 3654,67 | USD |
| 85171200 (id=36) | 6 | 12,07 kg | 2,01 kg | 1128,10 | USD |
| 84713000 (id=103) | 52 | 1159,32 kg | 22,29 kg | 33960,47 | USD |
| 85287200 (id=7) | 95 | 177,43 kg | 1,87 kg | 7570,07 | USD |
| 85287200 (id=32) | 32 | 56,46 kg | 1,76 kg | 2802,13 | USD |

### Valeurs aberrantes signalées (non supprimées)

Le max de 169,02 kg/unité pour `85287200` provient de 2 déclarations (id=3053, id=4039) dont `CODE_NGP_INITIAL` était respectivement `87168000` (véhicules, 150 kg/unité type) et `84501100` (machines, 20 kg/unité type), **reclassifiées** vers `85287200` par la logique de reclassification déjà existante (3% des lignes, inchangée). `QUANTITE`/`POIDS` restent cohérents avec la catégorie **déclarée à l'origine**, pas le code final après reclassification — reflet réaliste : les marchandises physiques ne changent pas de poids parce que leur code douanier est corrigé après coup. Sur les 3 catégories ML : 6/118 (Smartphone), 10/109 (PC Portable), 15/111 (Televiseur) lignes sont dans ce cas — non supprimées, signalées ici.

## Limites

- Les paramètres de poids/quantité/valeur unitaire sont par **catégorie métier large** (8 groupes), pas par `CODE_NGP` individuel. `electronique` regroupe smartphones ET téléviseurs sous un même poids unitaire (~2 kg) — réaliste pour un smartphone, sous-évalué pour un vrai téléviseur (qui pèserait plutôt 10-15 kg). `machines` regroupe PC portables et gros électroménager sous ~20 kg/unité — trop lourd pour un vrai PC portable (~2 kg). C'est une simplification MVP assumée, pas une erreur : aller au niveau `CODE_NGP` aurait été une architecture parallèle plus complexe, hors de ce qui était demandé.
- Les déclarations reclassifiées (~3% du total, logique préexistante inchangée) gardent la physique de leur catégorie d'origine, pas du code final — cohérent avec la réalité douanière, mais produit les quelques valeurs aberrantes signalées ci-dessus.
- `VALEUR`/`VALEUR_INITIALE` restent des tirages synthétiques indépendants du prix retail réel — aucune garantie que `VALEUR_UNITAIRE_MAD` futur sera proche de `PRIX_REFERENCE`, seulement que le calcul sera désormais mathématiquement cohérent (comparaison unité à unité).
