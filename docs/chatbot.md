# Chatbot LLM (Phase 2.24)

Complète [`docs/grafana_dashboard.md`](grafana_dashboard.md) (Phase 2.23). Couche d'interrogation et d'explication en langage naturel au-dessus de la Gold — **le LLM ne calcule jamais l'arbitrage, il le lit et l'explique**.

## Architecture

```
Utilisateur
  -> Interface web (chatbot/static/index.html, HTML/JS vanille)
  -> Backend FastAPI (chatbot/app.py)
  -> GroqCloud (modele configurable, defaut llama-3.3-70b-versatile)
  -> Tool calling controle (chatbot/tools.py, 5 fonctions fixes)
  -> Trino (lecture seule, catalogue iceberg, schema gold)
  -> Gold/dbt (fct_arbitrage, mart_arbitrage_kpi, mart_arbitrage_par_ngp, mart_arbitrage_temps)
  -> resultats reels renvoyes au LLM
  -> GroqCloud compose la reponse en francais a partir de CES resultats
  -> reponse + requetes SQL executees affichees a l'utilisateur (transparence)
```

Aucune infrastructure dupliquee : nouveau service Docker `chatbot` (le seul composant qui manquait), connecte au `trino` deja existant, sur le meme `adii-network`.

## Pourquoi ce n'est pas un texte-vers-SQL libre

Le LLM n'a **jamais** un accès SQL direct. Il ne peut appeler que 5 fonctions Python fixes (`chatbot/tools.py`), chacune construite à partir de paramètres strictement validés (regex, enums, `int()`, jamais de texte libre interpolé). `run_select()` est un filet de sécurité supplémentaire : refuse tout ce qui n'est pas un `SELECT` pur sur une des 4 tables Gold autorisées (mots-clés `INSERT/UPDATE/DELETE/DROP/ALTER/CREATE/MERGE/TRUNCATE/GRANT/REVOKE/COMMIT/ROLLBACK/CALL` bloqués). C'est un contrôle applicatif (pas une permission Trino/Iceberg au niveau base — ce projet n'a pas de plugin d'autorisation configuré), documenté ici comme tel plutôt que présenté comme une garantie base de données.

## Rôle du LLM vs rôle du pipeline

| | Pipeline (Spark + dbt) | Chatbot (GroqCloud) |
|---|---|---|
| Calcule `RATIO_UNITAIRE` | Oui (Phase 2.20) | Non |
| Décide `NORMAL/MINORE/MAJORE` | Oui, seuil absolu `RATIO_UNITAIRE` vs `1 ± 10 %` (règle métier, `arbitrage_gold.py`) | Non — lit le verdict déjà écrit dans Gold |
| Peut modifier un verdict | — | **Jamais** |
| Peut inventer un chiffre | — | **Jamais** — toute statistique vient d'un appel d'outil |
| Explique un verdict | — | Oui, à partir du `RATIO_UNITAIRE` réel et du ratio moyen/médian réel de la catégorie (`mart_arbitrage_par_ngp`) |

La règle est un seuil absolu (`1 ± ARBITRAGE_SEUIL_*_PCT/100`, défaut 10 %, cf. [`arbitrage_gold.md`](arbitrage_gold.md)) ; les bornes **ne sont pas stockées dans Gold** (`DESCRIBE fct_arbitrage` / `mart_arbitrage_par_ngp` n'ont pas ces colonnes). Le prompt système autorise le LLM à citer la valeur par défaut (10 %) en la présentant comme paramétrable, mais lui interdit d'inventer un autre chiffre ou de recalculer un verdict ; pour situer une déclaration il compare son ratio à 1 et au ratio moyen/médian réel de sa catégorie.

## Modèle Groq utilisé

`llama-3.3-70b-versatile` (configurable via `GROQ_MODEL` dans `.env`, défaut appliqué par `docker-compose.yml` si absent). Clé API lue depuis `GROQ_API_KEY` (`.env`, jamais committée, jamais affichée/loggée par le code).

## Tables Gold interrogées (uniquement, vérifiées via `DESCRIBE` avant d'écrire le code)

| Table | Colonnes réelles (Trino, minuscules) |
|---|---|
| `iceberg.gold.fct_arbitrage` | `badr_id, date_depot, code_ngp, code_ngp_initial, est_reclassifie, pays, devise, quantite, valeur, valeur_mad, prix_reference, valeur_unitaire_mad, ratio_unitaire, arbitrage` |
| `iceberg.gold.mart_arbitrage_kpi` | `nb_declarations, nb_normal, nb_minore, nb_majore, pct_normal, pct_minore, pct_majore, ratio_moyen, ratio_median, valeur_totale_mad, quantite_totale` |
| `iceberg.gold.mart_arbitrage_par_ngp` | `code_ngp, nb_declarations, nb_normal, nb_minore, nb_majore, taux_minoration, taux_majoration, ratio_median, ratio_moyen, valeur_totale_mad, quantite_totale` |
| `iceberg.gold.mart_arbitrage_temps` | `mois, nb_declarations, nb_normal, nb_minore, nb_majore, ratio_moyen, valeur_totale_mad` |

Aucun accès direct à `data/badr.db`/SQLite brut — tout passe par Gold.

## Outils (tools) créés

| Outil | Rôle | Table source |
|---|---|---|
| `get_kpi_globaux` | KPI globaux (aucun paramètre) | `mart_arbitrage_kpi` |
| `get_kpi_par_ngp` | KPI par catégorie (code ou nom : Smartphone/PC Portable/Televiseur) | `mart_arbitrage_par_ngp` |
| `get_kpi_temporel` | Évolution mensuelle, filtrable par période | `mart_arbitrage_temps` |
| `rechercher_declarations` | Recherche filtrée/triée de déclarations (ratio, valeur, quantité) | `fct_arbitrage` |
| `expliquer_declaration` | Détail complet d'une déclaration (par `badr_id`) + contexte de sa catégorie | `fct_arbitrage` + `mart_arbitrage_par_ngp` |

`resolve_code_ngp()` accepte les noms de catégorie (ex. "smartphones") en plus des codes NGP bruts, via une table de correspondance dupliquée depuis `ingestion/config.py::SCRAPING_CATEGORY_TO_NGP8` (Phase 2.8) — le service chatbot reste indépendant des dépendances de scraping.

## Sécurité / read-only

- 5 fonctions fixes, aucune génération de SQL libre par le LLM.
- Toute requête est validée par `run_select()` : doit commencer par `SELECT`, doit référencer une des 4 tables Gold autorisées, ne doit contenir aucun mot-clé de modification.
- `GROQ_API_KEY` lu uniquement côté backend (container `chatbot`), jamais exposé au navigateur (`static/index.html` n'appelle que `/api/chat`, jamais Groq directement).
- `.env` vérifié non suivi par Git (`.gitignore` déjà présent) ; recherche `git grep "gsk_"` sur l'ensemble du dépôt suivi : aucun résultat.
- La clé n'apparaît dans aucun fichier créé/modifié dans cette phase (`chatbot/`, `docker-compose.yml`, `.env.example` ne contiennent que `${GROQ_API_KEY}` ou une valeur vide).

## Interface

Page unique HTML/JS (`chatbot/static/index.html`, servie par le backend, `http://localhost:8501/`) : champ de saisie, bouton d'envoi, historique de conversation, indicateur de chargement, affichage d'erreur, section dépliable "Données utilisées" par réponse (outil appelé, table source, requête SQL exécutée) — transparence demandée par la Phase 2.24.

## Exemples de questions testées (voir Validations)

- « Combien de déclarations au total sont analysées ? »
- « Combien de déclarations sont NORMAL, MINORE et MAJORE ? »
- « Combien de déclarations MAJORE pour les smartphones ? »
- « Quel est le ratio moyen pour les PC portables ? »
- « Quelle est la valeur totale déclarée en MAD ? »
- « Pourquoi la déclaration BADR_ID 170 est-elle classée MINORE ? »
- « Explique-moi pourquoi la déclaration BADR_ID 334 est classée MAJORE. »
- « Quelle est la capitale de la France ? » (hors périmètre)
- « Quel est le seuil exact utilisé pour les smartphones ? » (piège : il n'y a plus de seuil par catégorie ; le LLM doit répondre « seuil absolu, 10 % par défaut, paramétrable » sans inventer une valeur par produit)

## Limites

- **Quota gratuit GroqCloud** : le compte utilisé pendant cette phase a atteint sa limite quotidienne de tokens (100 000 TPD) pendant les tests intensifs. Le backend gère cette erreur proprement (message d'erreur clair, pas de crash, aucune donnée inventée) — testé et confirmé en conditions réelles.
- Comportement occasionnel observé : sur une formulation directe de "seuil exact", le modèle a une fois tenté d'appeler `expliquer_declaration` avec une valeur d'exemple non numérique (rejeté par la validation de schéma de Groq) ; corrigé par un ajustement du prompt système interdisant les appels d'outil avec des valeurs de type "exemple"/placeholder, et par une gestion d'erreur qui empêche tout crash même si cela se reproduit.
- Pas de contrôle d'accès Trino au niveau base (pas de plugin d'autorisation configuré dans ce projet) — le read-only est appliqué au niveau applicatif (liste blanche de tables + blocage de mots-clés), documenté comme tel.
- Pas de mémoire de conversation persistante côté serveur (l'historique est renvoyé par le frontend à chaque requête, rien n'est stocké côté backend).
- Pas de RAG documentaire dans cette phase (Tarif douanier, réglementation) — hors périmètre explicite de la Phase 2.24, pourra être ajouté plus tard.
