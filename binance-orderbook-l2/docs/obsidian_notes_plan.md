# Notes Obsidian prévues — À VALIDER avant toute écriture dans le vault

Statut : **en attente** — le MCP Obsidian n'est pas connecté à la session.
Une fois connecté : observer d'abord l'organisation existante du vault
(dossiers, conventions de nommage) et s'y conformer ; sinon proposer la
structure ci-dessous pour validation. Rien ne sera créé sans accord.

## Emplacement proposé (si aucune convention observable)

`Projets/Binance L2 Order Book/`

## Notes prévues (une par module + une transverse)

| Note | Contenu |
|---|---|
| `00 - Vue d'ensemble` | objectif, schéma des flux, liens vers toutes les notes |
| `Config` | dataclasses, validations, ajout d'un paramètre |
| `Exchange Client` | interface abstraite, contrat, comment brancher un 2e exchange |
| `Binance Client` | endpoints REST/WS, nommage des streams, parsing U/u/E |
| `WebSocket Client` | boucle de réception, asyncio.Queue, backoff + jitter, arrêt |
| `Order Book` | SortedDict, application des deltas, qty 0, requêtes d'état |
| `Sequencing` | règle U/u complète, tableau des SeqResult, invariant evaluate/commit |
| `Sync` | machine à états, cycle complet, exceptions internes de contrôle |
| `Capture SQLite` | schéma, thread + queue, lots, snapshots REST (ts_event NULL), rejeu |
| `Décisions techniques` | voir ci-dessous |

Liens : chaque note pointe vers ses dépendances internes en [[wikilinks]]
(ex : [[Sync]] → [[Sequencing]], [[Order Book]], [[WebSocket Client]]).

## Note « Décisions techniques » — points à couvrir

- sortedcontainers plutôt qu'un dict trié maison (O(log n), itération triée
  native, code éprouvé vs maintenance d'une structure critique) ;
- interface abstraite ExchangeClient (second exchange sans réécriture) ;
- SQLite plutôt que JSONL + rotation (requêtage SQL direct — compétence forte
  de l'utilisateur —, fichier unique auto-suffisant, écriture par lots WAL) ;
- Decimal pour les prix (clés exactes : pas de niveau fantôme dû aux flottants) ;
- validateur de séquence pur, séparé de l'orchestrateur (testabilité) ;
- signal standard plutôt que add_signal_handler (ProactorEventLoop Windows) ;
- snapshots REST enregistrés dans la capture (rejeu auto-suffisant).
