# Plan de la page Notion « vue projet » — À VALIDER avant création

Statut : **en attente** de la page Notion exemple (pour reprendre son style de
structure/formatting). La page sera créée via le MCP Notion connecté, après
validation de ce plan.

## Contenu prévu

1. **Titre** : Binance L2 Order Book Reconstructor
   Résumé en une phrase : reconstruction locale temps réel des carnets d'ordres
   L2 de plusieurs paires Binance (7 par défaut : BTC, ETH, SOL, NEAR, HYPE,
   ONDO, RENDER /USDT) (snapshot REST + flux WebSocket @depth@100ms), avec
   resynchronisation automatique et capture SQLite optionnelle.
2. **Architecture** — tableau à deux colonnes (module → rôle) reprenant les
   11 modules listés dans le README (config, exchange, binance_client,
   ws_client, sequencing, orderbook, sync, capture, metrics, ui, main).
3. **Installation & lancement** — bloc de code : venv, pip install,
   copie de config.example.yaml, `python main.py`.
4. **Paramètres de configuration** — référence au fichier `config.yaml`,
   tableau des sections (exchange/symbol/depth_limit/ws_speed_ms, display,
   capture, network, binance, logging) avec valeurs par défaut.
5. **Règle de synchronisation U/u** — résumé : bufferisation dès la connexion,
   purge des u ≤ lastUpdateId, premier événement U ≤ lastUpdateId+1 ≤ u,
   continuité stricte U == u_précédent+1, toute rupture = rejet complet +
   resync. Gestion des reconnexions : backoff exponentiel + jitter,
   coupure 24 h Binance couverte.
6. **État actuel / prochaines étapes** — section libre, modifiable par
   l'utilisateur (pré-remplie : « fonctionnel, testé ; rejeu depuis capture.db
   et second exchange envisagés »).

Questions ouvertes : emplacement de la page (quel espace/parent ?),
et style exact — repris de la page exemple à fournir.
