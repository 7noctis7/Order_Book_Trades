# Binance L2 Order Book Reconstructor

Reconstruction locale, en temps réel, des carnets d'ordres L2 de plusieurs paires Binance Spot simultanément (BTC, ETH, SOL, NEAR, ONDO, RENDER contre USDT par défaut — liste libre dans `config.yaml`) : snapshot REST + flux WebSocket `@depth@100ms`, vérification stricte de la continuité de séquence (protocole U/u), resynchronisation automatique sur toute rupture, mode démo console multi-paires et capture brute optionnelle en SQLite. Chaque paire vit dans une pile totalement isolée (connexion WebSocket, file, carnet, machine à états) : un resync, une déconnexion ou un symbole invalide n'affecte jamais les autres paires. Flux public en lecture seule — aucune clé API.

S'y ajoutent : **paper trading** complet (ordres marché/limite/stop contre le carnet reconstruit, lots FIFO, PnL nets de frais), **backtest par rejeu** de la capture SQLite à vitesse variable, **statistiques de performance** (win rate, profit factor, max drawdown, courbe d'equity, export CSV), flux **@trade** (last, VWAP, déclenchements réalistes), un second exchange (**Kraken**, WebSocket v2 avec checksum CRC32), un **export Prometheus** avec dashboard Grafana, et une **version web autonome** (`web/index.html`) : bougies temps réel multi-horizons (1m → 1M) et carte de liquidité type Bookmap.

## Installation

```bash
python -m venv venv
venv\Scripts\activate          # Windows  (Linux/macOS : source venv/bin/activate)
pip install -r requirements.txt
copy config.example.yaml config.yaml   # Linux/macOS : cp
```

Prérequis : Python 3.11+. Toute la configuration (paire, profondeur, cadence, affichage, capture, endpoints, backoff, logging) se fait dans `config.yaml` — rien n'est codé en dur.

## Lancement

```bash
python main.py                 # utilise config.yaml
python main.py --config autre.yaml
```

L'écran affiche un tableau de bord de toutes les paires — mid, spread en bp, jauge d'imbalance, latence p50, débit, état (● streaming, spinner en synchro, ✖ indisponible) — et le carnet détaillé de la paire au focus : ladder asks/bids avec profondeur cumulée, spread central, compteurs. **Navigation : `Tab`/`→` paire suivante, `←` précédente, `1`-`9` sélection directe.** Pendant la synchronisation d'une paire, sa progression dans la machine à états (`CONNECTING → BUFFERING → SNAPSHOT_FETCHED → DISCARDING_STALE → VALIDATING_FIRST → STREAMING`) est affichée. Les décimales de prix et de quantités sont déduites automatiquement du tick réel de chaque paire (`price_decimals: auto`) — BTC s'affiche en 0.01, ONDO en 0.0001, sans configuration. Rafraîchissement : `display.refresh_seconds` (1 s par défaut). Avec 6 paires et `levels: 8`, prévoir un terminal d'au moins ~32 lignes (sinon réduire `levels`). **Ctrl+C** déclenche un arrêt propre : fermeture de la socket WebSocket, flush et fermeture de la base de capture, restauration du terminal.

`display.enabled: false` bascule en mode service : pas d'affichage, logs sur la console.

## Interpréter les logs de resynchronisation

Les logs vont dans `lob.log` (configurable) ; les deux dernières alertes sont aussi affichées en pied d'écran.

- `rupture de séquence → resynchronisation complète : RESYNC_GAP — reçu U=…, u=…, attendu U=…` : un trou dans les update IDs a été détecté (deltas manqués). Le carnet local est **intégralement rejeté**, un nouveau snapshot est récupéré et le flux est revalidé depuis zéro. `RESYNC_OVERLAP` signale un chevauchement/retour en arrière, `MALFORMED` un événement incohérent (u < U). Comportement attendu et sans perte : le compteur `RESYNC` de l'UI s'incrémente.
- `WebSocket déconnecté … reconnexion dans X s (tentative n)` : coupure réseau ; reconnexion avec backoff exponentiel plafonné + jitter, puis resynchronisation complète. Binance coupe par ailleurs toute connexion après 24 h : cette coupure périodique est gérée par le même mécanisme.
- `échec snapshot REST … nouvel essai dans X s` : le REST a échoué pendant que le flux continue d'être bufferisé ; nouvel essai automatique.
- `erreur permanente — paire arrêtée (les autres continuent)` : le snapshot a été refusé définitivement (HTTP 400, typiquement `-1121 Invalid symbol` : paire absente du spot Binance.com). La paire passe en `✖ INDISPONIBLE` dans l'UI, les six autres continuent normalement. Corriger le symbole dans `config.yaml`, ou pointer `binance.rest_base`/`ws_base` vers un autre environnement Binance qui liste la paire.

Tous les logs sont préfixés par la paire (`sync.SOLUSDT`, `ws.ETHUSDT`, …).

Un resync occasionnel est normal ; des resyncs en rafale signalent un réseau dégradé ou une machine saturée.

## Latence

À chaque message, l'écart entre le timestamp Binance (champ `E`) et l'heure de réception locale est mesuré, loggé en DEBUG et affiché (dernière valeur, p50, p95). Attention : la mesure inclut l'écart d'horloge machine↔Binance — des valeurs négatives ou aberrantes indiquent une horloge locale non synchronisée (activer NTP).

## Capture SQLite

Activer dans `config.yaml` (`capture.enabled: true`, indépendant du mode console). **Chaque message WebSocket brut** est enregistré — pas des snapshots périodiques — pour que l'historique soit rejouable sans perte de deltas ; les snapshots REST sont également enregistrés (`ts_event_ms IS NULL`) afin que le fichier soit auto-suffisant pour un rejeu. Fichier unique `data/capture.db` partagé par toutes les paires (colonne `symbol`), sans rotation (choix assumé), table `messages(id, ts_recv_ms, ts_event_ms, symbol, payload)`. Écriture par lots dans un thread dédié (WAL), flush garanti à l'arrêt.

```sql
-- Volume capturé par minute
SELECT strftime('%Y-%m-%d %H:%M', ts_recv_ms/1000, 'unixepoch') AS minute, COUNT(*)
FROM messages GROUP BY 1 ORDER BY 1;

-- Deltas d'une paire sur un intervalle, dans l'ordre de réception (rejeu)
SELECT payload FROM messages
WHERE symbol = 'BTCUSDT' AND ts_recv_ms BETWEEN :t0 AND :t1 ORDER BY id;

-- Volume par paire
SELECT symbol, COUNT(*) FROM messages GROUP BY symbol;

-- Points de départ de rejeu (snapshots REST)
SELECT id, ts_recv_ms FROM messages WHERE ts_event_ms IS NULL ORDER BY id;
```

## Paper trading (console)

Ordres fictifs exécutés contre le carnet reconstruit — le prix moyen d'un
ordre au marché inclut le slippage réel de la quantité demandée, plus des
frais taker simulés (0,10 % par défaut, `paper.fee_bps`).

- Quatre vues : `m` marché, `g` graphique (marqueurs ▲ achat / ▼ vente,
  ligne ┄ du prix d'entrée moyen), `t` trades, `p` performance.
- Passer un ordre : `a` (achat) ou `v` (vente), puis :

  | Saisie          | Ordre                                                  |
  |-----------------|--------------------------------------------------------|
  | `0.05`          | au marché (marche le carnet, slippage réel)            |
  | `0.05@114900`   | **LIMITE** : exécuté au prix limite quand le meilleur niveau — ou une transaction réelle — traverse le prix |
  | `0.05!112000`   | **STOP** : déclenche un ordre au marché (stop-loss, take-profit, entrée en cassure) |

  `Entrée` valide, `Échap` annule ; l'équivalent USDT s'affiche en direct.
- Les ordres en attente sont listés dans la vue TRADES (`x` + numéro pour
  annuler), persistés en base, réévalués 4×/s en live et à chaque événement
  en backtest. Contrôles cash/position **au déclenchement** ; une vente
  déclenchée est plafonnée à la position restante.
- Position en lots **FIFO** : chaque vente consomme les achats les plus
  anciens ; le tableau des trades montre par lot dates d'achat et de vente,
  quantité, prix, PnL latent (lots ouverts ○) et réalisé (lots clos ●),
  **nets de frais**. Spot uniquement : pas de vente à découvert.
- L'historique survit aux redémarrages (`data/paper.db`).

## Statistiques de performance

Vue `p` : trades clôturés, win rate, profit factor, espérance, meilleur/pire
trade, durée moyenne de détention, frais cumulés, **max drawdown** et courbe
d'equity (échantillonnée chaque seconde, temps simulé en backtest). Touche
`e` : export CSV horodaté (`data/trades_YYYYMMDD_HHMMSS.csv`, lots clos +
ouverts, colonnes statut/symbole/dates/quantité/prix/PnL).

## Backtest par rejeu de capture

La capture du live devient un jeu de données de backtest : le rejeu
reconstruit les carnets avec **le même code** que le live (validation U/u
stricte, resynchronisation sur trou de séquence — en rejeu, attente du
snapshot suivant présent dans la capture) et vous laisse trader contre le
même moteur paper, à vitesse variable.

```bash
python backtest.py --db data/capture.db     # vitesse ×10 par défaut
python backtest.py --speed 0                # plein débit (sans pacing)
python backtest.py --fresh                  # portefeuille de backtest vierge
```

`Espace` pause/reprise, `+`/`-` vitesse (×1 ×2 ×5 ×10 ×25 ×100 MAX) ;
l'entête affiche `REPLAY ×10 ▶ 43%` et l'horloge **simulée**. Ordres,
historique de prix et courbe d'equity sont datés en temps simulé : résultats
reproductibles. Le portefeuille de backtest vit dans `data/backtest.db`,
séparé du live.

## Flux @trade (transactions réelles)

Chaque paire s'abonne aussi au flux `@trade` (même connexion, stream
combiné) : LAST price, VWAP 60 s, volume et part acheteuse s'affichent dans
les vues MARCHÉ et GRAPHIQUE. Les ordres **limites** se déclenchent de façon
réaliste : un trade réel imprimé à votre prix suffit, même si le meilleur
niveau du carnet ne l'a pas encore traversé.

## Multi-exchange : Kraken

```yaml
exchange: kraken
symbols: [BTC/USD, ETH/USD, SOL/USD]
kraken:
  depth: 100        # 10 / 25 / 100 / 500 / 1000
```

Deux styles d'intégration cohabitent volontairement :

- **Binance** : snapshot REST + deltas séquencés U/u (interface
  `ExchangeClient`, une pile isolée par paire) ;
- **Kraken** : snapshot et deltas sur le WebSocket v2, **une seule connexion
  pour toutes les paires**, intégrité vérifiée par un **checksum CRC32** des
  10 meilleurs niveaux à chaque mise à jour (messages parsés avec
  `parse_float=Decimal` pour préserver la précision exacte du fil).
  Checksum invalide → carnet abandonné, réabonnement, nouveau snapshot :
  même politique de tolérance zéro que le protocole Binance.

L'algorithme de checksum suit la documentation Kraken v2 mais n'a pas pu
être validé contre le flux réel (développement hors ligne). Coinbase a été
écarté : son canal L2 (Advanced Trade) exige une clé API authentifiée,
contraire au principe « aucune clé » de l'outil.

## Observabilité : Prometheus + Grafana

```yaml
observability:
  prometheus:
    enabled: true
    port: 9109
```

Le moteur expose `http://127.0.0.1:9109/metrics` (format texte Prometheus,
implémentation stdlib pure — zéro dépendance) : état, latences p50/p95,
messages/s, resyncs, déconnexions, best bid/ask, spread bp, imbalance, last
et VWAP par paire, plus cash/equity/PnL/ordres en attente du paper trading
et l'uptime. `grafana/prometheus.yml` fournit la config de scrape (5 s) ;
`grafana/dashboard.json` s'importe tel quel dans Grafana (8 panneaux :
equity, PnL, spreads, latences, débit, resyncs, imbalance, prix).

```bash
curl -s http://127.0.0.1:9109/metrics | head
```

## Version web (gratuite, sans installation)

`web/index.html` est une application **autonome** : le navigateur se
connecte directement aux API publiques Binance (WebSocket combiné
depth+trade, snapshot REST), reconstruit les carnets avec la même validation
de séquence, et offre le paper trading complet : ticket marché/limite/stop,
ordres en attente avec annulation (déclenchés par le carnet **et** par les
transactions réelles), graphique avec marqueurs, table des trades, ligne de
statistiques (win rate, profit factor, max drawdown) et export CSV. Aucun
serveur, aucune clé, aucune donnée transmise ailleurs qu'à Binance.

S'y ajoutent un **graphique en bougies temps réel avec historique** (500
dernières bougies via l'API klines publique + flux WebSocket `@kline`) sur
neuf horizons — `1m 5m 15m 30m 1h 4h D W M` (bouton `tick` pour revenir à la
ligne tick par tick) — avec volumes, EMA 9/25/50/100/200 superposées,
molette pour zoomer, OHLCV au survol, marqueurs d'exécution et ligne
d'entrée moyenne ; et une **carte de liquidité** type Bookmap : la
profondeur du carnet reconstruit (40 niveaux par côté) est échantillonnée
chaque seconde et dessinée dans le temps — intensité = quantité posée,
vert = bids, rouge = asks, ligne claire = mid.

Chaque horizon est accompagné d'un module d'**analyse & recommandation** :
badge **ACHETER/LONG · VENDRE/SHORT · NEUTRE** avec score motivé ligne par
ligne — EMA 9/25/50/100/200 (tendance et structure), RSI 14 (surachat /
survente), volume relatif (moyenne 20), imbalance du carnet sur 10 niveaux
et murs de liquidité (plus gros niveau posé ≥ 3× la médiane, projetés en
pointillés sur les bougies) — plus la **zone de liquidité** (mur bid =
support, mur ask = résistance) et des **objectifs de prix** : take profit
calé sur le mur opposé ou ±2×ATR, stop suggéré derrière le mur ou ±1,5×ATR,
ratio risque/rendement. Signaux indicatifs et pédagogiques — en aucun cas
un conseil en investissement.

Trois façons de l'utiliser, toutes gratuites :

1. **En local** : ouvrir le fichier dans un navigateur (double-clic).
2. **GitHub Pages** : pousser le dépôt → Settings → Pages → « Deploy from a
   branch » → l'application est en ligne à
   `https://<user>.github.io/<repo>/web/`.
3. **Netlify Drop** (`app.netlify.com/drop`) : glisser-déposer le fichier,
   URL publique immédiate.

L'historique (trades + ordres en attente) est conservé localement dans le
navigateur ; bouton « Réinitialiser le portefeuille » pour l'effacer.
Certains aperçus intégrés bloquent les connexions sortantes : une bannière
l'explique — utiliser le fichier téléchargé ou l'hébergement.

## Tests

```bash
python -m unittest discover -s tests -v
```

22 tests couvrent la règle de synchronisation U/u (cas normal, trou, chevauchement, événements périmés, cas limites) et l'application des deltas au carnet.

## Notes Windows

- Les séquences ANSI de l'affichage sont activées automatiquement (mode VT) sur Windows 10+.
- Le Ctrl+C est capté via le module `signal` standard : `loop.add_signal_handler` n'existe pas sur le `ProactorEventLoop`.

## Structure

```
main.py                point d'entrée (asyncio, signaux, assemblage)
lob/config.py          lecture + validation du YAML (dataclasses)
lob/exchange.py        interface abstraite ExchangeClient + types
lob/binance_client.py  implémentation Binance (REST + parsing WS)
lob/ws_client.py       WebSocket, asyncio.Queue, reconnexion backoff
lob/sequencing.py      validateur de séquence U/u (pur, testé)
lob/orderbook.py       carnet trié (sortedcontainers), deltas, requêtes
lob/sync.py            machine à états snapshot ↔ flux, resync
lob/capture.py         capture SQLite (thread dédié + queue)
lob/metrics.py         latence et débit
lob/ui.py              console : 4 vues, saisie d'ordres, mode rejeu
lob/paper.py           moteur paper : marché/limite/stop, FIFO, persistance
lob/stats.py           win rate, profit factor, drawdown, export CSV
lob/trades_feed.py     bande des transactions (@trade) : last, VWAP
lob/replay.py          rejeu de capture (backtest), vitesse variable
lob/kraken.py          feed Kraken WS v2 : checksum CRC32, multi-paires
lob/metrics_server.py  export Prometheus (stdlib pure)
lob/history.py         échantillonnage mid + equity (1 s)
lob/chart.py           graphiques ASCII (prix, equity, marqueurs)
lob/keyboard.py        clavier non bloquant multi-plateforme
lob/logging_setup.py   logging fichier/console + alertes UI
backtest.py            point d'entrée du backtest interactif
web/index.html         version web autonome (navigateur → Binance)
grafana/               prometheus.yml + dashboard.json
```

## Limites connues (assumées)

- Exécutions paper « training-grade » : un ordre limite déclenché est réputé
  entièrement exécuté à son prix (pas de file d'attente ni d'exécution
  partielle) ; un stop déclenché marche le carnet visible.
- Un seul exchange actif à la fois (`exchange:` dans la config) — la
  comparaison croisée Binance/Kraken en simultané est l'étape suivante.
- Checksum Kraken conforme à la doc v2, non validé contre le flux réel.
- Le rejeu suppose une capture Binance (snapshots REST + depth/trade).
