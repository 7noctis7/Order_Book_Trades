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
sous-graphique RSI 14 (bornes 30/70), marqueurs d'exécution, ligne d'entrée
moyenne, murs de liquidité et objectifs TP/SL projetés sur les bougies.
Les EMA suivent un **dégradé d'une seule teinte** — clair = période courte,
foncé = période longue, EMA 200 plus épaisse — contraste et séparation
daltonisme validés sur fond sombre ; le graphique reste sobre, les bougies
restent l'information principale. Un bouton « ? » regroupe l'aide des
gestes, et la recommandation s'accompagne d'une **jauge de score** visuelle
(curseur du rouge au vert).
**Navigation type TradingView** : glisser pour parcourir l'historique,
molette pour zoomer (ancré sur le curseur — le RSI et les EMA suivent la
même fenêtre), double-clic pour revenir au direct, croix de visée avec
OHLCV et prix au survol, et poignées pour redimensionner verticalement le
graphique et la carte de liquidité. Une barre de **filtres** permet
d'afficher ou masquer chaque indicateur individuellement (EMA ×5, volume,
RSI, murs, TP/SL) ; horizon, paire, filtres et hauteurs de panneaux sont
**mémorisés localement** d'une visite à l'autre.

La carte propose aussi des **vues durée façon Coinglass** — `LIVE · 1h ·
4h · J · S · M` : l'historique du carnet est enregistré localement en
colonnes agrégées (max par palier — les murs persistent) pendant que
l'app tourne, et **persiste dans le navigateur pour J/S/M** : la carte
s'enrichit au fil des sessions. Les **bougies de prix sont superposées à
la carte** (intervalle adapté à la durée : 1m → 4h), elles, disponibles
sur toute la durée dès l'ouverture. Palette **viridis** (violet →
sarcelle → jaune, fond violet profond, barre d'échelle 0 → p95) par
défaut, bascule possible vers la palette bid/ask verte/rouge. Nuance
honnête : Coinglass cartographie des liquidations estimées de contrats
perpétuels (donnée propriétaire) ; ici, c'est la **liquidité réellement
posée au carnet spot** — même lecture, donnée différente.

La **carte de liquidité** type Bookmap est agrégée en grille temps × prix :
le carnet est ramené chaque seconde à des paliers de prix sur une fenêtre
**sélectionnable — ± 0,35 % · ± 1 % · ± 2 % · ± 5 % · ± 10 %** autour du
mid (la profondeur cumulée suit la même fenêtre). Près du prix, les
données viennent du carnet temps réel ; au-delà de sa couverture, elles
sont complétées par l'**instantané REST profond de Binance (5000
niveaux/côté, rafraîchi ~30 s)** — la couverture réelle du carnet est
affichée dans l'en-tête, car au-delà de ce que Binance publie il n'existe
pas de données. Échelle au 95ᵉ percentile — les murs saturent en pleine
couleur sans écraser la liquidité ordinaire. Le plus gros mur de chaque côté est
étiqueté directement sur la carte — **ZONE LONG · support** (bids) et
**ZONE SHORT · résistance** (asks) — et le survol affiche heure, prix et
quantité posée de chaque case. Le corps de la carte est mis en cache et
n'est redessiné qu'à l'arrivée d'un échantillon : survol et
rafraîchissements restent fluides.

L'agencement suit le geste de trading : la colonne de gauche enchaîne
**carnet → ticket d'ordre → profondeur cumulée → transactions/alertes** —
et **cliquer un prix du carnet préremplit un ordre limite** à ce niveau
(le curseur passe directement à la quantité). La colonne de droite déroule
le contexte : bougies, carte de liquidité, puis la synthèse
Analyse & recommandation.

Sous la carte de liquidité, une bande **CVD de session** (delta cumulé
achats − ventes au marché, même axe temporel que la carte) montre qui
domine le flux ; le CVD entre aussi dans le score de la recommandation
(±0,5 si le delta sur 60 s dépasse 15 % du volume échangé). La
**profondeur cumulée** (courbe en escalier bids/asks sur la même fenêtre
± 0,35 %) et la **bande des transactions** (25 derniers trades colorés par
agressivité, part acheteuse et delta 60 s) complètent la lecture d'order
flow.

Deux types d'**alertes**, persistées et évaluées même onglet caché :
alertes de **prix** (saisir un prix, sens déduit, toast + notification
navigateur au franchissement) et alertes automatiques d'**apparition de
mur** — dès qu'un mur ≥ 8× le palier moyen apparaît sur n'importe quelle
paire suivie (anti-spam : détection d'apparition réelle + 3 min de silence
par paire/côté ; désactivable d'une case à cocher). Les **ordres fictifs
en attente** sont tracés sur les bougies (filtre « Ordres »), et le rendu
se met en pause quand l'onglet est caché — collecte, déclencheurs et
alertes continuent de tourner.

Le ticket est pensé pour installer les habitudes qui conditionnent la
profitabilité : champ **risque par trade** (en % de l'equity) avec bouton
**Taille auto** — la quantité est calculée à partir de la distance au stop
loss — champs **TP/SL** qui posent à l'exécution un **bracket OCO** (vente
limite + vente stop liées : la première exécutée annule l'autre, y compris
sur les achats limite/stop déclenchés plus tard), et des garde-fous dans
l'aperçu : entrée **sans stop** signalée, risque **> 2 % de l'equity**
signalé, **R/R < 1** signalé, avertissement en cas de surtrading (> 5
entrées en 30 min). Chaque position garde son stop d'entrée : la table des
trades affiche le **multiple R** de chaque lot (réalisé et latent), le CSV
l'exporte, et les statistiques ajoutent **espérance par trade, payoff
(gain moyen / perte moyenne), R moyen, série en cours et PnL par paire** —
les chiffres qui montrent où se joue réellement la profitabilité.

Chaque horizon est accompagné d'un module d'**analyse & recommandation**
volontairement compact : badge **ACHETER/LONG · VENDRE/SHORT · NEUTRE**,
phrase de synthèse avec score, et objectifs — entrée, take profit calé sur
le mur opposé ou ±2×ATR, stop suggéré derrière le mur ou ±1,5×ATR, ratio
risque/rendement. Tout le détail du calcul (RSI 14, ATR, volume relatif,
imbalance sur 10 niveaux, EMA 9/25/50/100/200, murs, justification ligne
par ligne) est replié derrière « Détails du calcul ». Un **mur** est un
palier de prix ≥ 3× le palier moyen représentant ≥ 4 % du côté scanné
(fenêtre ± 0,35 %, agrégée — les carnets réels étant remplis de
niveaux-poussière, une médiane niveau par niveau serait inutilisable), et
toujours à **distance utile du prix** (≥ 8 bp, et ≥ 0,3×ATR pour les
objectifs) : un « support » collé au meilleur bid n'en est pas un.
Signaux indicatifs et pédagogiques — en aucun cas un conseil en
investissement.

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
