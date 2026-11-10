//! DhanHQ session management: credentials, validation, the live market feed and
//! the `/api/connect`, `/api/status`, `/api/feed/*` routes the header bar uses.
//!
//! The SDK lives in the `dhan-hq` crate; this module owns the runtime session
//! (which is intentionally in-memory only) and bridges live ticks into the
//! shared [`MarketState`] quote cache.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use dhan_hq::{
    CandleRow, DhanClient, DhanError, ExchangeSegment, FeedCommand, FeedMode, FeedPacket,
    FeedSubscription, Instrument, IntradayRequest, HistoricalRequest, MarketFeed, OptionChainData,
    OptionChainRequest,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, RwLock};

use algo_core::model::Candle;

use crate::market::{quote_key, securities, MarketState};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Session {
    client: DhanClient,
    client_id: String,
}

/// Consecutive feed socket failures that trip the circuit-breaker.
const MAX_FAIL_STREAK: u32 = 5;
/// First quiet period (seconds) the circuit-breaker parks the feed for so Dhan
/// can clear a rate-limit block instead of being hammered every few seconds.
const LONG_PARK_SECS: u64 = 300;
/// Ceiling for the adaptive (doubling) park. A Dhan IP / client-id block can
/// outlast the first 5-minute park; repeatedly retrying too soon keeps poking
/// Dhan and can extend the block, so each consecutive park doubles the wait up
/// to this cap. Any successful stream resets it back to `LONG_PARK_SECS`.
const MAX_PARK_SECS: u64 = 1800;

/// Ping/pong freshness window for the operator-facing pong status. Dhan may
/// pause its pings for ~30s while data keeps flowing, so this only flips to
/// "pong not received from dhan" when the client's own liveness timeout (60s)
/// is nearly reached. Data freshness is reported separately by the watchdog.
const PONG_STATUS_SECS: f64 = 55.0;

/// Data-liveness window. Dhan's websocket can stay "connected" and answer our
/// keepalive Pong while it silently stops delivering market data; watching
/// `last_activity` alone can never see that (the Pong keeps it fresh), so the
/// feed looked connected for hours while every panel was frozen and only a
/// manual Connect fixed it. Once a socket has delivered at least one real
/// market packet (`feed_up`), this much silence from the DATA stream is treated
/// as a stalled feed and the socket is restarted automatically.
const DATA_STALL_SECS: u64 = 60;

/// True when a socket that has already streamed a real market packet
/// (`feed_up`) has delivered no data for `DATA_STALL_SECS`. Heartbeats do not
/// count: a half-open Dhan socket that keeps answering Pings must still be
/// reconnected, otherwise the app shows "pong received" with a frozen feed.
fn data_stalled(feed_up: bool, tick_age: Option<Duration>) -> bool {
    feed_up
        && tick_age
            .map(|a| a > Duration::from_secs(DATA_STALL_SECS))
            .unwrap_or(false)
}

/// Next park duration given the previous one (0 = first park).
fn next_park_secs(prev: u64) -> u64 {
    if prev == 0 {
        LONG_PARK_SECS
    } else {
        prev.saturating_mul(2).min(MAX_PARK_SECS)
    }
}

/// Wait for a just-stopped feed supervisor to finish its graceful close, but
/// never longer than a bounded window; abort it if it is wedged so the task is
/// not detached/leaked.
async fn reap_feed_task(task: tokio::task::JoinHandle<()>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !task.is_finished() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !task.is_finished() {
        task.abort();
    }
}

/// RAII wrapper that sets `feed_starting` while a stop-then-spawn restart is in
/// progress and always clears it on drop, so an early return, panic or task
/// cancellation can never leave the flag stuck `true` (which would make
/// `feed_running()` report a permanently-alive feed and suppress legitimate
/// reconnects).
struct FeedStartingGuard(Arc<AtomicBool>);

impl FeedStartingGuard {
    fn new(flag: Arc<AtomicBool>) -> Self {
        flag.store(true, Ordering::Release);
        Self(flag)
    }
}

impl Drop for FeedStartingGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Base spacing between outbound Dhan DATA calls. Dhan meters these at ~1/s per
/// user; the old 1050ms left zero headroom, so a scanner/chart/portfolio burst
/// queued up and tripped the `429 / 805` rate limit, which then reset the feed
/// and eventually blocked the client id. A deliberate 1200ms gap keeps the
/// account off the limit instead of riding it.
const DATA_GATE_MS: u64 = 1200;
/// First adaptive backoff the data gate takes after Dhan answers a call with a
/// rate limit (`429 / 805`). Doubles per consecutive hit.
const RATE_LIMIT_FIRST_PENALTY_SECS: u64 = 10;
/// Ceiling for that adaptive data-gate backoff.
const RATE_LIMIT_MAX_PENALTY_SECS: u64 = 120;
/// How long Dhan must stay quiet before the adaptive penalty is forgotten.
const RATE_LIMIT_DECAY_SECS: u64 = 120;
/// Maximum number of background candle-seed fetches allowed to be queued on the
/// Dhan data gate at once. Keeps the reservation horizon bounded so the chart
/// stays responsive even while a large watch universe is warming.
const MAX_CONCURRENT_SEEDS: usize = 12;

/// Reservation state for the single global Dhan data gate.
///
/// The gate hands out time slots ~1.2s apart without holding the mutex during
/// the wait, so an operator request can be placed in front of a long background
/// backlog. Each caller reserves `last + DATA_GATE_MS` (or the adaptive hold,
/// whichever is later) under the lock, advances `last`, and then sleeps out the
/// difference on its own.
struct GateState {
    /// When the next reserved data slot fires.
    last: Instant,
    /// Operator requests waiting. While this is non-zero, background callers
    /// refuse to extend `last` (they wait and re-check), so the background
    /// backlog can never push the operator's slot further out.
    urgent_waiters: u32,
}

impl Default for GateState {
    fn default() -> Self {
        GateState {
            last: Instant::now() - Duration::from_secs(5),
            urgent_waiters: 0,
        }
    }
}

/// RAII guard that decrements the gate's `urgent_waiters` when an operator
/// reservation ends (on success, early return, or task cancellation), so a
/// cancelled chart request can never permanently starve the background pollers.
struct UrgentWaiter {
    gate: Arc<tokio::sync::Mutex<GateState>>,
    active: bool,
}

impl Drop for UrgentWaiter {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut g) = self.gate.try_lock() {
            g.urgent_waiters = g.urgent_waiters.saturating_sub(1);
        } else if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let gate = self.gate.clone();
            handle.spawn(async move {
                let mut g = gate.lock().await;
                g.urgent_waiters = g.urgent_waiters.saturating_sub(1);
            });
        }
    }
}

/// Adaptive backoff state hung off the single global data gate. When Dhan
/// returns a rate-limit error the gate refuses to fire again until `hold_until`,
/// so a retrying scanner cannot keep the account throttled (the positive
/// feedback loop that made the feed disconnect every few minutes).
///
/// `seen_in_hold` counts the successive `429`s observed inside the CURRENT hold
/// window; the penalty only doubles when that count looks like a real continued
/// burst rather than the tail of the initial one.
#[derive(Default)]
struct RateHold {
    /// Earliest instant the data gate may fire again.
    hold_until: Option<Instant>,
    /// Current penalty duration in seconds (0 = none).
    penalty_secs: u64,
    /// When the rate limit was last seen, so a long quiet spell can clear it.
    last_limit: Option<Instant>,
    /// `429` replies absorbed inside the current hold window. Drives the
    /// once-per-window escalation below.
    seen_in_hold: u32,
    /// `429`s absorbed without any successful Dhan data call in between. A
    /// successful call proves the account recovered, so it resets the ramp
    /// instead of leaving the penalty at its ceiling for the rest of the day.
    consecutive_limits: u32,
}

#[derive(Default)]
struct FeedHealth {
    ws_running: bool,
    feed_up: bool,
    subscribed: usize,
    last_tick: Option<Instant>,
    /// Last time ANY frame arrived on the socket (a trade packet, a PrevClose/OI
    /// packet, or a websocket heartbeat). Dhan pings every ~10s, so this stays
    /// fresh for a live socket even when the market is closed and no ticks flow.
    /// The watchdog uses this, not `last_tick`, to decide a socket is stalled.
    last_activity: Option<Instant>,
    /// Last time a websocket ping/pong was seen from Dhan (our 5s keepalive's
    /// Pong, or Dhan's own ~10s Ping). This is the signal the operator reads as
    /// "pong received from Dhan": when it goes stale the socket is half-open and
    /// the feed auto-reconnects (see `PONG_TIMEOUT` in the feed client).
    last_pong: Option<Instant>,
    /// When the current feed supervisor last (re)started. Lets the watchdog tell
    /// a socket that has simply not ticked yet from one that has gone silent.
    feed_started: Option<Instant>,
    /// True while the supervisor currently holds an open, handshaken socket that
    /// has not dropped yet. Stays false during reconnect backoff, so the
    /// watchdog can avoid resetting a growing backoff.
    socket_open: bool,
    /// When the current socket was opened (for the connected-but-silent check).
    connected_at: Option<Instant>,
    /// Consecutive dropped/failed sockets since the last successful stream. When
    /// Dhan rate-limits the client id it accepts a handshake, sends a packet or
    /// two and then goes silent, so a naive reconnect flaps every few seconds and
    /// keeps the block alive. Past a small threshold the consumer parks the feed
    /// for a long quiet period instead of hammering (see `LONG_PARK_SECS`).
    fail_streak: u32,
    parked_until: Option<Instant>,
    /// Duration of the most recent park, so consecutive parks can double it
    /// (`MAX_PARK_SECS` cap). Reset to 0 once a socket streams again.
    park_secs: u64,
    /// Access token the current supervisor was started with. `start_feed` uses
    /// this to make a repeated Connect idempotent: a fresh Connect click (or a
    /// page reload re-posting saved creds) with the exact same token while the
    /// feed is already up and streaming must NOT tear the socket down and
    /// re-subscribe every instrument (that churn spiked CPU and grew the
    /// subscribe set from ~284 to ~900 for no reason).
    feed_token: Option<String>,
    ws_task: Option<tokio::task::JoinHandle<()>>,
}

/// Aborts the wrapped task when dropped. A bare `tokio::task::JoinHandle`
/// merely detaches on drop, so aborting the feed supervisor used to leave the
/// inner `MarketFeed` websocket task running - leaking a Dhan connection slot
/// until Dhan timed it out (which then made later handshakes fail and Connect
/// appear to need a second click).
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl FeedHealth {
    fn snapshot(&self) -> Value {
        let age = self
            .last_tick
            .map(|t| (t.elapsed().as_secs_f64() * 10.0).round() / 10.0);
        let parked = self
            .parked_until
            .map(|t| t.saturating_duration_since(Instant::now()).as_secs_f64())
            .unwrap_or(0.0);
        // Ping/pong freshness surfaced as an explicit operator status string.
        let pong_age = self
            .last_pong
            .map(|t| (t.elapsed().as_secs_f64() * 10.0).round() / 10.0);
        let pong_received = pong_age.map(|a| a < PONG_STATUS_SECS).unwrap_or(false);
        let pong_status = if pong_received {
            "pong received from dhan"
        } else {
            "pong not received from dhan"
        };
        json!({
            "status": "success",
            "ws_running": self.ws_running && self.ws_task.as_ref().map(|t| !t.is_finished()).unwrap_or(false),
            "feed_up": self.feed_up,
            "subscribed": self.subscribed,
            "persist": self.subscribed,
            "last_tick_age_sec": age,
            "last_activity_age_sec": self
                .last_activity
                .map(|t| (t.elapsed().as_secs_f64() * 10.0).round() / 10.0),
            "last_pong_age_sec": pong_age,
            "pong_received": pong_received,
            "pong_status": pong_status,
            "feed_started_age_sec": self
                .feed_started
                .map(|t| t.elapsed().as_secs_f64())
                .map(|v| (v * 10.0).round() / 10.0),
            "reconnect_parked_sec": (parked * 10.0).round() / 10.0,
        })
    }
}

/// Runtime DhanHQ session + feed health. Holds only an in-memory access token;
/// nothing is written to disk.
#[derive(Clone)]
pub struct DhanState {
    pub market: MarketState,
    session: Arc<RwLock<Option<Session>>>,
    health: Arc<Mutex<FeedHealth>>,
    auth_error: Arc<Mutex<Option<String>>>,
    /// Every `(security_id, exchange_segment)` the browser has asked a quote for.
    /// The daily-candle backfill walks this set to fill change / change_pct for
    /// symbols Dhan's `/marketfeed/quote` reports with `net_change == 0`
    /// (weekends, holidays and pre-open, when the day-close equals the LTP).
    watch: Arc<Mutex<Vec<(i64, String)>>>,
    /// Option strikes already promoted to Full mode, kept separate from
    /// [`Self::watch`]. A scanner underlying is registered in `watch` on Ticker
    /// mode, so using that list to dedup the option hot path would skip the
    /// Full-mode promotion for any strike whose id was also a scanner
    /// underlying (leaving its IV/OI/volume permanently missing).
    option_subs: Arc<Mutex<HashSet<(i64, String)>>>,
    /// Scanner-underlying instruments already subscribed in Ticker mode. Kept
    /// apart from [`Self::option_subs`] so a strike can still be promoted from
    /// Ticker to Full, and cleared whenever a new socket is dialled: the fresh
    /// connection carries none of the previous socket's instruments, so the
    /// scanner must re-register its universe on it.
    scanner_subs: Arc<Mutex<HashSet<(i64, String)>>>,
    /// Serialises every outbound Dhan data call (quote / candle / option chain /
    /// portfolio) and keeps them >= ~1/s apart. Dhan rate-limits these surfaces
    /// together per user (DH-904 / error 805 "too many requests"); running the
    /// surfaces in parallel trips it and risks the account being blocked, so a
    /// single global slot is deliberate. Entry latency is instead cut by caching
    /// every LTP the entry path already fetched (see the option-leg resolver)
    /// so it makes the fewest possible calls.
    ///
    /// Callers reserve a *logical future slot* under this lock and then sleep
    /// OUTSIDE it, so a long background backlog (candle seeds) can never hold the
    /// mutex and stack its 1200ms sleeps in front of an operator chart request.
    /// Operator requests (see [`DhanState::user_active`]) jump the reservation
    /// queue - they only wait for the call already in flight - which is what
    /// stops the chart / option-chain from stalling ~40s behind hundreds of
    /// queued scanner seeds.
    gate: Arc<tokio::sync::Mutex<GateState>>,
    /// Bounds how many background candle-seed REST fetches may be queued on the
    /// Dhan gate at once. Without it a whole watch universe (1000+ symbols) can
    /// park a seed per symbol and push the reservation horizon minutes out. The
    /// scanner re-requests a still-cold series on its next pass, so a small cap
    /// simply spreads the warm-up over a few ticks instead of starving the chart.
    seed_slots: Arc<tokio::sync::Semaphore>,
    /// Spacing for read-only order-book / super-order polls. Kept separate from
    /// the placement budget so reconciliation never consumes an order slot.
    order_read_gate: Arc<tokio::sync::Mutex<Instant>>,
    /// Live control channel to the running feed, so newly registered option
    /// strikes are added with a subscribe message instead of reopening the
    /// socket (Dhan allows only a handful of concurrent feeds).
    feed_tx: Arc<Mutex<Option<mpsc::UnboundedSender<FeedCommand>>>>,
    /// Serialises `spawn_feed_task`. Two triggers (the watchdog and the browser's
    /// reconnect nudge) can fire at once and run on different runtime threads;
    /// without this lock both opened a supervisor, the second overwrote the first
    /// task handle (so the first was never aborted) and the account ended up with
    /// two concurrent Dhan feeds - exactly what trips Dhan's per-account
    /// connection cap and yields "client id is blocked" 429s. Async so the
    /// stop-then-wait for the previous socket can run while serialised.
    feed_spawn_lock: Arc<tokio::sync::Mutex<()>>,
    /// True from the moment `spawn_feed_task` takes the stop-then-spawn lock
    /// until it has published the new supervisor handle. `feed_running()` reads
    /// `ws_task`, which is briefly `None` in that window (the old handle is taken
    /// and the new one is not stored yet), so without this flag the 2s API
    /// reconnect watchdog saw "no supervisor" mid-restart and opened a SECOND
    /// concurrent Dhan feed. That second socket contended the account's feed slot
    /// and reset the first, producing a reconnect loop every ~20s.
    feed_starting: Arc<AtomicBool>,
    /// Previous-session close per quote key, seeded from the REST quote API and
    /// the daily-candle backfill. Dhan's feed never sends a PrevClose packet, so
    /// the live loop reads this map to compute a real change / change_pct on
    /// every tick instead of publishing close = 0 (which used to clobber the
    /// sidebar's gain/loss column).
    closes: Arc<Mutex<HashMap<String, f64>>>,
    /// Last good candle series per `security:exchange:instrument:timeframe`.
    /// The chart, the engine scanners and the data pool all ask for the same
    /// series; caching the result coalesces those duplicate calls and, more
    /// importantly, lets a transient Dhan error (DH-906 token refresh / 429
    /// rate limit) serve the previous bars instead of blanking the chart.
    candle_cache: Arc<Mutex<HashMap<String, (Instant, Vec<Candle>)>>>,
    /// Monotonic time of the last operator-facing data request (chart candles,
    /// option chain, sidebar quotes). Every Dhan call shares one ~1/s slot, so
    /// the background pollers (account / movers / NIFTY scan / prev-close seeds)
    /// read this and skip their cycle while it is fresh - the operator's click
    /// then waits at most for the one in-flight call instead of queueing behind
    /// a wall of scanner fetches.
    last_user: Arc<Mutex<Instant>>,
    /// Adaptive backoff seen by [`DhanState::throttle`] after a `429 / 805`.
    rate_hold: Arc<Mutex<RateHold>>,
    /// Shared broker-positions snapshot. One process-wide poller owns the Dhan
    /// positions API (2s cadence + immediate event kicks), so the three live
    /// engines never issue duplicate portfolio calls.
    positions_cache: Arc<Mutex<Vec<Value>>>,
    /// Wakes the shared positions poller for an immediate refresh after an
    /// entry / exit / fill / rejection event.
    positions_kick: Arc<tokio::sync::Notify>,
    /// Throttles position-poll failure logging so an expired token does not
    /// write a warning every 2 seconds.
    positions_warn_at: Arc<Mutex<Instant>>,
}

impl DhanState {
    pub fn new(market: MarketState) -> Self {
        let st = Self {
            market,
            session: Arc::new(RwLock::new(None)),
            health: Arc::new(Mutex::new(FeedHealth::default())),
            auth_error: Arc::new(Mutex::new(None)),
            watch: Arc::new(Mutex::new(Vec::new())),
            option_subs: Arc::new(Mutex::new(HashSet::new())),
            scanner_subs: Arc::new(Mutex::new(HashSet::new())),
            gate: Arc::new(tokio::sync::Mutex::new(GateState::default())),
            seed_slots: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_SEEDS)),
            order_read_gate: Arc::new(tokio::sync::Mutex::new(
                Instant::now() - Duration::from_secs(5),
            )),
            feed_tx: Arc::new(Mutex::new(None)),
            feed_spawn_lock: Arc::new(tokio::sync::Mutex::new(())),
            feed_starting: Arc::new(AtomicBool::new(false)),
            closes: Arc::new(Mutex::new(HashMap::new())),
            candle_cache: Arc::new(Mutex::new(HashMap::new())),
            last_user: Arc::new(Mutex::new(Instant::now() - Duration::from_secs(60))),
            rate_hold: Arc::new(Mutex::new(RateHold::default())),
            positions_cache: Arc::new(Mutex::new(Vec::new())),
            positions_kick: Arc::new(tokio::sync::Notify::new()),
            positions_warn_at: Arc::new(Mutex::new(Instant::now() - Duration::from_secs(60))),
        };
        st.spawn_daily_fill();
        st.spawn_seed_prev_close();
        st
    }

    /// One process-wide positions poller shared by every live engine.
    ///
    /// Dhan meters the portfolio surfaces together with quotes / candles on the
    /// same ~1/s data slot, so three engines polling independently used to burn
    /// three slots for the same answer. This task polls every 2 seconds, yields
    /// to an operator chart, and can be woken instantly by [`Self::kick_positions`]
    /// after an order / fill / rejection. The last good snapshot is retained on
    /// errors so a transient failure can never blank the account view.
    pub fn spawn_positions_poller(self) {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(2));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                // A kick is urgent: skip the quiet-period yield so an order
                // event refreshes the book immediately. Multiple events inside
                // the 350ms debounce collapse into one poll.
                let mut urgent = false;
                tokio::select! {
                    _ = self.positions_kick.notified() => {
                        urgent = true;
                        tokio::time::sleep(Duration::from_millis(350)).await;
                    }
                    _ = tick.tick() => {}
                }
                if !self.is_connected().await {
                    continue;
                }
                if !urgent && self.user_active() {
                    continue;
                }
                self.dhan_acct_throttle().await;
                let Some(client) = self.session_client().await else { continue };
                match client.positions().await {
                    Ok(rows) => {
                        let vals: Vec<Value> = rows
                            .iter()
                            .map(|p| serde_json::to_value(p).unwrap_or(Value::Null))
                            .collect();
                        if let Ok(mut g) = self.positions_cache.lock() {
                            *g = vals;
                        }
                    }
                    Err(e) => {
                        let warn = self
                            .positions_warn_at
                            .lock()
                            .map(|mut t| {
                                if t.elapsed() >= Duration::from_secs(30) {
                                    *t = Instant::now();
                                    true
                                } else {
                                    false
                                }
                            })
                            .unwrap_or(false);
                        if warn {
                            tracing::warn!("shared positions poll failed: {e}");
                        }
                        if e.is_rate_limit() {
                            self.note_rate_limited();
                        }
                    }
                }
            }
        });
    }

    /// Ask the shared poller for an immediate positions refresh.
    pub fn kick_positions(&self) {
        self.positions_kick.notify_one();
    }

    /// Latest shared positions snapshot (cheap clone).
    pub fn positions_snapshot(&self) -> Vec<Value> {
        self.positions_cache
            .lock()
            .map(|rows| rows.clone())
            .unwrap_or_default()
    }

    /// Record the securities a `/api/quotes` poll asked for so the daily-candle
    /// backfill covers them. Deduplicated; order is preserved.
    pub fn register_watch(&self, secs: &[(i64, String)]) {
        let Ok(mut g) = self.watch.lock() else { return };
        for (sid, exch) in secs {
            if *sid <= 0 {
                continue;
            }
            let entry = (*sid, exch.to_uppercase());
            if !g.contains(&entry) {
                g.push(entry);
            }
        }
        // Bound the prev-close backfill set so a long session cannot grow it
        // without limit.
        const MAX_WATCH: usize = 8000;
        if g.len() > MAX_WATCH {
            let drop_n = g.len() - MAX_WATCH;
            g.drain(0..drop_n);
        }
    }

    /// Register option strikes with the synthetic ticker / REST watch set and,
    /// when the live feed is running, push them over the feed's control channel
    /// so they start streaming without reopening the websocket.
    pub async fn subscribe_options(&self, secs: &[(i64, String)]) {
        if secs.is_empty() {
            return;
        }
        self.market.register_extra(secs);
        self.register_watch(secs);
        let subs: Vec<FeedSubscription> = secs
            .iter()
            .filter(|(sid, _)| *sid > 0)
            .filter_map(|(sid, exch)| {
                exchange_segment(exch)
                    .map(|seg| FeedSubscription::with_mode(seg, *sid, feed_mode_for(*sid)))
            })
            .collect();
        if subs.is_empty() {
            return;
        }
        if let Ok(g) = self.feed_tx.lock() {
            if let Some(tx) = g.as_ref() {
                let _ = tx.send(FeedCommand::Subscribe(subs));
            }
        }
    }

    /// Subscribe the scanner's UNDERLYING universe in Ticker mode (LTP only).
    ///
    /// The scanners re-register their whole universe on every pass, and the
    /// universe only needs an LTP plus the prev-close backfill for its
    /// gain/loss ranking - no volume, OI or depth. Sending it as Full pulled
    /// 21-field quote packets for hundreds of underlyings on every pass, which
    /// is where most of the websocket bandwidth (and the "corrupt message"
    /// resets that churned the socket) came from. Option strikes still go
    /// through [`Self::watch_options_now`] in Full mode because IV, OI and the
    /// order pricing need them.
    pub fn watch_scanner_now(&self, secs: &[(i64, String)]) {
        if secs.is_empty() {
            return;
        }
        self.market.register_extra(secs);
        // Only feed the prev-close backfill set; the Ticker subscription has its
        // own dedup book so it survives a mode mismatch with the option path.
        {
            let Ok(mut g) = self.watch.lock() else { return };
            for (sid, exch) in secs {
                if *sid <= 0 {
                    continue;
                }
                let entry = (*sid, exch.to_uppercase());
                if !g.contains(&entry) {
                    g.push(entry);
                }
            }
            const MAX_WATCH: usize = 8000;
            if g.len() > MAX_WATCH {
                let drop_n = g.len() - MAX_WATCH;
                g.drain(0..drop_n);
            }
        }
        let mut fresh: Vec<(i64, String)> = Vec::new();
        {
            let Ok(mut g) = self.scanner_subs.lock() else { return };
            for (sid, exch) in secs {
                if *sid <= 0 {
                    continue;
                }
                let entry = (*sid, exch.to_uppercase());
                if g.insert(entry.clone()) {
                    fresh.push(entry);
                }
            }
        }
        if fresh.is_empty() {
            return;
        }
        let subs: Vec<FeedSubscription> = fresh
            .iter()
            .filter_map(|(sid, exch)| {
                exchange_segment(exch).map(|seg| FeedSubscription::with_mode(seg, *sid, FeedMode::Ticker))
            })
            .collect();
        if subs.is_empty() {
            return;
        }
        if let Ok(g) = self.feed_tx.lock() {
            if let Some(tx) = g.as_ref() {
                let _ = tx.send(FeedCommand::Subscribe(subs));
            }
        }
    }

    /// Synchronous, deduplicated "make sure these strikes are streaming" used by
    /// the entry hot path (the "+green premium" scan) so its candidate window can
    /// be quoted without awaiting a REST/feed round-trip. Returns immediately;
    /// the strikes' ticks land on the websocket on the following packets.
    pub fn watch_options_now(&self, secs: &[(i64, String)]) {
        if secs.is_empty() {
            return;
        }
        self.market.register_extra(secs);
        // Register these in the daily-candle backfill set too (the strike's prev
        // close drives its change%), but dedup the FEED subscription on the
        // dedicated Full-mode set so a scanner-underlying id still gets promoted.
        {
            let Ok(mut g) = self.watch.lock() else { return };
            for (sid, exch) in secs {
                if *sid <= 0 {
                    continue;
                }
                let entry = (*sid, exch.to_uppercase());
                if !g.contains(&entry) {
                    g.push(entry);
                }
            }
            const MAX_WATCH: usize = 8000;
            if g.len() > MAX_WATCH {
                let drop_n = g.len() - MAX_WATCH;
                g.drain(0..drop_n);
            }
        }
        let mut fresh: Vec<(i64, String)> = Vec::new();
        {
            let Ok(mut g) = self.option_subs.lock() else { return };
            for (sid, exch) in secs {
                if *sid <= 0 {
                    continue;
                }
                let entry = (*sid, exch.to_uppercase());
                if g.insert(entry.clone()) {
                    fresh.push(entry);
                }
            }
        }
        if fresh.is_empty() {
            return;
        }
        let subs: Vec<FeedSubscription> = fresh
            .iter()
            .filter_map(|(sid, exch)| {
                exchange_segment(exch)
                    .map(|seg| FeedSubscription::with_mode(seg, *sid, FeedMode::Full))
            })
            .collect();
        if subs.is_empty() {
            return;
        }
        if let Ok(g) = self.feed_tx.lock() {
            if let Some(tx) = g.as_ref() {
                let _ = tx.send(FeedCommand::Subscribe(subs));
            }
        }
    }

    /// Read and decay the adaptive hold, returning the instant the gate may next
    /// fire. Kept separate so both the throttle loop and the diagnostics read the
    /// same state without duplicating the decay rules.
    fn current_hold(&self) -> Option<Instant> {
        let mut h = self.rate_hold.lock().unwrap_or_else(|e| e.into_inner());
        // The hold window has expired: this gate is opening again, so the burst
        // counter restarts (the next `429` is a NEW burst and may escalate). The
        // penalty itself is only forgotten after a long quiet spell, so a rapid
        // re-burst keeps its larger step.
        if h.hold_until.map(|u| u <= Instant::now()).unwrap_or(false) {
            h.hold_until = None;
            h.seen_in_hold = 0;
        }
        if let Some(t) = h.last_limit {
            if t.elapsed() >= Duration::from_secs(RATE_LIMIT_DECAY_SECS) {
                h.penalty_secs = 0;
                h.hold_until = None;
                h.last_limit = None;
                h.seen_in_hold = 0;
                h.consecutive_limits = 0;
            }
        }
        h.hold_until
    }

    /// Block until Dhan's data-API budget allows another request. Dhan meters the
    /// data endpoints together (~1/s per user on this account, error 805), so a
    /// single global slot is the only safe design; the entry path avoids extra
    /// calls by caching every LTP it already fetched.
    ///
    /// The wait happens OUTSIDE the gate mutex: each caller loops, reads the
    /// earliest legal fire instant, and sleeps on its own. Previously the mutex
    /// was held across the 1200ms sleep, so a backlog of N background candle
    /// seeds stacked N serialized sleeps in front of an operator chart request -
    /// the chart (and the F&O / commodity scanners) could stall ~40s. A caller is
    /// *urgent* while the operator is active, and background callers back off
    /// whenever an urgent waiter is present.
    async fn throttle(&self) {
        self.throttle_impl(self.user_active()).await;
    }

    /// Try to reserve one of the bounded background candle-seed slots. Returns
    /// `None` when the seed budget is full, so the caller can skip this pass and
    /// retry on the next scan tick instead of parking an unbounded backlog on the
    /// Dhan gate (which is what starved the operator chart for ~40s).
    pub fn try_seed_slot(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.seed_slots.clone().try_acquire_owned().ok()
    }

    async fn throttle_impl(&self, urgent: bool) {
        // Register as an operator waiter for the whole wait so background callers
        // see the priority pressure and yield. The RAII guard decrements it on
        // every exit path (success, early return, or cancellation).
        if urgent {
            let mut g = self.gate.lock().await;
            g.urgent_waiters = g.urgent_waiters.saturating_add(1);
            drop(g);
        }
        let _guard = UrgentWaiter {
            gate: self.gate.clone(),
            active: urgent,
        };
        loop {
            let sleep_for;
            {
                let mut g = self.gate.lock().await;
                // Background callers yield while an operator request is waiting,
                // so the backlog can never delay the chart by more than one slot.
                if !urgent && g.urgent_waiters > 0 {
                    drop(g);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                let now = Instant::now();
                let mut eligible = g.last + Duration::from_millis(DATA_GATE_MS);
                if let Some(h) = self.current_hold() {
                    if h > eligible {
                        eligible = h;
                    }
                }
                if now >= eligible {
                    g.last = now;
                    return;
                }
                sleep_for = eligible - now;
            }
            tokio::time::sleep(sleep_for).await;
        }
    }

    /// Record that Dhan answered a data call with a rate limit (`429 / 805`).
    /// The shared gate then holds every outbound data call for an escalating
    /// penalty (10s, 20s, ... capped at 120s) so a retrying scanner/chart cannot
    /// keep the account throttled - the loop that used to reset the websocket and
    /// eventually park the feed for minutes.
    ///
    /// A single burst produces MANY `429` replies (one per in-flight call), so
    /// the escalation is stepped at most once per hold window and the ramp is
    /// reset by any successful data call. Without that, one burst jumped the
    /// penalty to the 120s ceiling and every later burst re-armed it, leaving the
    /// data gate effectively closed for the whole session.
    pub fn note_rate_limited(&self) {
        let mut h = self.rate_hold.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let in_current_hold = h.hold_until.map(|u| u > now).unwrap_or(false);
        h.seen_in_hold = h.seen_in_hold.saturating_add(1);
        h.consecutive_limits = h.consecutive_limits.saturating_add(1);
        h.last_limit = Some(now);
        if in_current_hold && h.seen_in_hold > 1 {
            // Tail of the same burst: keep the existing hold, never escalate.
            return;
        }
        h.penalty_secs = if h.penalty_secs == 0 {
            RATE_LIMIT_FIRST_PENALTY_SECS
        } else {
            (h.penalty_secs * 2).min(RATE_LIMIT_MAX_PENALTY_SECS)
        };
        h.hold_until = Some(now + Duration::from_secs(h.penalty_secs));
        tracing::warn!(
            "Dhan rate limit hit; pausing data calls for {}s (adaptive, {} consecutive)",
            h.penalty_secs,
            h.consecutive_limits
        );
    }

    /// A data call actually succeeded: the account is answering again, so the
    /// adaptive ramp restarts from its first step instead of staying pinned at
    /// the ceiling. Called from the successful paths of the quote / candle /
    /// option-chain calls.
    pub fn note_data_ok(&self) {
        let mut h = self.rate_hold.lock().unwrap_or_else(|e| e.into_inner());
        if h.consecutive_limits > 0 {
            h.consecutive_limits = 0;
            h.penalty_secs = 0;
            h.hold_until = None;
            h.last_limit = None;
            h.seen_in_hold = 0;
        }
    }

    /// Seconds left on the adaptive data-gate hold (0 when the gate is open).
    /// Surfaced in the diagnostics so a stalled engine can be told apart from one
    /// that is simply waiting out Dhan's penalty.
    pub fn rate_hold_remaining(&self) -> f64 {
        self.rate_hold
            .lock()
            .ok()
            .and_then(|h| {
                h.hold_until
                    .map(|u| u.saturating_duration_since(Instant::now()).as_secs_f64())
            })
            .map(|v| (v * 10.0).round() / 10.0)
            .unwrap_or(0.0)
    }

    /// True while the adaptive data-gate penalty is still counting down.
    pub fn rate_limited(&self) -> bool {
        self.rate_hold
            .lock()
            .map(|h| h.hold_until.map(|u| u > Instant::now()).unwrap_or(false))
            .unwrap_or(false)
    }

    /// Alias kept for the historical candle call sites. Dhan meters all data
    /// surfaces through the same budget, so these share the global slot.
    async fn hist_throttle(&self) {
        self.throttle().await;
    }

    /// Alias kept for the option-chain / expiry call sites (shared global slot).
    async fn oc_throttle(&self) {
        self.throttle().await;
    }

    /// Alias kept for the portfolio call sites (shared global slot).
    async fn acct_throttle(&self) {
        self.throttle().await;
    }

    /// Order-API gate: the single send-boundary reservation shared by all three
    /// live engines. Waits for a slot in the trailing one-second window instead
    /// of spacing a fixed gap, so the aggregate can never exceed the configured
    /// Dhan-safe cap (see `crate::order_rate`).
    async fn order_throttle(&self) {
        crate::order_rate::reserve_wait(crate::order_rate::Group::Real).await;
    }

    /// Read-only order polls (order book / super orders). These are not order
    /// placements, so they get their own light 250ms spacing and never consume
    /// the shared placement budget.
    async fn order_read_throttle(&self) {
        let mut last = self.order_read_gate.lock().await;
        let wait = Duration::from_millis(250);
        let elapsed = last.elapsed();
        if elapsed < wait {
            tokio::time::sleep(wait - elapsed).await;
        }
        *last = Instant::now();
    }

    async fn client(&self) -> Option<DhanClient> {
        self.session.read().await.as_ref().map(|s| s.client.clone())
    }

    async fn client_id(&self) -> Option<String> {
        self.session.read().await.as_ref().map(|s| s.client_id.clone())
    }

    /// Public session accessors for the realtime trading engine.
    pub async fn session_client(&self) -> Option<DhanClient> {
        self.client().await
    }

    pub async fn session_client_id(&self) -> Option<String> {
        self.client_id().await
    }

    pub async fn is_connected(&self) -> bool {
        self.session.read().await.is_some()
    }

    /// True while the broker websocket is actually streaming (the first packet
    /// has been seen). The realtime engine uses the false -> true edge to detect
    /// a reconnect and resync its book with the broker, so a trade whose native
    /// stop leg fired - or a manual fill - while we were blind is not missed.
    pub fn feed_up(&self) -> bool {
        self.health.lock().map(|g| g.feed_up).unwrap_or(false)
    }

    /// True only while the broker socket is streaming AND a tick landed within
    /// the last 15s. This is the single gate the sidebar refresh, the previous
    /// close backfill and the trading engine use, so cached / REST / historical
    /// values can never be acted on while the live feed is silent, closed or
    /// disconnected.
    /// Compact feed health for the diagnostics snapshot. Includes the parked /
    /// rate-limited flags so "the feed is down" can be told apart from "the app
    /// put the feed to sleep itself after repeated failures".
    pub fn feed_diagnostic(&self) -> Value {
        let base = self.health.lock().map(|g| g.snapshot()).unwrap_or(Value::Null);
        let mut out = base;
        if let Some(o) = out.as_object_mut() {
            o.insert("feedLive".into(), json!(self.feed_live()));
            o.insert("parkedSec".into(), json!(self.current_park_secs()));
            o.insert("rateLimited".into(), json!(self.rate_limited()));
            o.insert("rateHoldRemainingSec".into(), json!(self.rate_hold_remaining()));
        }
        out
    }

    pub fn feed_live(&self) -> bool {
        let Ok(g) = self.health.lock() else {
            return false;
        };
        g.feed_up
            && g.last_tick
                .map(|t| t.elapsed().as_secs_f64() < 15.0)
                .unwrap_or(false)
    }

    /// Record an operator-facing data request (chart candles / option chain /
    /// sidebar quotes) so the background pollers yield Dhan's single data slot
    /// to it for the next moment.
    pub fn mark_user_activity(&self) {
        if let Ok(mut g) = self.last_user.lock() {
            *g = Instant::now();
        }
    }

    /// True while an operator-facing data request arrived within the last
    /// ~1.8s. Background pollers return early while this holds; the 100ms engine
    /// tick re-checks, so they resume the instant the operator goes quiet.
    pub fn user_active(&self) -> bool {
        self.last_user
            .lock()
            .map(|g| g.elapsed() < Duration::from_millis(1800))
            .unwrap_or(false)
    }

    /// Serialised, rate-limited Dhan REST call gate (shared with the chart).
    /// This is the QUOTE slot (marketfeed quote / ltp) at ~1/s.
    pub async fn dhan_throttle(&self) {
        self.throttle().await;
    }

    /// Historical candle slot (~3/s) for the realtime engine's candle fetches.
    pub async fn dhan_hist_throttle(&self) {
        self.hist_throttle().await;
    }

    /// Option-chain / expiry slot (~1 call / 3s).
    pub async fn dhan_oc_throttle(&self) {
        self.oc_throttle().await;
    }

    /// Portfolio slot (~1/s) for positions / funds / holdings / margin.
    pub async fn dhan_acct_throttle(&self) {
        self.acct_throttle().await;
    }

    /// Fast order-API gate: use for place / modify / cancel / super orders so an
    /// entry never waits behind background candle fetches.
    pub async fn dhan_order_throttle(&self) {
        self.order_throttle().await;
    }

    pub async fn dhan_order_read_throttle(&self) {
        self.order_read_throttle().await;
    }

    fn set_auth_error(&self, msg: Option<String>) {
        if let Ok(mut g) = self.auth_error.lock() {
            *g = msg;
        }
    }

    // -----------------------------------------------------------------------
    // Candles (real Dhan data when connected)
    // -----------------------------------------------------------------------

    /// Fetch candles for the chart. Returns `Err` when not connected or when
    /// Dhan rejects the request, so the caller can fall back to synthetic data.
    pub async fn fetch_candles(
        &self,
        sec_id: i64,
        exch: &str,
        inst: &str,
        tf: &str,
    ) -> Result<Vec<Candle>, DhanError> {
        let key = format!(
            "{sec_id}:{}:{}:{tf}",
            exch.to_uppercase(),
            inst.to_uppercase()
        );
        // A just-fetched series serves the chart, the engine scanners and the
        // data pool without a second Dhan call. 4s window so a symbol switch
        // back and forth (and the chart's own reload) reuses the last bars
        // instead of spending another 1/s data slot.
        if let Some(fresh) = self.cached_candles(&key, Duration::from_secs(4)) {
            return Ok(fresh);
        }
        let client = self.client().await.ok_or(DhanError::NotConnected)?;
        let segment = exchange_segment(exch)
            .ok_or_else(|| DhanError::Message(format!("unknown exchange segment: {exch}")))?;
        let instrument = instrument_type(inst);
        let ist_now = now_secs() + 19800;

        // Two attempts: Dhan answers a transient DH-906 while a token refresh is
        // in flight and 429 under short bursts, so one spaced retry recovers most
        // chart loads instead of showing an empty series.
        let mut last_err: Option<DhanError> = None;
        for attempt in 0..2 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(1500)).await;
            }
            let rows: Vec<CandleRow> = match timeframe_plan(tf) {
                TfPlan::Intraday { interval, .. } => {
                    let from = ist_now - 30 * 86400;
                    let mut rows = match self
                        .intraday_rows(&client, sec_id, segment, instrument, interval, from, ist_now)
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            last_err = Some(e);
                            continue;
                        }
                    };
                    // Dhan distinguishes index options (OPTIDX) from stock options
                    // (OPTSTK); sending the wrong one yields an empty payload or a
                    // single junk bar - the old app saw exactly one "fat candle" from
                    // this. Retry the alternate type and keep whichever has more bars
                    // (port of old `fetch_intraday_candles` fallback).
                    if rows.len() <= 2 {
                        if let Some(alt) = alt_option_instrument(instrument) {
                            if let Ok(alt_rows) = self
                                .intraday_rows(&client, sec_id, segment, alt, interval, from, ist_now)
                                .await
                            {
                                if alt_rows.len() > rows.len() {
                                    rows = alt_rows;
                                }
                            }
                        }
                    }
                    rows
                }
                TfPlan::Daily => self.daily_rows(&client, sec_id, segment, instrument).await,
            };

            if rows.is_empty() {
                last_err = Some(DhanError::Invalid("Dhan returned no candles".into()));
                continue;
            }
            // Dhan timestamps are UTC epoch; shift to the IST wall clock the rest
            // of the app (and the synthetic generator) uses.
            let mut shifted: Vec<CandleRow> = rows
                .into_iter()
                .map(|mut r| {
                    r.time += 19800;
                    r
                })
                .collect();
            shifted.sort_by_key(|r| r.time);

            let step = tf_step_secs(tf);
            let out = if let TfPlan::Intraday { resample, .. } = timeframe_plan(tf) {
                match resample {
                    Some(secs) => resample_candles(&shifted, secs),
                    None => to_candles(shifted),
                }
            } else {
                resample_candles(&shifted, step)
            };
            if !out.is_empty() {
                if let Ok(mut c) = self.candle_cache.lock() {
                    c.insert(key.clone(), (Instant::now(), out.clone()));
                }
                self.note_data_ok();
                return Ok(out);
            }
            last_err = Some(DhanError::Invalid("no candles after resample".into()));
        }

        // Both attempts failed. Rather than blank the chart, serve the last good
        // series (bounded, so a long-disconnected session still surfaces an
        // error) — mirrors the old app keeping the previous grid on screen.
        if let Some(stale) = self.cached_candles(&key, Duration::from_secs(900)) {
            return Ok(stale);
        }
        Err(last_err.unwrap_or(DhanError::NotConnected))
    }

    /// One Dhan `/charts/intraday` call, throttled, returning the raw rows.
    async fn intraday_rows(
        &self,
        client: &DhanClient,
        sec_id: i64,
        segment: ExchangeSegment,
        instrument: Instrument,
        interval: u32,
        from: i64,
        to: i64,
    ) -> Result<Vec<CandleRow>, DhanError> {
        let req = IntradayRequest {
            security_id: sec_id.to_string(),
            exchange_segment: segment,
            instrument,
            interval: interval.to_string(),
            oi: false,
            from_date: fmt_datetime(from),
            to_date: fmt_datetime(to),
        };
        self.hist_throttle().await;
        Ok(client.intraday(&req).await?.rows())
    }

    /// Cached candle series for `key` when younger than `max_age`.
    fn cached_candles(&self, key: &str, max_age: Duration) -> Option<Vec<Candle>> {
        let g = self.candle_cache.lock().ok()?;
        let (at, v) = g.get(key)?;
        if !v.is_empty() && at.elapsed() <= max_age {
            Some(v.clone())
        } else {
            None
        }
    }

    // -----------------------------------------------------------------------
    // Option chain (real Dhan data when connected)
    // -----------------------------------------------------------------------

    /// Dhan's option-chain expiry list for an underlying. `Err` when not
    /// connected or rejected, so the caller can fall back to synthetic dates.
    pub async fn fetch_option_expiries(
        &self,
        sec_id: i64,
        exch: &str,
    ) -> Result<Vec<String>, DhanError> {
        let client = self.client().await.ok_or(DhanError::NotConnected)?;
        let segment = exchange_segment(exch)
            .ok_or_else(|| DhanError::Message(format!("unknown exchange segment: {exch}")))?;
        self.oc_throttle().await;
        let req = OptionChainRequest {
            underlying_scrip: sec_id,
            underlying_seg: segment,
            expiry: None,
        };
        client.option_chain_expiry_list(&req).await
    }

    /// Dhan's option chain for one expiry.
    pub async fn fetch_option_chain(
        &self,
        sec_id: i64,
        exch: &str,
        expiry: &str,
    ) -> Result<OptionChainData, DhanError> {
        let client = self.client().await.ok_or(DhanError::NotConnected)?;
        let segment = exchange_segment(exch)
            .ok_or_else(|| DhanError::Message(format!("unknown exchange segment: {exch}")))?;
        self.oc_throttle().await;
        let req = OptionChainRequest {
            underlying_scrip: sec_id,
            underlying_seg: segment,
            expiry: Some(expiry.to_string()),
        };
        client.option_chain(&req).await
    }

    // -----------------------------------------------------------------------
    // REST quotes (market feed snapshot)

    // -----------------------------------------------------------------------

    /// Fetch a REST market-quote snapshot for the requested `(security_id,
    /// exchange_segment)` pairs. Returns `None` when not connected or when Dhan
    /// rejects/returns nothing, so callers can fall back to the cached/synthetic
    /// quote surface. Unlike the websocket feed this still returns the last
    /// traded price when the market is closed.
    pub async fn fetch_rest_quotes(
        &self,
        secs: &[(i64, String)],
    ) -> Option<HashMap<String, Value>> {
        let client = self.client().await?;
        let mut groups: std::collections::BTreeMap<String, Vec<i64>> =
            std::collections::BTreeMap::new();
        for (sid, exch) in secs {
            if *sid <= 0 {
                continue;
            }
            groups.entry(exch.to_uppercase()).or_default().push(*sid);
        }
        if groups.is_empty() {
            return None;
        }
        // Dhan caps `/marketfeed/quote` at 1000 security ids per request
        // ("Requested Number of SecurityId Limit exceeded"). A loaded option
        // chain easily exceeds that, so split each segment into <=1000-id
        // batches and merge the responses instead of failing the whole call.
        const QUOTE_CHUNK: usize = 1000;
        let mut resp: dhan_hq::MarketFeedResponse = std::collections::BTreeMap::new();
        let mut any_ok = false;
        for (seg, ids) in &groups {
            for chunk in ids.chunks(QUOTE_CHUNK) {
                self.throttle().await;
                let one: dhan_hq::SegmentInstruments =
                    std::iter::once((seg.clone(), chunk.to_vec())).collect();
                match client.market_feed_quote(&one).await {
                    Ok(r) => {
                        any_ok = true;
                        self.note_data_ok();
                        for (s, m) in r {
                            resp.entry(s).or_default().extend(m);
                        }
                    }
                    Err(e) => {
                        if e.is_rate_limit() {
                            self.note_rate_limited();
                        }
                        tracing::warn!(
                            "Dhan market quote failed (segment={seg}, n={}): {e}",
                            chunk.len()
                        );
                        // A `429` means Dhan's budget is gone for the WHOLE
                        // account: every further chunk in this call would just
                        // re-arm the penalty and extend the hold. Abort the pass
                        // and let the caller retry once the gate reopens.
                        if e.is_rate_limit() {
                            return None;
                        }
                    }
                }
            }
        }
        if !any_ok {
            return None;
        }
        let now = now_secs();
        let mut out: HashMap<String, Value> = HashMap::new();
        for (sid, exch) in secs {
            if *sid <= 0 {
                continue;
            }
            let seg = exch.to_uppercase();
            let Some(q) = resp.get(&seg).and_then(|m| m.get(&sid.to_string())) else {
                continue;
            };
            let ltp = q.last_price;
            if !(ltp > 0.0) {
                continue;
            }
            // Authoritative previous close: Dhan reports `previous_close_price`
            // for many instruments; when it is missing fall back to
            // `ltp - net_change` (only while net_change is non-zero). Never use
            // `ohlc.close` - that is the day close (== ltp during the session),
            // and using it would zero out change / change_pct.
            let change = q.net_change;
            let mut close = q.previous_close_price;
            if close <= 0.0 && change != 0.0 {
                close = ltp - change;
            }
            let pct = if close != 0.0 {
                change / close * 100.0
            } else {
                0.0
            };
            let bid = q
                .depth
                .as_ref()
                .and_then(|d| d.buy.first())
                .map(|l| l.price)
                .unwrap_or(0.0);
            let ask = q
                .depth
                .as_ref()
                .and_then(|d| d.sell.first())
                .map(|l| l.price)
                .unwrap_or(0.0);
            let mut v = json!({
                "ltp": round2(ltp),
                "change": round2(change),
                "close": round2(close),
                "change_pct": round2(pct),
                "at": now,
                "live": 1,
            });
            // Carry the same Full-mode extras the websocket feed publishes, so a
            // freshly loaded option chain shows real OI / Volume / Bid-Ask at once
            // instead of a wall of zeros until the first ticks land.
            if q.volume > 0.0 {
                v["volume"] = json!(round2(q.volume));
            }
            if q.oi > 0.0 {
                v["oi"] = json!(round2(q.oi));
            }
            if bid > 0.0 {
                v["bid"] = json!(round2(bid));
            }
            if ask > 0.0 {
                v["ask"] = json!(round2(ask));
            }
            out.insert(quote_key(*sid, exch), v);
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    /// Batch-seed the shared quote cache for a freshly loaded option chain.
    ///
    /// Mirrors the old app's `_apply_live_quotes` + `_seed_chain_quotes`: a single
    /// `/marketfeed/quote` call fills LTP / change / prev-close / volume / OI /
    /// bid / ask for every CE+PE strike at once, and Black-Scholes fills IV +
    /// delta/theta/gamma/vega. It also resolves the underlying's real spot (the
    /// client never sends one) which both centres the ATM window and feeds the
    /// greeks. Crucially it writes the derived previous close into the live
    /// feed's [`Self::closes`] map, so the websocket tick path computes a real
    /// change instead of zeroing it. Without this the chain only populated via
    /// the shared 1-call/second seed loops, which is why the live columns were
    /// missing / very slow to appear.
    pub async fn seed_chain_quotes(&self, secs: Vec<(i64, String)>, under: i64, under_exch: &str) {
        if self.client().await.is_none() {
            return;
        }
        // One batched call also covers the underlying, so the spot is free.
        let mut fetch = secs.clone();
        if under > 0 && !under_exch.is_empty() {
            fetch.push((under, under_exch.to_string()));
        }
        let Some(real) = self.fetch_rest_quotes(&fetch).await else {
            return;
        };
        // First pass: publish the underlying spot before any greek is derived.
        let under_key = quote_key(under, under_exch);
        if under > 0 {
            if let Some(spot) = real
                .get(&under_key)
                .and_then(|v| v.get("ltp"))
                .and_then(|v| v.as_f64())
            {
                crate::optionchain::set_index_spot(under, spot);
            }
        }
        let mut sid_by_key: HashMap<String, i64> = HashMap::with_capacity(secs.len());
        for (sid, exch) in &secs {
            sid_by_key.insert(quote_key(*sid, exch), *sid);
        }
        let mut accepted: HashMap<String, Value> = HashMap::with_capacity(real.len());
        for (key, mut v) in real {
            if let Some(sid) = sid_by_key.get(&key).copied() {
                let ltp = v.get("ltp").and_then(|x| x.as_f64()).unwrap_or(0.0);
                if let Some(g) = crate::optionchain::opt_greeks(sid, ltp) {
                    if let Some(obj) = v.as_object_mut() {
                        for field in ["iv", "delta", "vega", "gamma", "theta"] {
                            if let Some(val) = g.get(field) {
                                obj.insert(field.to_string(), val.clone());
                            }
                        }
                    }
                }
            }
            let close = v.get("close").and_then(|c| c.as_f64()).unwrap_or(0.0);
            if close > 0.0 {
                if let Ok(mut c) = self.closes.lock() {
                    c.insert(key.clone(), close);
                }
            }
            self.market.set_quote(key.clone(), v.clone());
            accepted.insert(key, v);
        }
        if !accepted.is_empty() {
            self.market.broadcast_quotes(&accepted);
        }
    }

    // -----------------------------------------------------------------------
    // Daily-candle backfill (previous close when the quote API has no change)
    // -----------------------------------------------------------------------

    /// Last two daily closes for a security, oldest->newest (`(last, prev)`).
    ///
    /// The old app derives change_pct this way when Dhan's `/marketfeed/quote`
    /// returns `net_change == 0` (weekends, holidays, pre-open). On a weekend the
    /// last candle is the most recent session (Friday) and the second-to-last is
    /// the session before it (Thursday), so the diff is the correct display value
    /// rather than 0.
    pub async fn fetch_daily_closes(
        &self,
        sec_id: i64,
        exch: &str,
        inst: &str,
    ) -> Option<(f64, f64)> {
        let client = self.client().await?;
        let segment = exchange_segment(exch)?;
        let instrument = instrument_type(inst);
        let mut rows = self.daily_rows(&client, sec_id, segment, instrument).await;
        if rows.len() < 2 {
            return None;
        }
        rows.sort_by_key(|r| r.time);
        let last = rows[rows.len() - 1].close;
        let prev = rows[rows.len() - 2].close;
        if last > 0.0 && prev > 0.0 {
            Some((last, prev))
        } else {
            None
        }
    }

    /// Long multi-year daily history for the Auto Experiment backtester.
    ///
    /// Unlike the chart path this deliberately asks Dhan for a wide window
    /// (`years` back) in a single `/charts/historical` call, so a strategy can
    /// be simulated over ~3 years of daily bars. Timestamps are shifted to the
    /// IST wall clock the rest of the app uses and rows are sorted oldest->
    /// newest. Returns an error when the session is not connected or Dhan
    /// rejects the request - the caller reports that to the operator.
    pub async fn fetch_daily_history(
        &self,
        sec_id: i64,
        exch: &str,
        inst: &str,
        years: i64,
    ) -> Result<Vec<Candle>, String> {
        let client = self
            .client()
            .await
            .ok_or_else(|| "Dhan session not connected".to_string())?;
        let segment = exchange_segment(exch)
            .ok_or_else(|| format!("unknown exchange segment: {exch}"))?;
        let instrument = instrument_type(inst);
        let years = years.clamp(1, 8);
        let ist_now = now_secs() + 19800;
        // Dhan caps a daily request at ~5 years; a couple of days of slack on
        // each side keeps a full `years` worth of sessions.
        let span_days = years * 366 + 3;
        self.hist_throttle().await;
        let req = HistoricalRequest {
            security_id: sec_id.to_string(),
            exchange_segment: segment,
            instrument,
            expiry_code: 0,
            oi: false,
            from_date: fmt_date(ist_now - span_days * 86400),
            to_date: fmt_date(ist_now),
        };
        match client.historical(&req).await {
            Ok(d) => {
                let mut rows = d.rows();
                if rows.is_empty() {
                    return Err("Dhan returned no daily candles".into());
                }
                for r in &mut rows {
                    r.time += 19800;
                }
                rows.sort_by_key(|r| r.time);
                Ok(to_candles(rows))
            }
            Err(e) => Err(format!("daily historical failed: {e}")),
        }
    }

    /// Raw daily candles (no IST shift - the caller shifts) from Dhan's
    /// `/charts/historical`, with a resample fallback. Dhan's daily endpoint
    /// answers DH-905 for several instruments (INDIA VIX, GIFT NIFTY, SENSEX) and
    /// for index requests before the session opens; the old app covered exactly
    /// that case by resampling 15-minute intraday rows into daily bars.
    async fn daily_rows(
        &self,
        client: &DhanClient,
        sec_id: i64,
        segment: ExchangeSegment,
        instrument: Instrument,
    ) -> Vec<CandleRow> {
        let ist_now = now_secs() + 19800;
        self.hist_throttle().await;
        let req = HistoricalRequest {
            security_id: sec_id.to_string(),
            exchange_segment: segment,
            instrument,
            expiry_code: 0,
            oi: false,
            from_date: fmt_date(ist_now - 730 * 86400),
            to_date: fmt_date(ist_now),
        };
        match client.historical(&req).await {
            Ok(d) => {
                let rows = d.rows();
                if rows.len() >= 2 {
                    return rows;
                }
            }
            Err(e) => {
                tracing::debug!("daily historical unavailable sec={sec_id} exch={exch:?}: {e}", exch = segment)
            }
        }
        // Fallback: resample 15-minute intraday rows into IST-day bars.
        self.hist_throttle().await;
        let req = IntradayRequest {
            security_id: sec_id.to_string(),
            exchange_segment: segment,
            instrument,
            interval: "15".to_string(),
            oi: false,
            from_date: fmt_datetime(ist_now - 90 * 86400),
            to_date: fmt_datetime(ist_now),
        };
        let Ok(d) = client.intraday(&req).await else {
            return Vec::new();
        };
        let mut rows = d.rows();
        rows.sort_by_key(|r| r.time);
        let mut out: Vec<CandleRow> = Vec::new();
        let mut day = i64::MIN;
        for r in rows {
            let ist = r.time + 19800;
            let d = ist - ist.rem_euclid(86400);
            if d != day {
                day = d;
                out.push(CandleRow {
                    time: d - 19800,
                    open: r.open,
                    high: r.high,
                    low: r.low,
                    close: r.close,
                    volume: r.volume,
                    open_interest: r.open_interest,
                });
            } else if let Some(c) = out.last_mut() {
                c.high = c.high.max(r.high);
                c.low = c.low.min(r.low);
                c.close = r.close;
                c.volume += r.volume;
            }
        }
        out
    }

    fn spawn_daily_fill(&self) {
        let st = self.clone();
        tokio::spawn(async move { st.daily_fill_loop().await });
    }

    /// Walk the registered watch set and backfill change / change_pct from daily
    /// candles for every symbol that still has no usable close. Runs only while
    /// a session is connected and skips symbols whose cache already carries a
    /// close, so a completed pass costs no further Dhan calls until reconnect.
    async fn daily_fill_loop(&self) {
        // Let the REST quote snapshot seed the cache first (the old app waited
        // 12s) so most symbols are already filled and we never flood Dhan.
        tokio::time::sleep(Duration::from_secs(12)).await;
        loop {
            // Seed as long as a Dhan session exists - a closed market must still
            // show each symbol's previous close / daily change. A disconnected
            // (no-session) state stops this, so nothing is seeded while offline.
            if self.client().await.is_none() {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            // Yield Dhan's single data slot while the operator is interacting
            // (chart / option chain / quotes), then resume this pass.
            if self.user_active() {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            let mut list = self.watch.lock().map(|g| g.clone()).unwrap_or_default();
            // Backfill the WHOLE sidebar catalog (indices / F&O / commodities),
            // not only the ids a `/api/quotes` poll happened to register. Which
            // ids were registered depended on poll timing, so the daily-change
            // backfill used to cover the sidebar in one session and miss it in
            // the next - the exact "works, then stops" behaviour. The seed loop
            // already unions `securities()`; the daily fill now does too.
            {
                let mut seen: std::collections::HashSet<i64> = list.iter().map(|(s, _)| *s).collect();
                for (sid, exch) in securities() {
                    if seen.insert(sid) {
                        list.push((sid, exch));
                    }
                }
            }
            if list.is_empty() {
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
            // Indices and commodities first: few entries, most visible, and the
            // old app explicitly prioritised MCX so their rows did not wait for
            // the long NSE_EQ queue.
            list.sort_by_key(|(_, exch)| match exch.as_str() {
                "IDX_I" => 0,
                "MCX_COMM" => 1,
                "NSE_EQ" => 2,
                _ => 3,
            });
            for (sid, exch) in list {
                let key = quote_key(sid, &exch);
                let cached = self
                    .market
                    .quotes
                    .lock()
                    .ok()
                    .and_then(|g| g.get(&key).cloned());
                let is_synth = cached.as_ref().and_then(|v| v.get("synth")).is_some();
                let has_close = !is_synth
                    && cached
                        .as_ref()
                        .and_then(|v| v.get("close"))
                        .and_then(|c| c.as_f64())
                        .map(|c| c > 0.0)
                        .unwrap_or(false);
                if has_close {
                    continue;
                }
                // Option strikes: the daily-candle backfill must fetch them with
                // their REAL option instrument (OPTFUT / OPTSTK / OPTIDX), never
                // the futures type. A FUTSTK/FUTCOM fetch on an option id returns
                // the UNDERLYING's candles and would corrupt the option's previous
                // close. The scrip master gives us the exact instrument; the
                // option-chain seed path is preferred when it covers the strike,
                // but engine-discovered strikes (notably MCX OPTFUT commodities)
                // are NOT in that map, and skipping them entirely is exactly what
                // left their `change` / `prev_ltp` at zero so `premium_is_green`
                // never saw them as green. Resolve the instrument and backfill.
                let opt_inst = crate::scrip::get()
                    .and_then(|s| s.contract(sid).map(|c| c.instrument.to_uppercase()))
                    .filter(|i| matches!(i.as_str(), "OPTFUT" | "OPTSTK" | "OPTIDX" | "OPTCUR"));
                let inst = if let Some(i) = opt_inst {
                    i
                } else {
                    match exch.as_str() {
                        "IDX_I" => "INDEX",
                        "MCX_COMM" => "FUTCOM",
                        "NSE_FNO" | "BSE_FNO" => "FUTSTK",
                        _ => "EQUITY",
                    }
                    .to_string()
                };
                if let Some((last, prev)) = self.fetch_daily_closes(sid, &exch, &inst).await {
                    let ltp = cached
                        .as_ref()
                        .filter(|v| v.get("synth").is_none())
                        .and_then(|v| v.get("ltp"))
                        .and_then(|c| c.as_f64())
                        .filter(|v| *v > 0.0)
                        .unwrap_or(last);
                    let chg = ltp - prev;
                    let pct = if prev != 0.0 { chg / prev * 100.0 } else { 0.0 };
                    // Feed the live loop's prev-close map too, otherwise the next
                    // tick would republish close = 0 and undo this backfill.
                    if let Ok(mut c) = self.closes.lock() {
                        c.insert(key.clone(), prev);
                    }
                    let q = json!({
                        "ltp": round2(ltp),
                        "change": round2(chg),
                        "close": round2(prev),
                        "change_pct": round2(pct),
                        "at": now_secs(),
                        "live": 1,
                    });
                    self.market.set_quote(key.clone(), q.clone());
                    let mut one: HashMap<String, Value> = HashMap::new();
                    one.insert(key, q);
                    self.market.broadcast_quotes(&one);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    // -----------------------------------------------------------------------
    // Previous-close seeding (REST quote snapshot)
    // -----------------------------------------------------------------------

    fn spawn_seed_prev_close(&self) {
        let st = self.clone();
        tokio::spawn(async move { st.seed_prev_close_loop().await });
    }

    /// Refresh the shared previous-close map from the REST quote API so every
    /// live tick carries a real change / change_pct, and push the seeded rows so
    /// a quiet symbol (index / commodity) still refreshes in the sidebar. This
    /// mirrors the old app's `_seed_watchlist_prev_close`, which is why its
    /// sidebar showed live change even though the feed delivers no PrevClose
    /// packet.
    async fn seed_prev_close_loop(&self) {
        // Let the session settle before the first call.
        tokio::time::sleep(Duration::from_secs(3)).await;
        loop {
            // Refresh the previous close as long as a Dhan session exists, so a
            // closed market still shows history. Offline (no session) stops it.
            if self.client().await.is_none() {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            // Yield Dhan's single data slot while the operator is interacting
            // (chart / option chain / quotes), then resume this pass.
            if self.user_active() {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            let mut list = self.watch.lock().map(|g| g.clone()).unwrap_or_default();
            let mut seen: std::collections::HashSet<i64> = list.iter().map(|(s, _)| *s).collect();
            // Seed the whole sidebar watch set, not only the securities the
            // browser has explicitly asked about, so the first pass covers it.
            for (sid, exch) in securities() {
                if seen.insert(sid) {
                    list.push((sid, exch));
                }
            }
            // Only ask for symbols that still lack a previous close. A prev-close
            // does not change during the session, so re-requesting the whole set
            // every cycle only burns Dhan's ~1 req/s budget and trips 429s that
            // then starve the chart's candle fetch.
            list.retain(|(sid, exch)| {
                let key = quote_key(*sid, exch);
                let cached = self
                    .market
                    .quotes
                    .lock()
                    .ok()
                    .and_then(|g| g.get(&key).cloned());
                let is_synth = cached.as_ref().and_then(|v| v.get("synth")).is_some();
                let has_close = !is_synth
                    && cached
                        .as_ref()
                        .and_then(|v| v.get("close"))
                        .and_then(|c| c.as_f64())
                        .map(|c| c > 0.0)
                        .unwrap_or(false);
                !has_close
            });
            if !list.is_empty() {
                if let Some(real) = self.fetch_rest_quotes(&list).await {
                    let mut accepted: HashMap<String, Value> = HashMap::new();
                    for (k, v) in &real {
                        let close = v.get("close").and_then(|c| c.as_f64()).unwrap_or(0.0);
                        if close > 0.0 {
                            if let Ok(mut c) = self.closes.lock() {
                                c.insert(k.clone(), close);
                            }
                        }
                    }
                    for (k, v) in &real {
                        // A row with no derived close must never overwrite a good
                        // value already seeded from the daily-candle backfill.
                        let close = v.get("close").and_then(|c| c.as_f64()).unwrap_or(0.0);
                        if close <= 0.0 {
                            let existing_close = self
                                .market
                                .quotes
                                .lock()
                                .ok()
                                .and_then(|g| {
                                    g.get(k)
                                        .and_then(|e| e.get("close"))
                                        .and_then(|c| c.as_f64())
                                })
                                .unwrap_or(0.0);
                            if existing_close > 0.0 {
                                continue;
                            }
                        }
                        self.market.set_quote(k.clone(), v.clone());
                        accepted.insert(k.clone(), v.clone());
                    }
                    if !accepted.is_empty() {
                        self.market.broadcast_quotes(&accepted);
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(120)).await;
        }
    }

    // -----------------------------------------------------------------------
    // Live feed lifecycle
    // -----------------------------------------------------------------------

    async fn start_feed(&self, creds: Session) {
        // Honour an active cooldown: /api/feed/reset parks the feed so Dhan can
        // release its concurrent-connection slots. Without this, every Connect
        // click during the cooldown would reopen the socket and re-trip the
        // limit, so the limit would never clear. /api/feed/restart clears the
        // park explicitly when the user (or the UI) wants to retry.
        let parked = self
            .health
            .lock()
            .ok()
            .and_then(|g| g.parked_until)
            .filter(|until| *until > Instant::now());
        if let Some(until) = parked {
            // Stop the current socket, then schedule one automatic retry for
            // when the cooldown ends, so a manual Connect during the park is not
            // left stranded. Calls spawn_feed_task (not start_feed) to avoid an
            // infinitely recursive async type.
            self.stop_feed();
            let st = self.clone();
            let wait = until.saturating_duration_since(Instant::now());
            tokio::spawn(async move {
                tokio::time::sleep(wait).await;
                if st.feed_running() {
                    return;
                }
                if let Some(s) = st.session.read().await.clone() {
                    if let Ok(mut g) = st.health.lock() {
                        g.parked_until = None;
                    }
                    st.spawn_feed_task(s).await;
                }
            });
            return;
        }

        // No park: make a repeated Connect idempotent. A page reload (or the
        // frontend re-posting saved creds) can call /api/connect again with the
        // same token while a perfectly healthy feed is already streaming. Tearing
        // that socket down and re-subscribing every instrument just to reopen the
        // same feed spiked CPU and grew the subscribe set (~284 -> ~900). If the
        // token matches and the feed is alive with a fresh heartbeat, keep it.
        let same_token = {
            let g = self.health.lock().unwrap_or_else(|e| e.into_inner());
            g.feed_token.as_deref() == Some(creds.client.access_token())
        };
        let fresh = self
            .health
            .lock()
            .ok()
            .and_then(|g| g.last_activity)
            .map(|t| t.elapsed() < Duration::from_secs(15))
            .unwrap_or(false);
        if same_token && self.feed_running() && fresh {
            tracing::info!("connect: same live feed already running, skipping restart");
            return;
        }

        // Let `spawn_feed_task` stop the old socket gracefully and wait for it to
        // release Dhan's slot before opening the next one.
        self.spawn_feed_task(creds).await;
    }

    async fn spawn_feed_task(&self, creds: Session) {
        // Serialise the whole stop-then-spawn so two concurrent callers cannot
        // each leave a live supervisor behind (see `feed_spawn_lock`).
        let _spawn_guard = self.feed_spawn_lock.lock().await;
        // Mark the restart in progress so `feed_running()` reports "alive" while
        // the old handle is taken and the new one is not stored yet. Otherwise
        // the 2s API reconnect watchdog races this window and spawns a second
        // concurrent feed. Guard clears the flag on drop (early return / panic /
        // cancellation), and it is also cleared explicitly once the new handle
        // is published so a healthy restart is immediately visible as "down"-
        // free without waiting for scope exit.
        let starting = FeedStartingGuard::new(self.feed_starting.clone());
        // Single-flight: never let two supervisors run at once. Each one holds a
        // Dhan websocket slot, and Dhan caps concurrent feeds per account. Stop
        // the old socket *gracefully* and WAIT for it to exit before opening the
        // next one: opening a new socket while Dhan still holds the old slot made
        // Dhan accept the handshake and then reset the new connection, which is
        // what produced the endless reconnect + park loop.
        if let Some(prev) = self.take_feed_task() {
            reap_feed_task(prev).await;
        }
        // The new socket starts with only the bounded initial set, so both
        // dedup books must forget the previous socket's instruments; otherwise
        // the scanner and the option hot path would believe their strikes are
        // still streaming and never re-subscribe them on the fresh connection.
        if let Ok(mut g) = self.scanner_subs.lock() {
            g.clear();
        }
        if let Ok(mut g) = self.option_subs.lock() {
            g.clear();
        }
        // Bound the initial subscribe set. A reconnect used to replay the whole
        // accumulated option-strike set (it grew to ~4k over a session), which
        // Dhan resets as an oversized/abusive subscribe burst. Start with the
        // base catalog plus only the most recent strikes; the scanner and the
        // option-chain tab re-register the rest over the live command channel as
        // they are needed (same path the healthy first connect always used).
        const MAX_INITIAL_EXTRA: usize = 600;
        let mut subs = feed_subscriptions();
        let mut extras = self.market.extra_secs();
        if extras.len() > MAX_INITIAL_EXTRA {
            extras.drain(0..extras.len() - MAX_INITIAL_EXTRA);
        }
        // Option strikes registered by the option-chain tab ride along with the
        // static catalog so they get real websocket ticks too. Only actual
        // options take Full mode; scanner/underlying ids ride Ticker.
        for (sid, exch) in extras {
            if let Some(seg) = exchange_segment(&exch) {
                if !subs.iter().any(|s| s.security_id == sid.to_string()) {
                    subs.push(FeedSubscription::with_mode(seg, sid, feed_mode_for(sid)));
                }
            }
        }
        let market = self.market.clone();
        let health = self.health.clone();
        let closes = self.closes.clone();
        let client_id = creds.client_id.clone();
        let token = creds.client.access_token().to_string();
        let token_for_health = token.clone();

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<FeedCommand>();
        if let Ok(mut g) = self.feed_tx.lock() {
            *g = Some(cmd_tx);
        }

        let task = tokio::spawn(async move {
            run_feed(client_id, token, subs, market, health, closes, cmd_rx).await;
        });
        if let Ok(mut g) = self.health.lock() {
            g.ws_task = Some(task);
            g.feed_token = Some(token_for_health);
        }
        drop(starting);
    }

    /// Tear down the feed supervisor *without* aborting it, so it can send Dhan a
    /// close frame (RequestCode 12) and release its connection slot immediately.
    /// Aborting skipped that close; Dhan then held the slot and reset the next
    /// socket that came up on the same client id. A delayed abort still fires if
    /// the supervisor somehow fails to exit, so the task never leaks.
    fn stop_feed(&self) {
        if let Some(t) = self.take_feed_task() {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(4)).await;
                if !t.is_finished() {
                    t.abort();
                }
            });
        }
    }

    /// Drop the control channel (which signals the socket to close politely),
    /// take the supervisor handle and reset the health flags. Returns the handle
    /// so the caller can wait for the close before opening the next socket.
    fn take_feed_task(&self) -> Option<tokio::task::JoinHandle<()>> {
        if let Ok(mut g) = self.feed_tx.lock() {
            *g = None;
        }
        let mut g = self.health.lock().unwrap_or_else(|e| e.into_inner());
        g.ws_running = false;
        g.feed_up = false;
        g.feed_started = None;
        g.socket_open = false;
        g.connected_at = None;
        // Drop the heartbeat clock with the socket, so a just-stopped feed
        // cannot read "live" off a stale Pong while it is actually down.
        g.last_activity = None;
        g.feed_token = None;
        g.ws_task.take()
    }

    /// True while a feed supervisor task is alive (including reconnect backoff),
    /// or a stop-then-spawn restart is in-flight. The latter matters because
    /// `ws_task` is briefly `None` while the old supervisor is reaped and the new
    /// one is not stored yet; treating that window as "down" let the reconnect
    /// watchdog start a second concurrent feed.
    fn feed_running(&self) -> bool {
        if self.feed_starting.load(Ordering::Acquire) {
            return true;
        }
        if let Ok(g) = self.health.lock() {
            return g.ws_task.as_ref().map(|t| !t.is_finished()).unwrap_or(false);
        }
        false
    }

    /// True while a `/api/feed/reset` cooldown (or a circuit-breaker park) is
    /// still counting down. Public so the scanners can refuse to REST-hammer
    /// Dhan while the account is meant to be quiet.
    pub fn park_active(&self) -> bool {
        if let Ok(g) = self.health.lock() {
            return g
                .parked_until
                .map(|until| until > Instant::now())
                .unwrap_or(false);
        }
        false
    }

    /// Counts one feed failure and trips the circuit-breaker once too many
    /// happen in a row. Returns true when it parked the feed, so the caller must
    /// NOT respawn immediately - the watchdog re-spawns only after the park.
    /// The streak is reset when it trips so the next round after the cool-down
    /// gets a fresh budget (otherwise the watchdog would re-park instantly and
    /// the feed would never be retried again).
    fn note_feed_failure(&self) -> bool {
        if let Ok(mut g) = self.health.lock() {
            g.fail_streak = g.fail_streak.saturating_add(1);
            if g.fail_streak >= MAX_FAIL_STREAK {
                let secs = next_park_secs(g.park_secs);
                g.park_secs = secs;
                g.parked_until = Some(Instant::now() + Duration::from_secs(secs));
                g.fail_streak = 0;
                return true;
            }
        }
        false
    }

    /// Clears the failure streak after the watchdog observes a genuinely
    /// streaming socket (so an occasional drop on a healthy feed never trips it).
    fn clear_feed_streak(&self) {
        if let Ok(mut g) = self.health.lock() {
            g.fail_streak = 0;
            // A healthy stream resets the adaptive park back to the minimum.
            g.park_secs = 0;
        }
    }

    /// The duration of the most recent park (for accurate log lines).
    fn current_park_secs(&self) -> u64 {
        self.health
            .lock()
            .map(|g| g.park_secs)
            .unwrap_or(LONG_PARK_SECS)
    }

    /// Background watchdog: if a session exists but no supervisor is alive
    /// (because the socket died in a way the inner task could not recover from,
    /// or the process restarted while a token was still valid), bring the feed
    /// back automatically. It also restarts a supervisor that is *alive but not
    /// streaming* - Dhan sometimes accepts the handshake and then never delivers
    /// a packet (connection-slot limit, silent rejection), which used to leave
    /// every panel blank until the user pressed Connect again. This is what makes
    /// the app stream from a single Connect without depending on the browser tab.
    /// Dhan's live feed reports a frozen price for the GIFT NIFTY index
    /// (IDX_I / security 5024): the candle series is correct and fresh, but the
    /// ticker stays parked near the previous session's level, which made the
    /// sidebar's daily change wrong while the chart was right. Own the key so no
    /// stale tick/REST snapshot can publish it, then mirror the latest candle
    /// close (and the seeded previous close) into the live quote on a timer.
    pub fn spawn_gift_nifty_fix(&self) {
        const KEY: &str = "IDX_I:5024";
        self.market.lock_key(KEY);
        let st = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(15));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if !st.is_connected().await {
                    continue;
                }
                let Some(ltp) = st
                    .fetch_candles(5024, "IDX_I", "INDEX", "1min")
                    .await
                    .ok()
                    .and_then(|c| c.last().map(|x| x.close))
                    .filter(|v| *v > 0.0)
                else {
                    continue;
                };
                // The frozen feed also poisons the *seeded* previous close (it
                // reports prev == ltp), so take the prior session's close straight
                // from the daily series; fall back to the seed only if the daily
                // call fails.
                let prev = st
                    .fetch_candles(5024, "IDX_I", "INDEX", "1day")
                    .await
                    .ok()
                    .filter(|c| c.len() >= 2)
                    .map(|c| c[c.len() - 2].close)
                    .filter(|v| *v > 0.0)
                    .or_else(|| {
                        st.closes
                            .lock()
                            .ok()
                            .and_then(|g| g.get(KEY).copied())
                            .filter(|v| *v > 0.0)
                    })
                    .unwrap_or(0.0);
                let q = feed_quote(ltp, prev, now_secs() + 19800, 0.0, 0.0, 0.0, 0.0);
                let mut batch = HashMap::new();
                batch.insert(KEY.to_string(), q.clone());
                st.market.set_quote_owned(KEY.to_string(), q);
                st.market.broadcast_quotes(&batch);
            }
        });
    }

    pub fn spawn_watchdog(&self) {
        let st = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut beats: u32 = 0;
            loop {
                tick.tick().await;
                // ~60s feed-health heartbeat so a client-side "it just stopped"
                // report has a server-side timeline to line up against.
                beats = beats.wrapping_add(1);
                if beats % 6 == 1 {
                    let (up, act, tick_age, park) = st
                        .health
                        .lock()
                        .map(|g| {
                            (
                                g.feed_up,
                                g.last_activity
                                    .map(|t| t.elapsed().as_secs())
                                    .unwrap_or(u64::MAX),
                                g.last_tick
                                    .map(|t| t.elapsed().as_secs())
                                    .unwrap_or(u64::MAX),
                                g.parked_until
                                    .map(|u| u.saturating_duration_since(Instant::now()).as_secs())
                                    .unwrap_or(0),
                            )
                        })
                        .unwrap_or((false, u64::MAX, u64::MAX, 0));
                    tracing::info!(
                        "feed hb: running={} up={} act_age={}s tick_age={}s park={}s open={}",
                        st.feed_running(),
                        up,
                        act,
                        tick_age,
                        park,
                        market_open_now()
                    );
                }
                if st.park_active() {
                    continue;
                }
                let Some(session) = st.session.read().await.clone() else {
                    continue;
                };
                if !st.feed_running() {
                    if st.note_feed_failure() {
                        tracing::warn!(
                            "feed circuit-breaker: supervisor gone repeatedly, parking {}s",
                            st.current_park_secs()
                        );
                        continue;
                    }
                    if let Ok(mut g) = st.health.lock() {
                        g.parked_until = None;
                    }
                    tracing::warn!("feed watchdog: supervisor not running, restarting");
                    st.spawn_feed_task(session).await;
                    continue;
                }
                // Alive but silent: give a fresh socket 30s to produce its first
                // packet, and restart one that ticked and then went quiet for
                // 25s. Only during exchange hours, otherwise a closed market
                // (which legitimately delivers nothing) would churn connections.
                if !market_open_now() {
                    continue;
                }
                let (silent, data_stale, healthy) = st
                    .health
                    .lock()
                    .map(|g| {
                        let fresh = g.feed_up
                            && g.last_tick
                                .map(|t| t.elapsed() < Duration::from_secs(25))
                                .unwrap_or(false);
                        // A socket that has streamed but then received NOTHING at
                        // all for a while is a stall. Crucially this watches any
                        // frame (heartbeat / PrevClose / OI), not just trade ticks:
                        // on a closed holiday the market is quiet yet Dhan keeps
                        // pinging every ~10s, so a live socket never looks stalled.
                        let silent = g.feed_up
                            && g.last_activity
                                .map(|t| t.elapsed() > Duration::from_secs(45))
                                .unwrap_or(false);
                        // Half-open socket: heartbeats/Pongs keep `last_activity`
                        // fresh, yet the DATA stream stopped. This is the daily
                        // freeze the operator could only fix with a manual
                        // Connect, so a data stall must reconnect on its own.
                        let data_stale = data_stalled(g.feed_up, g.last_tick.map(|t| t.elapsed()));
                        (silent, data_stale, fresh)
                    })
                    .unwrap_or((false, false, false));
                if healthy {
                    st.clear_feed_streak();
                }
                if silent || data_stale {
                    let reason = if data_stale {
                        "socket heartbeats alive but market data stale"
                    } else {
                        "socket alive but silent"
                    };
                    if st.note_feed_failure() {
                        tracing::warn!(
                            "feed circuit-breaker: repeated stalled sockets ({}), parking {}s",
                            reason,
                            st.current_park_secs()
                        );
                        st.stop_feed();
                        continue;
                    }
                    tracing::warn!("feed watchdog: {reason}, restarting");
                    st.spawn_feed_task(session).await;
                }
            }
        });
    }

    /// Automatic Dhan REST ("API") reconnect. The market *feed* healed itself on
    /// its own, but the Dhan *session* was only ever (re)validated by the Connect
    /// button, so a dropped or silently-rotated session kept the app "connected"
    /// in the UI while every REST call (chart, option chain, orders) failed until
    /// the user pressed Connect again.
    ///
    /// This watchdog re-runs the exact `profile()` authentication that Connect
    /// uses, but ONLY when no feed supervisor is alive (a live supervisor heals
    /// itself with its own capped backoff). Calls are backed off (5s doubling to
    /// 60s) and a 429 / rate-limit reply parks the feed for `LONG_PARK_SECS`
    /// instead of retrying - hammering `/profile` while Dhan is rate-limiting is
    /// what kept the IP / client id blocked. It also honours the
    /// `/api/feed/reset` park cooldown.
    ///
    /// Note: an *expired daily token* still needs a fresh paste - the app holds no
    /// app-secret/TOTP, so it cannot mint a new token on its own.
    pub fn spawn_api_reconnect(&self) {
        let st = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(2));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut last_err: Option<String> = None;
            let mut next_try = Instant::now();
            let mut backoff = 5u64;
            loop {
                tick.tick().await;
                if st.park_active() || !market_open_now() {
                    continue;
                }
                // Never add REST pressure while a feed supervisor is alive: it
                // owns reconnection with its own capped backoff. The old code
                // called `/profile` every 2s here even during a feed outage,
                // which is exactly the flood that made Dhan answer "too many
                // requests from this IP hence client id is blocked".
                if st.feed_running() {
                    continue;
                }
                if Instant::now() < next_try {
                    continue;
                }
                let Some(session) = st.session.read().await.clone() else {
                    continue;
                };
                // Already alive: any frame (trade tick OR heartbeat) within 15s
                // means there is nothing to reconnect - a quiet market must not
                // launch a reconnect storm.
                let live = st
                    .health
                    .lock()
                    .map(|g| {
                        g.last_activity
                            .map(|t| t.elapsed() < Duration::from_secs(15))
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);
                if live {
                    backoff = 5;
                    next_try = Instant::now();
                    continue;
                }
                match session.client.profile().await {
                    Ok(_) => {
                        st.set_auth_error(None);
                        last_err = None;
                        backoff = 5;
                        next_try = Instant::now();
                        tracing::warn!("api auto-reconnect: session valid, restarting feed");
                        st.spawn_feed_task(session).await;
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        let rate_limited = e.is_rate_limit()
                            || msg.contains("429")
                            || msg.to_ascii_lowercase().contains("too many");
                        if rate_limited {
                            // A Dhan IP / client-id block needs a long quiet
                            // window; park so neither this loop nor the feed
                            // watchdog keeps poking it (each poke can extend
                            // the block). Consecutive parks double up to
                            // MAX_PARK_SECS. The frontend honours this cooldown.
                            let secs = {
                                let mut g = st.health.lock().unwrap_or_else(|e| e.into_inner());
                                let secs = next_park_secs(g.park_secs);
                                g.park_secs = secs;
                                g.parked_until =
                                    Some(Instant::now() + Duration::from_secs(secs));
                                secs
                            };
                            if last_err.as_deref() != Some(msg.as_str()) {
                                tracing::warn!(
                                    "api auto-reconnect rate-limited; parking {}s: {msg}",
                                    secs
                                );
                                last_err = Some(msg.clone());
                            }
                        } else if last_err.as_deref() != Some(msg.as_str()) {
                            tracing::warn!("api auto-reconnect failed: {msg}");
                            last_err = Some(msg.clone());
                        }
                        st.set_auth_error(Some(msg));
                        next_try = Instant::now() + Duration::from_secs(backoff);
                        backoff = (backoff * 2).min(60);
                    }
                }
            }
        });
    }
}

/// True during the Indian exchange session (IST 09:15-23:30, Mon-Fri), which
/// covers both the cash/F&O day and the MCX evening session. Used to gate the
/// silent-feed watchdog so a closed market does not cause reconnect churn.
fn market_open_now() -> bool {
    let ist = now_secs() + 19_800; // UTC -> IST
    let days = ist.div_euclid(86_400);
    let weekday = (days + 4).rem_euclid(7); // 1970-01-01 was a Thursday
    if weekday == 0 || weekday == 6 {
        return false;
    }
    let secs = ist.rem_euclid(86_400);
    secs >= 9 * 3600 + 15 * 60 && secs <= 23 * 3600 + 30 * 60
}

/// Derived connection link state for the header/popups and the disconnect
/// monitor. Pure (no state) so it can be unit-tested exhaustively:
/// - `offline`    no Dhan session at all -> needs Client ID + Token
/// - `closed`     session exists but the exchange is closed -> idle, not an error
/// - `live`       socket streaming: a frame (trade tick OR heartbeat) landed
///                within the last 15s. Heartbeats deliberately count, so a quiet
///                market with no trade ticks is never reported as disconnected
///                (that false "down" is what triggered the auto-reset disconnect).
/// - `connecting` socket is up but has not delivered its first packet yet
/// - `down`       connected + market open, but nothing has arrived for >15s
fn link_state(
    connected: bool,
    market_open: bool,
    feed_up: bool,
    last_frame_age_sec: Option<f64>,
    ws_running: bool,
    feed_started_age_sec: Option<f64>,
) -> &'static str {
    if !connected {
        return "offline";
    }
    if !market_open {
        return "closed";
    }
    // Liveness is "any frame in the last 15s", not "a trade tick in the last
    // 15s": our 5s keepalive ping and Dhan's own heartbeats refresh this even
    // when the exchange sends nothing (off-hours / illiquid strikes / holiday).
    let fresh = last_frame_age_sec.map(|a| a < 15.0).unwrap_or(false);
    if fresh {
        return "live";
    }
    let still_handshaking = !feed_up
        && ws_running
        && feed_started_age_sec.map(|a| a < 30.0).unwrap_or(false);
    if still_handshaking {
        return "connecting";
    }
    "down"
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct ConnectReq {
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub access_token: String,
}

pub async fn connect(
    State(st): State<DhanState>,
    Json(req): Json<ConnectReq>,
) -> (StatusCode, Json<Value>) {
    let client_id = req.client_id.trim();
    let access_token = req.access_token.trim();
    if client_id.is_empty() || access_token.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"status":"error","message":"Client ID and Access Token required"})),
        );
    }

    let client = DhanClient::new(client_id, access_token);
    match client.profile().await {
        Ok(profile) => {
            st.set_auth_error(None);
            // A fresh login is a clean slate: clear any accumulated failure streak
            // so the circuit-breaker does not immediately re-park the new session.
            if let Ok(mut g) = st.health.lock() {
                g.fail_streak = 0;
            }
            let id = if profile.dhan_client_id.is_empty() {
                client_id.to_string()
            } else {
                profile.dhan_client_id.clone()
            };
            // Mirror the header login into the shared Broker Settings store.
            // The multi-broker router and the Dhan order adapter read Dhan
            // credentials from there, so without this a Dhan entry routed
            // through the adapter would fail even though the header shows
            // "connected". The Broker Settings card for Dhan is display-only
            // (its inputs live in the header), so this is the only writer.
            {
                let mut fields = serde_json::Map::new();
                fields.insert("clientId".to_string(), json!(id.clone()));
                fields.insert("accessToken".to_string(), json!(access_token));
                if let Err(e) = crate::brokers::update_account("dhan", None, Some(&fields)) {
                    tracing::warn!("could not mirror the Dhan header login into Broker Settings: {e}");
                }
            }
            *st.session.write().await = Some(Session {
                client: client.clone(),
                client_id: id.clone(),
            });
            let creds = Session {
                client,
                client_id: id.clone(),
            };
            st.start_feed(creds).await;
            // Prime the shared positions cache immediately instead of waiting
            // for the next 2s tick, so a fresh login shows the book at once.
            st.kick_positions();
            (
                StatusCode::OK,
                Json(json!({"status":"success","message":"Connected successfully","client_id":id})),
            )
        }
        Err(e) => {
            st.set_auth_error(Some(e.to_string()));
            // 4xx (not 5xx): Cloudflare in front of the preview tunnel rewrites
            // origin 5xx responses with its own opaque "error code: 502" page,
            // which hides the real Dhan message from the UI.
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"status":"error","message":e.to_string(),"auth_error":e.to_string()})),
            )
        }
    }
}

pub async fn status(State(st): State<DhanState>) -> impl IntoResponse {
    let connected = st.session.read().await.is_some();
    let client_id = st.client_id().await.unwrap_or_default();
    let auth_error = st.auth_error.lock().ok().and_then(|g| g.clone());
    Json(json!({
        "connected": connected,
        "client_id": client_id,
        "auth_error": auth_error,
    }))
}

pub async fn feed_status(State(st): State<DhanState>) -> impl IntoResponse {
    let connected = st.session.read().await.is_some();
    let auth_error = st.auth_error.lock().ok().and_then(|g| g.clone());
    let market_open = market_open_now();
    let mut snap = st.health.lock().map(|g| g.snapshot()).unwrap_or(Value::Null);
    // Derive the single link state the UI reacts to (popups + auto reconnect).
    let (feed_up, ws_running, age, started) = {
        let o = snap.as_object();
        let tick_age = o.and_then(|o| o.get("last_tick_age_sec")).and_then(|v| v.as_f64());
        let activity_age = o
            .and_then(|o| o.get("last_activity_age_sec"))
            .and_then(|v| v.as_f64());
        // Liveness for the UI: a socket kept alive by Dhan's heartbeats (every
        // ~10-15s) counts as streaming even when the market is quiet and trade
        // ticks are sparse (holiday / off-hours), so the header does not flicker
        // to a false "down" while the connection is actually healthy.
        let age = match (tick_age, activity_age) {
            (Some(t), Some(a)) => Some(t.min(a)),
            (t, a) => t.or(a),
        };
        (
            o.and_then(|o| o.get("feed_up")).and_then(|v| v.as_bool()).unwrap_or(false),
            o.and_then(|o| o.get("ws_running")).and_then(|v| v.as_bool()).unwrap_or(false),
            age,
            o.and_then(|o| o.get("feed_started_age_sec")).and_then(|v| v.as_f64()),
        )
    };
    let link = link_state(connected, market_open, feed_up, age, ws_running, started);
    if let Some(o) = snap.as_object_mut() {
        o.insert("connected".into(), json!(connected));
        o.insert("market_open".into(), json!(market_open));
        o.insert("link".into(), json!(link));
        o.insert("auth_error".into(), json!(auth_error));
    }
    Json(snap)
}

pub async fn feed_reset(State(st): State<DhanState>) -> impl IntoResponse {
    const COOLDOWN: u64 = 90;
    st.stop_feed();
    if let Ok(mut g) = st.health.lock() {
        g.last_tick = None;
        // Clear the heartbeat clock too: otherwise a socket we just tore down
        // would keep its last Pong fresh for up to 15s and briefly read "live".
        g.last_activity = None;
        // And the pong clock, so the status flips to "not received" immediately
        // instead of showing a stale "pong received".
        g.last_pong = None;
        // Never shorten an already-running (circuit-breaker) park: the browser's
        // automatic reset repeatedly calls this, and shortening it would defeat
        // the long quiet period Dhan needs to clear the rate limit.
        let until = Instant::now() + Duration::from_secs(COOLDOWN);
        if !g.parked_until.map(|u| u > until).unwrap_or(false) {
            g.parked_until = Some(until);
        }
    }
    Json(json!({
        "status": "success",
        "message": "Feed stopped. Connection slots are being released by Dhan.",
        "cooldown": COOLDOWN,
    }))
}

pub async fn feed_restart(State(st): State<DhanState>) -> (StatusCode, Json<Value>) {
    let sess = st.session.read().await.clone();
    match sess {
        Some(s) => {
            // This endpoint is also the browser's automatic nudge. Two cases
            // must NOT tear the supervisor down:
            //  * an active cooldown is deliberate (/api/feed/reset parks the
            //    feed so Dhan releases the connection slot). Clearing it here
            //    reopened the socket mid-cooldown and immediately re-tripped the
            //    block, so the cooldown could never complete.
            //  * a supervisor is already alive - it reconnects on its own with a
            //    growing backoff. Respawning resets that backoff to 2s and hammers
            //    Dhan, which is what kept the client id blocked.
            if st.park_active() {
                return (
                    StatusCode::OK,
                    Json(json!({"status":"success","message":"Feed cooling down; leaving it parked"})),
                );
            }
            if st.feed_running() {
                return (
                    StatusCode::OK,
                    Json(json!({"status":"success","message":"Feed already running"})),
                );
            }
            st.start_feed(s).await;
            (
                StatusCode::OK,
                Json(json!({"status":"success","message":"Feed threads restarted"})),
            )
        }
        None => (
            StatusCode::CONFLICT,
            Json(json!({"status":"error","message":"Not connected"})),
        ),
    }
}

// ---------------------------------------------------------------------------
// Feed runtime
// ---------------------------------------------------------------------------

fn segment_exchange(seg: u8) -> Option<&'static str> {
    Some(match seg {
        0 => "IDX_I",
        1 => "NSE_EQ",
        2 => "NSE_FNO",
        3 => "NSE_CURRENCY",
        4 => "BSE_EQ",
        5 => "MCX_COMM",
        7 => "BSE_CURRENCY",
        8 => "BSE_FNO",
        _ => return None,
    })
}

/// GIFT NIFTY (IDX_I / security 5024) streams a frozen price on Dhan's live
/// feed, so its ticks are dropped here and the correct candle-derived quote is
/// published by `spawn_gift_nifty_fix` instead.
fn is_gift_nifty(sec_id: i64, exch: &str) -> bool {
    sec_id == 5024 && exch == "IDX_I"
}

fn feed_subscriptions() -> Vec<FeedSubscription> {
    securities()
        .into_iter()
        .filter_map(|(sid, exch)| {
            // Watchlist symbols and indices ride on Ticker mode; Dhan delivers
            // IDX_I as an Index Packet (response code 1) only in this mode.
            let mode = FeedMode::Ticker;
            exchange_segment(&exch).map(|s| FeedSubscription::with_mode(s, sid, mode))
        })
        .collect()
}

fn feed_quote(ltp: f64, close: f64, at: i64, volume: f64, oi: f64, bid: f64, ask: f64) -> Value {
    let change = if close != 0.0 { ltp - close } else { 0.0 };
    let pct = if close != 0.0 { change / close * 100.0 } else { 0.0 };
    let mut v = json!({
        "ltp": round2(ltp),
        "change": round2(change),
        "close": round2(close),
        "change_pct": round2(pct),
        "at": at,
        "live": 1,
    });
    // Only publish extra fields when present so a Ticker-mode symbol is not given
    // a fake volume/OI/depth (the old app merged them in per packet type).
    if volume > 0.0 {
        v["volume"] = json!(round2(volume));
    }
    if oi > 0.0 {
        v["oi"] = json!(round2(oi));
    }
    if bid > 0.0 {
        v["bid"] = json!(round2(bid));
    }
    if ask > 0.0 {
        v["ask"] = json!(round2(ask));
    }
    v
}

/// Build a live quote for one tick, merging the accumulated Volume / OI / Bid /
/// Ask extras and inferring IV + delta + vega for option strikes (Dhan's feed
/// carries no IV, so the old app inverted Black-Scholes from the live premium on
/// every tick, throttled to ~4/sec per strike).
fn live_quote(
    key: &str,
    sid: i64,
    ltp: f64,
    now: i64,
    closes: &HashMap<String, f64>,
    market: &MarketState,
    extras: &HashMap<String, (f64, f64, f64, f64)>,
    iv_at: &mut HashMap<String, Instant>,
    oi_base: &mut HashMap<String, f64>,
) -> Value {
    // Dhan's feed never sends a PrevClose packet, so the authoritative previous
    // close comes from the REST / daily-candle seed map. When a symbol is missing
    // there, fall back to the last cached close so a live tick can never zero out
    // an already-good change / change_pct.
    let mut close = closes.get(key).copied().unwrap_or(0.0);
    if close <= 0.0 {
        close = market
            .quotes
            .lock()
            .ok()
            .and_then(|g| {
                g.get(key)
                    .and_then(|e| e.get("close"))
                    .and_then(|c| c.as_f64())
            })
            .unwrap_or(0.0);
    }
    let (vol, oi, bid, ask) = extras.get(key).copied().unwrap_or((0.0, 0.0, 0.0, 0.0));
    let mut q = feed_quote(ltp, close, now, vol, oi, bid, ask);
    // Dhan's feed carries no previous-OI packet, so seed the OI baseline from the
    // first OI seen this session and derive the live change against it. This keeps
    // "Chg OI" ticking on the websocket instead of polling the REST chain.
    if oi > 0.0 {
        let base = *oi_base.entry(key.to_string()).or_insert(oi);
        if base > 0.0 {
            q["chg_oi"] = json!(round2(oi - base));
        }
    }
    if !key.starts_with("IDX_I:") && ltp > 0.0 {
        let due = iv_at
            .get(key)
            .map(|t| t.elapsed() >= Duration::from_millis(250))
            .unwrap_or(true);
        if due {
            iv_at.insert(key.to_string(), Instant::now());
            if let Some(g) = crate::optionchain::opt_greeks(sid, ltp) {
                for field in ["iv", "delta", "vega", "theta", "gamma"] {
                    if let Some(v) = g.get(field).and_then(|v| v.as_f64()) {
                        q[field] = json!(v);
                    }
                }
            }
        }
    }
    q
}

/// [`live_quote`] with the shared previous-close map locked for the duration of
/// the call (the REST seeder writes the same map from another task).
#[allow(clippy::too_many_arguments)]
fn live_quote_locked(
    closes: &Arc<Mutex<HashMap<String, f64>>>,
    key: &str,
    sid: i64,
    ltp: f64,
    now: i64,
    market: &MarketState,
    extras: &HashMap<String, (f64, f64, f64, f64)>,
    iv_at: &mut HashMap<String, Instant>,
    oi_base: &mut HashMap<String, f64>,
) -> Value {
    let guard = closes.lock().unwrap_or_else(|e| e.into_inner());
    live_quote(key, sid, ltp, now, &guard, market, extras, iv_at, oi_base)
}

async fn run_feed(
    client_id: String,
    token: String,
    subs: Vec<FeedSubscription>,
    market: MarketState,
    health: Arc<Mutex<FeedHealth>>,
    closes: Arc<Mutex<HashMap<String, f64>>>,
    cmd_rx: mpsc::UnboundedReceiver<FeedCommand>,
) {
    let count = subs.len();
    let feed = MarketFeed::new(client_id, token, subs);
    let (tx, mut rx) = mpsc::channel::<FeedPacket>(4096);
    let _runner = AbortOnDrop(tokio::spawn(feed.run(tx, cmd_rx)));

    if let Ok(mut g) = health.lock() {
        g.ws_running = true;
        // `feed_up` means "the broker socket is actually streaming", so it stays
        // false until the first packet arrives - a spawned task that is stuck
        // reconnecting must not look healthy.
        g.feed_up = false;
        g.socket_open = false;
        g.connected_at = None;
        g.feed_started = Some(Instant::now());
        g.subscribed = count;
    }

    // Accumulated (volume, oi, bid, ask) per quote key so a Ticker/Quote packet
    // keeps the Full-mode extras already received for the same strike.
    let mut extras: HashMap<String, (f64, f64, f64, f64)> = HashMap::new();
    let mut iv_at: HashMap<String, Instant> = HashMap::new();
    let mut oi_base: HashMap<String, f64> = HashMap::new();
    let mut pending: HashMap<String, Value> = HashMap::new();
    // Coalesce ticks into small batches. 150ms keeps the sidebar cadence smooth
    // (~6-7 pushes/sec) instead of the old 400ms step that read as "ruk ruk".
    // Coalescing (not per-tick sends) still keeps a busy option burst from
    // flooding the websocket.
    let mut flush = tokio::time::interval(Duration::from_millis(150));
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                let Some(pkt) = maybe else { break };
                let now = now_secs();
                // Link is a control signal, not market data: it must not mark
                // the feed as "up"/fresh (that made a socket stuck in reconnect
                // backoff look live and masked the outage).
                if let FeedPacket::Link { up } = pkt {
                    let mut park_long = 0u64;
                    if let Ok(mut g) = health.lock() {
                        g.socket_open = up;
                        if up {
                            g.connected_at = Some(Instant::now());
                        } else {
                            g.feed_up = false;
                            g.fail_streak = g.fail_streak.saturating_add(1);
                            if g.fail_streak >= MAX_FAIL_STREAK {
                                let secs = next_park_secs(g.park_secs);
                                g.park_secs = secs;
                                g.parked_until =
                                    Some(Instant::now() + Duration::from_secs(secs));
                                // Fresh budget for the next round after the park.
                                g.fail_streak = 0;
                                park_long = secs;
                            }
                        }
                    }
                    if park_long > 0 {
                        tracing::warn!(
                            "feed circuit-breaker: {} consecutive failures, parking {}s so Dhan can clear the rate limit",
                            MAX_FAIL_STREAK,
                            park_long
                        );
                        // End this supervisor; the watchdog re-spawns only after
                        // the park expires, giving Dhan a real quiet window.
                        break;
                    }
                    continue;
                }
                // Only a real trade packet counts as "the feed is streaming". A
                // PrevClose / OI / MarketStatus packet is sent even on a closed
                // market, so treating those as ticks made a holiday socket look
                // live and then "stall" 25s later, churning the connection.
                let is_live_tick = matches!(
                    &pkt,
                    FeedPacket::Ticker { .. }
                        | FeedPacket::Quote { .. }
                        | FeedPacket::Full { .. }
                );
                if is_live_tick {
                    if let Ok(mut g) = health.lock() {
                        g.last_tick = Some(Instant::now());
                        g.feed_up = true;
                    }
                }
                // Any frame (trade tick, PrevClose/OI snapshot, or a later
                // heartbeat) proves the socket is alive.
                if let Ok(mut g) = health.lock() {
                    g.last_activity = Some(Instant::now());
                    // A ping/pong specifically: refresh the operator-facing pong
                    // clock the moment Dhan answers. This is the signal behind the
                    // "pong received / not received from dhan" status; when it goes
                    // stale the feed client tears the half-open socket down and
                    // auto-reconnects.
                    if matches!(&pkt, FeedPacket::Heartbeat) {
                        g.last_pong = Some(Instant::now());
                    }
                }
                match pkt {
                    FeedPacket::PrevClose { segment, security_id, prev_close, .. } => {
                        if let Some(exch) = segment_exchange(segment) {
                            if let Ok(mut g) = closes.lock() {
                                g.insert(quote_key(security_id as i64, exch), prev_close as f64);
                            }
                        }
                    }
                    FeedPacket::Oi { segment, security_id, oi } => {
                        if let Some(exch) = segment_exchange(segment) {
                            let key = quote_key(security_id as i64, exch);
                            extras.entry(key).or_insert((0.0, 0.0, 0.0, 0.0)).1 = oi as f64;
                        }
                    }
                    FeedPacket::Ticker { segment, security_id, ltp, .. } => {
                        if let Some(exch) = segment_exchange(segment) {
                            if is_gift_nifty(security_id as i64, exch) {
                                continue;
                            }
                            let key = quote_key(security_id as i64, exch);
                            let q = live_quote_locked(&closes, &key, security_id as i64, ltp as f64, now, &market, &extras, &mut iv_at, &mut oi_base);
                            if key.starts_with("IDX_I:") {
                                crate::optionchain::set_index_spot(security_id as i64, ltp as f64);
                            }
                            market.patch_tick(security_id as i64, exch, ltp as f64, now + 19800);
                            market.set_quote(key.clone(), q.clone());
                            pending.insert(key, q);
                        }
                    }
                    FeedPacket::Quote { segment, security_id, ltp, volume, .. } => {
                        if let Some(exch) = segment_exchange(segment) {
                            if is_gift_nifty(security_id as i64, exch) {
                                continue;
                            }
                            let key = quote_key(security_id as i64, exch);
                            extras.entry(key.clone()).or_insert((0.0, 0.0, 0.0, 0.0)).0 = volume as f64;
                            let q = live_quote_locked(&closes, &key, security_id as i64, ltp as f64, now, &market, &extras, &mut iv_at, &mut oi_base);
                            if key.starts_with("IDX_I:") {
                                crate::optionchain::set_index_spot(security_id as i64, ltp as f64);
                            }
                            market.patch_tick(security_id as i64, exch, ltp as f64, now + 19800);
                            market.set_quote(key.clone(), q.clone());
                            pending.insert(key, q);
                        }
                    }
                    FeedPacket::Full { segment, security_id, ltp, volume, oi, depth, .. } => {
                        if let Some(exch) = segment_exchange(segment) {
                            if is_gift_nifty(security_id as i64, exch) {
                                continue;
                            }
                            let key = quote_key(security_id as i64, exch);
                            let (bid, ask) = depth
                                .first()
                                .map(|d| (d.bid_price as f64, d.ask_price as f64))
                                .unwrap_or((0.0, 0.0));
                            let e = extras.entry(key.clone()).or_insert((0.0, 0.0, 0.0, 0.0));
                            e.0 = volume as f64;
                            e.1 = oi as f64;
                            if bid > 0.0 {
                                e.2 = bid;
                            }
                            if ask > 0.0 {
                                e.3 = ask;
                            }
                            let q = live_quote_locked(&closes, &key, security_id as i64, ltp as f64, now, &market, &extras, &mut iv_at, &mut oi_base);
                            if key.starts_with("IDX_I:") {
                                crate::optionchain::set_index_spot(security_id as i64, ltp as f64);
                            }
                            market.patch_tick(security_id as i64, exch, ltp as f64, now + 19800);
                            market.set_quote(key.clone(), q.clone());
                            pending.insert(key, q);
                        }
                    }
                    FeedPacket::MarketStatus { .. } => {}
                    FeedPacket::Heartbeat => {
                        // Liveness only; `last_activity` was already refreshed.
                    }
                    FeedPacket::Disconnect { .. } => {
                        // Dhan closes long-lived sockets periodically and the
                        // inner `MarketFeed` supervisor reconnects on its own
                        // with backoff. A disconnect must therefore NOT tear
                        // this consumer down: just mark the socket as not
                        // streaming and keep consuming. Treating it as terminal
                        // is what used to silently freeze the chart, ticker,
                        // option chain and running-strategy P&L until a manual
                        // reconnect.
                        if let Ok(mut g) = health.lock() {
                            g.feed_up = false;
                        }
                    }
                    FeedPacket::Link { .. } => {
                        // Handled above (it is not market data).
                    }
                    FeedPacket::Unknown { .. } => {}
                }
            }
            _ = flush.tick() => {
                market.broadcast_quotes(&pending);
                pending.clear();
            }
        }
    }

    // `_runner` (the inner websocket task) is aborted when it drops here.
    if let Ok(mut g) = health.lock() {
        g.ws_running = false;
        g.feed_up = false;
        g.feed_started = None;
        g.socket_open = false;
        g.connected_at = None;
    }
}

// ---------------------------------------------------------------------------
// Timeframe mapping (mirrors the old app's TIMEFRAME_CONFIG)
// ---------------------------------------------------------------------------

enum TfPlan {
    Intraday { interval: u32, resample: Option<i64> },
    Daily,
}

fn timeframe_plan(tf: &str) -> TfPlan {
    match tf {
        "1min" => TfPlan::Intraday { interval: 1, resample: None },
        "2min" => TfPlan::Intraday { interval: 1, resample: Some(120) },
        "3min" => TfPlan::Intraday { interval: 1, resample: Some(180) },
        "4min" => TfPlan::Intraday { interval: 1, resample: Some(240) },
        "5min" => TfPlan::Intraday { interval: 5, resample: None },
        "10min" => TfPlan::Intraday { interval: 5, resample: Some(600) },
        "15min" => TfPlan::Intraday { interval: 15, resample: None },
        "30min" => TfPlan::Intraday { interval: 5, resample: Some(1800) },
        "1hour" => TfPlan::Intraday { interval: 60, resample: None },
        "4hour" => TfPlan::Intraday { interval: 60, resample: Some(14400) },
        _ => TfPlan::Daily,
    }
}

fn tf_step_secs(tf: &str) -> i64 {
    match tf {
        "week" => 604800,
        "month" => 2592000,
        "year" => 31536000,
        "1min" | "2min" | "3min" | "4min" | "5min" | "10min" | "15min" | "30min" | "1hour" => 300,
        "4hour" => 14400,
        _ => 86400,
    }
}

fn to_candles(rows: Vec<CandleRow>) -> Vec<Candle> {
    rows.into_iter()
        .map(|r| Candle {
            time: r.time,
            open: r.open,
            high: r.high,
            low: r.low,
            close: r.close,
            volume: r.volume,
        })
        .collect()
}

/// Bucket candles into `step`-second bars (open=first, close=last, high=max,
/// low=min, volume=sum). Rows must already be sorted by time.
fn resample_candles(rows: &[CandleRow], step: i64) -> Vec<Candle> {
    if step <= 0 || rows.is_empty() {
        return to_candles(rows.to_vec());
    }
    let mut out: Vec<Candle> = Vec::new();
    let mut bucket = i64::MIN;
    let mut cur: Option<Candle> = None;
    for r in rows {
        let b = r.time - r.time.rem_euclid(step);
        if b != bucket {
            if let Some(c) = cur.take() {
                out.push(c);
            }
            bucket = b;
            cur = Some(Candle {
                time: b,
                open: r.open,
                high: r.high,
                low: r.low,
                close: r.close,
                volume: r.volume,
            });
        } else if let Some(c) = cur.as_mut() {
            c.high = c.high.max(r.high);
            c.low = c.low.min(r.low);
            c.close = r.close;
            c.volume += r.volume;
        }
    }
    if let Some(c) = cur {
        out.push(c);
    }
    out
}

// ---------------------------------------------------------------------------
// Enum + date helpers
// ---------------------------------------------------------------------------

fn exchange_segment(s: &str) -> Option<ExchangeSegment> {
    Some(match s.to_uppercase().as_str() {
        "IDX_I" => ExchangeSegment::IdxI,
        "NSE_EQ" => ExchangeSegment::NseEq,
        "NSE_FNO" => ExchangeSegment::NseFno,
        "NSE_CURRENCY" => ExchangeSegment::NseCurrency,
        "BSE_EQ" => ExchangeSegment::BseEq,
        "MCX_COMM" => ExchangeSegment::McxComm,
        "BSE_CURRENCY" => ExchangeSegment::BseCurrency,
        "BSE_FNO" => ExchangeSegment::BseFno,
        _ => return None,
    })
}

/// Feed depth for a security: only real OPTION contracts need the full packet
/// (IV from depth, OI, volume, greeks). Everything else - indices, cash
/// equities, underlying spot ids and commodity futures - is priced from the LTP
/// alone, so subscribing it in Ticker mode is enough.
///
/// This is the single biggest CPU saver on the 2-core host: the option hot path
/// used to push its entire window (spot underlying + every strike + scanner
/// ids) through Full mode, so every one of the ~900 live instruments arrived as
/// a 60+ byte multi-field packet that `live_quote` then turned into JSON (plus
/// Black-Scholes greeks) on each tick. Restricting Full to actual options cuts
/// both the parse volume and the per-tick JSON/greeks work for the underlying
/// and scanner universe, which is what was saturating both tokio workers.
fn feed_mode_for(sid: i64) -> FeedMode {
    let is_option = crate::optionchain::known_option_sid(sid)
        || crate::scrip::get().map(|s| s.is_option(sid)).unwrap_or(false);
    if is_option {
        FeedMode::Full
    } else {
        FeedMode::Ticker
    }
}

/// The alternate option instrument type Dhan may accept when the scrip-master /
/// resolved one returns no intraday bars: index options (OPTIDX) sometimes only
/// answer as OPTSTK and vice versa. Returns `None` for non-option instruments.
fn alt_option_instrument(inst: Instrument) -> Option<Instrument> {
    match inst {
        Instrument::OptIdx => Some(Instrument::OptStk),
        Instrument::OptStk => Some(Instrument::OptIdx),
        _ => None,
    }
}

fn instrument_type(s: &str) -> Instrument {
    match s.to_uppercase().as_str() {
        "INDEX" => Instrument::Index,
        "FUTIDX" => Instrument::FutIdx,
        "OPTIDX" => Instrument::OptIdx,
        "FUTSTK" => Instrument::FutStk,
        "OPTSTK" => Instrument::OptStk,
        "FUTCOM" => Instrument::FutCom,
        "OPTFUT" => Instrument::OptFut,
        "FUTCUR" => Instrument::FutCur,
        "OPTCUR" => Instrument::OptCur,
        _ => Instrument::Equity,
    }
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Days since Unix epoch to (year, month, day) - Howard Hinnant's algorithm.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn fmt_date(epoch_ist: i64) -> String {
    let days = epoch_ist.div_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

fn fmt_datetime(epoch_ist: i64) -> String {
    let days = epoch_ist.div_euclid(86400);
    let secs = epoch_ist.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        y,
        m,
        d,
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}

#[cfg(test)]
mod chart_instrument_tests {
    use super::*;

    #[test]
    fn alt_option_instrument_flips_index_and_stock_options() {
        assert_eq!(alt_option_instrument(Instrument::OptIdx), Some(Instrument::OptStk));
        assert_eq!(alt_option_instrument(Instrument::OptStk), Some(Instrument::OptIdx));
    }

    #[test]
    fn alt_option_instrument_ignores_non_options() {
        for inst in [
            Instrument::Index,
            Instrument::Equity,
            Instrument::FutIdx,
            Instrument::FutStk,
            Instrument::FutCom,
        ] {
            assert_eq!(alt_option_instrument(inst), None);
        }
    }

    #[test]
    fn instrument_type_maps_option_strings() {
        assert_eq!(instrument_type("OPTIDX"), Instrument::OptIdx);
        assert_eq!(instrument_type("optstk"), Instrument::OptStk);
        assert_eq!(instrument_type("OPTFUT"), Instrument::OptFut);
    }
}

#[cfg(test)]
mod live_quote_tests {
    use super::*;

    #[test]
    fn live_quote_derives_chg_oi_from_first_seen_baseline() {
        let market = MarketState::new();
        let mut closes: HashMap<String, f64> = HashMap::new();
        closes.insert("IDX_I:13".to_string(), 100.0);
        let mut extras: HashMap<String, (f64, f64, f64, f64)> = HashMap::new();
        extras.insert("IDX_I:13".to_string(), (0.0, 1200.0, 0.0, 0.0));
        let mut iv_at: HashMap<String, Instant> = HashMap::new();
        let mut oi_base: HashMap<String, f64> = HashMap::new();

        // First OI seen seeds the baseline, so the change starts at zero.
        let q = live_quote(
            "IDX_I:13", 13, 101.0, 0, &closes, &market, &extras, &mut iv_at, &mut oi_base,
        );
        assert_eq!(q.get("chg_oi").and_then(|v| v.as_f64()), Some(0.0));

        // A later tick measures against the seeded baseline, not the last tick.
        extras.insert("IDX_I:13".to_string(), (0.0, 1500.0, 0.0, 0.0));
        let q2 = live_quote(
            "IDX_I:13", 13, 101.0, 0, &closes, &market, &extras, &mut iv_at, &mut oi_base,
        );
        assert_eq!(q2.get("chg_oi").and_then(|v| v.as_f64()), Some(300.0));
    }
}

#[cfg(test)]
mod link_state_tests {
    use super::*;

    #[test]
    fn offline_when_no_session_regardless_of_market() {
        assert_eq!(link_state(false, true, true, Some(0.0), true, Some(1.0)), "offline");
        assert_eq!(link_state(false, false, false, None, false, None), "offline");
    }

    #[test]
    fn closed_when_session_but_market_shut() {
        assert_eq!(link_state(true, false, true, Some(0.0), true, Some(5.0)), "closed");
        assert_eq!(link_state(true, false, false, None, false, None), "closed");
    }

    #[test]
    fn live_when_fresh_tick() {
        assert_eq!(link_state(true, true, true, Some(0.0), true, Some(60.0)), "live");
        assert_eq!(link_state(true, true, true, Some(14.9), true, Some(60.0)), "live");
    }

    #[test]
    fn heartbeat_keeps_a_tickless_socket_live() {
        // No trade tick has landed yet (feed_up=false), but the 5s keepalive /
        // Dhan heartbeat arrived 5s ago: the socket is alive and must stay
        // "live" so the UI never trips its auto-reset while the market is quiet.
        assert_eq!(link_state(true, true, false, Some(5.0), true, Some(120.0)), "live");
        // Same, past the 30s handshake window: activity still wins.
        assert_eq!(link_state(true, true, false, Some(2.5), true, Some(600.0)), "live");
    }

    #[test]
    fn stale_tick_is_down_not_live() {
        // feed_up but the last tick is too old: a stalled socket must not look live.
        assert_eq!(link_state(true, true, true, Some(15.0), true, Some(600.0)), "down");
        assert_eq!(link_state(true, true, true, None, true, Some(600.0)), "down");
    }

    #[test]
    fn connecting_while_socket_opens() {
        assert_eq!(link_state(true, true, false, None, true, Some(0.0)), "connecting");
        assert_eq!(link_state(true, true, false, None, true, Some(29.0)), "connecting");
        // handshake window elapsed with still no data -> real outage.
        assert_eq!(link_state(true, true, false, None, true, Some(31.0)), "down");
        // socket not running at all -> outage.
        assert_eq!(link_state(true, true, false, None, false, Some(5.0)), "down");
    }

    #[test]
    fn park_backoff_doubles_then_caps() {
        // First park is the baseline, then it doubles up to MAX_PARK_SECS so a
        // long Dhan IP block is not poked every few minutes.
        assert_eq!(next_park_secs(0), LONG_PARK_SECS);
        assert_eq!(next_park_secs(LONG_PARK_SECS), LONG_PARK_SECS * 2);
        assert_eq!(next_park_secs(LONG_PARK_SECS * 2), LONG_PARK_SECS * 4);
        assert_eq!(next_park_secs(LONG_PARK_SECS * 4), MAX_PARK_SECS);
        assert_eq!(next_park_secs(MAX_PARK_SECS), MAX_PARK_SECS);
        assert!(MAX_PARK_SECS >= LONG_PARK_SECS * 4);
    }
}

#[cfg(test)]
mod data_stall_tests {
    use super::*;

    #[test]
    fn no_tick_yet_is_not_a_stall() {
        // `feed_up` false means this socket never produced a real market packet
        // (fresh handshake, closed market, holiday): it must not be churned.
        assert!(!data_stalled(false, None));
        assert!(!data_stalled(false, Some(Duration::from_secs(3600))));
    }

    #[test]
    fn fresh_tick_is_healthy() {
        assert!(!data_stalled(true, Some(Duration::from_secs(0))));
        assert!(!data_stalled(true, Some(Duration::from_secs(DATA_STALL_SECS))));
    }

    #[test]
    fn heartbeat_alive_but_data_stale_is_a_stall() {
        // The daily freeze: Dhan keeps answering keepalive Pings (so
        // `last_activity`/`last_pong` stay fresh) but no Ticker/Quote/Full
        // packet arrives. The old watchdog could never see this and only a
        // manual Connect recovered it; now it auto-restarts.
        assert!(data_stalled(
            true,
            Some(Duration::from_secs(DATA_STALL_SECS + 1))
        ));
        assert!(data_stalled(true, Some(Duration::from_secs(600))));
    }
}


#[cfg(test)]
mod rate_hold_tests {
    use super::*;

    fn state() -> DhanState {
        DhanState::new(MarketState::new())
    }

    #[tokio::test]
    async fn burst_escalates_only_once_per_hold_window() {
        let st = state();
        // The first 429 opens the hold at the first penalty step...
        st.note_rate_limited();
        let first = st.rate_hold_remaining();
        assert!(first > 0.0, "first hit must open a hold");
        assert!(first <= RATE_LIMIT_FIRST_PENALTY_SECS as f64 + 0.1);
        // ...and every other reply from the SAME burst is absorbed, so one burst
        // can never jump the penalty to its ceiling.
        for _ in 0..50 {
            st.note_rate_limited();
        }
        let after = st.rate_hold_remaining();
        assert!(
            after <= first + 0.5,
            "tail of a burst must not escalate (before {first}s, after {after}s)"
        );
        assert!(after < RATE_LIMIT_MAX_PENALTY_SECS as f64);
    }

    #[tokio::test]
    async fn a_successful_call_resets_the_ramp() {
        let st = state();
        st.note_rate_limited();
        st.note_data_ok();
        assert_eq!(st.rate_hold_remaining(), 0.0);
        assert!(!st.rate_limited());
        // After recovery the next hit starts from the first step again.
        st.note_rate_limited();
        let again = st.rate_hold_remaining();
        assert!(again > 0.0);
        assert!(again <= RATE_LIMIT_FIRST_PENALTY_SECS as f64 + 0.1);
    }

    #[tokio::test]
    async fn open_gate_reports_no_hold() {
        let st = state();
        assert_eq!(st.rate_hold_remaining(), 0.0);
        assert!(!st.rate_limited());
    }

    /// A scanner underlying is subscribed on Ticker mode; when the strike path
    /// later asks for the SAME security id it must still emit a Full-mode
    /// subscribe, otherwise the scanner's Ticker registration would silently
    /// suppress the strike's IV/OI/volume stream.
    #[tokio::test]
    async fn scanner_ticker_does_not_block_full_promotion() {
        let st = state();
        let (tx, mut rx) = mpsc::unbounded_channel();
        if let Ok(mut g) = st.feed_tx.lock() {
            *g = Some(tx);
        }
        st.watch_scanner_now(&[(11536, "NSE_EQ".to_string())]);
        let first = rx.try_recv().expect("scanner subscribe must be sent");
        let FeedCommand::Subscribe(first) = first else { panic!("expected subscribe") };
        assert_eq!(first[0].mode, FeedMode::Ticker);
        // Re-running the scanner pass is deduplicated (no socket churn).
        st.watch_scanner_now(&[(11536, "NSE_EQ".to_string())]);
        assert!(rx.try_recv().is_err(), "repeat scanner pass must not resubscribe");
        // The strike path must still promote the same id to Full.
        st.watch_options_now(&[(11536, "NSE_EQ".to_string())]);
        let promote = rx.try_recv().expect("Full promotion must be sent");
        let FeedCommand::Subscribe(promote) = promote else { panic!("expected subscribe") };
        assert_eq!(promote[0].mode, FeedMode::Full);
        // ...and its own dedup keeps it from re-sending on the next tick.
        st.watch_options_now(&[(11536, "NSE_EQ".to_string())]);
        assert!(rx.try_recv().is_err(), "repeat strike pass must not resubscribe");
    }
}
