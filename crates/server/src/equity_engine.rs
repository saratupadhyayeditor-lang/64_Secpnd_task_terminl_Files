use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::response::Json;
use dhan_hq::models::{
    ExchangeSegment, OrderRequest, OrderType, ProductType, TransactionType, Validity,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::broker::DhanState;
use crate::equity;
use crate::scrip;

const MODES: [&str; 2] = ["paper", "live"];

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Settings {
    pub capital: f64,
    pub per_trade_pct: f64,
    pub max_positions: usize,
    /// Hard cap on how much of `capital` any SINGLE name can occupy, as a
    /// percent. Auto-distribute over a small book could otherwise put 50%+ of
    /// the capital into one microcap, so one gap-down stop wipes the account.
    pub max_position_pct: f64,
    /// Minimum MEASURED win-rate (%) a call must show before it is bought. Since
    /// confidence now reports the replayed win-rate (typically 40-60%), this gate
    /// lives on that same honest scale instead of the old "data coverage" score.
    pub min_confidence: f64,
    /// One-time migration marker: when false the stored `min_confidence` is on
    /// the old 0-100 data-coverage scale and is rebased onto the win-rate scale.
    pub confidence_rebased: bool,
    pub min_upside_pct: f64,
    pub max_holding_days: i64,
    pub auto_exit: bool,
    pub allow_accumulate: bool,
    pub watch_symbols: Vec<String>,
    pub fixed_qty: i64,
    /// Auto-distribute margin: split the REMAINING margin across the REMAINING
    /// position slots, so every slot gets an equal share and the pool is fully
    /// deployed instead of shrinking after each fill. When ON, the per-trade
    /// percentage and fixed quantity are ignored (a manual qty override wins).
    pub auto_distribute: bool,
    /// Auto Buy Mode: allow the engine cycle to OPEN new positions from the
    /// research calls. OFF keeps position management (marks, targets, stops,
    /// exits) running but stops automatic entries; manual buys still work.
    pub auto_buy: bool,
    /// Charge-aware sizing: reserve the estimated round-trip delivery charges
    /// (brokerage + STT + exchange txn + SEBI + stamp + GST) out of the slot
    /// budget before computing quantity, and refuse a name whose expected net
    /// profit at target cannot clear those charges with a safety margin.
    pub charges_aware: bool,
    /// Percentage trailing stop loss. Once the position is in profit the stop
    /// ratchets up behind the peak, giving back `trail_sl_pct` percent of the
    /// running profit. `0` disables it (only the fixed stop applies).
    pub trail_sl_on: bool,
    pub trail_sl_pct: f64,
    /// Net Take-Profit: track the peak NET profit (delivery charges already
    /// deducted) and exit once that peak gives back `net_tp_giveback_pct`
    /// percent of itself. Only arms after the net profit crosses
    /// `net_tp_min` rupees. `0` rupees (or the switch OFF) disables it.
    pub net_tp_on: bool,
    pub net_tp_min: f64,
    pub net_tp_giveback_pct: f64,
    /// Daily square-off: exit EVERY open position at a fixed IST time each day
    /// so the freed margin is redeployed into the freshest, highest-probability
    /// names the same cycle. `daily_exit_time` is IST `HH:MM` (blank = 15:20).
    /// The exit fires once per calendar day; a new day re-arms it.
    pub daily_exit_on: bool,
    pub daily_exit_time: String,
    /// Last IST day (yyyymmdd-style day count) the daily square-off ran, so it
    /// fires exactly once per day even though the cycle ticks every 45 seconds.
    pub daily_exit_day: i64,
    /// Symbols handed over by Equity Research's "Auto send scanned stocks to
    /// trading engines" switch. They are bought as soon as a slot and shared
    /// margin are free, WITHOUT waiting for a Buy call or the confidence /
    /// upside filters - the operator explicitly asked for these names. A name is
    /// removed from this list once it is opened (or the list is cleared from the
    /// UI), so an unbought name keeps retrying while the book is full.
    pub auto_push_symbols: Vec<String>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            capital: 500_000.0,
            per_trade_pct: 10.0,
            max_positions: 8,
            max_position_pct: 15.0,
            min_confidence: 52.0,
            // Must default to false so an OLD state file (which lacks this field)
            // deserializes as "not yet rebased" and the one-time 70->52 migration
            // in `load()` actually runs. Defaulting to true silently kept the old
            // 70 gate on the new win-rate scale and blocked every honest call.
            confidence_rebased: false,
            min_upside_pct: 12.0,
            max_holding_days: 180,
            auto_exit: true,
            allow_accumulate: false,
            watch_symbols: Vec::new(),
            fixed_qty: 0,
            auto_distribute: false,
            auto_buy: true,
            charges_aware: true,
            trail_sl_on: false,
            trail_sl_pct: 25.0,
            net_tp_on: false,
            net_tp_min: 0.0,
            net_tp_giveback_pct: 25.0,
            daily_exit_on: false,
            daily_exit_time: "15:20".to_string(),
            daily_exit_day: 0,
            auto_push_symbols: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Trade {
    pub id: String,
    pub mode: String,
    pub symbol: String,
    pub name: String,
    pub exchange: String,
    pub status: String,
    pub confidence: f64,
    pub entry_price: f64,
    pub qty: i64,
    pub invested: f64,
    pub target: f64,
    pub stop: f64,
    pub last_price: f64,
    pub peak_price: f64,
    /// Live percentage trail stop level (0 = not armed yet). Ratchets only
    /// upward behind the peak profit, so it is safe to persist.
    pub trail_stop: f64,
    /// Peak NET rupee profit seen so far, for the Net Take-Profit give-back.
    pub net_peak: f64,
    /// Current net rupee profit at the last mark (gross minus round-trip
    /// delivery charges), shown in the UI so the operator can see the number
    /// Net TP is watching.
    pub net_now: f64,
    pub opened_at: i64,
    pub opened_date: String,
    pub order_id: String,
    pub exit_price: f64,
    pub exited_at: i64,
    pub exit_date: String,
    pub exit_reason: String,
    pub pnl: f64,
    pub pnl_pct: f64,
    pub days: i64,
    pub note: String,
}

impl Trade {
    fn is_open(&self) -> bool {
        self.exited_at == 0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EngineState {
    pub mode: String,
    pub enabled: bool,
    pub settings: Settings,
    pub trades: Vec<Trade>,
    pub log: Vec<String>,
    pub last_run: i64,
    pub last_error: String,
    pub created_at: i64,
}

impl Default for EngineState {
    fn default() -> Self {
        Self {
            mode: String::new(),
            enabled: false,
            settings: Settings::default(),
            trades: Vec::new(),
            log: Vec::new(),
            last_run: 0,
            last_error: String::new(),
            created_at: now_secs(),
        }
    }
}

static PAPER: OnceLock<Mutex<EngineState>> = OnceLock::new();
static LIVE: OnceLock<Mutex<EngineState>> = OnceLock::new();
static PAPER_RUNNING: AtomicBool = AtomicBool::new(false);
static LIVE_RUNNING: AtomicBool = AtomicBool::new(false);

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn today_ist() -> i64 {
    (now_secs() + 19_800) / 86_400
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn ymd(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Parse an IST `HH:MM` clock string into minutes past midnight.
fn parse_hhmm(s: &str) -> Option<i64> {
    let (h, m) = s.trim().split_once(':')?;
    let h: i64 = h.trim().parse().ok()?;
    let m: i64 = m.trim().parse().ok()?;
    if !(0..=23).contains(&h) || !(0..=59).contains(&m) {
        return None;
    }
    Some(h * 60 + m)
}

/// IST minute-of-day, or `None` on a weekend (Mon-Fri only).
fn ist_weekday_minutes(now: i64) -> Option<i64> {
    let ist = now + 19_800;
    let weekday = (ist.div_euclid(86_400) + 4).rem_euclid(7);
    if weekday == 0 || weekday == 6 {
        return None;
    }
    Some(ist.rem_euclid(86_400) / 60)
}

/// Should the daily square-off fire right now? It is due once per IST day,
/// after the configured clock time, and only while the NSE session is open so
/// the broker will actually accept the sell orders. The `enabled` switch is the
/// operator's "Daily square-off all positions" checkbox: when it is OFF the
/// schedule must never fire, no matter what time/day the engine is ticking.
/// Kept pure for tests.
fn daily_exit_due(now: i64, enabled: bool, time: &str, last_day: i64, session_open: bool) -> bool {
    if !enabled {
        return false;
    }
    if !session_open {
        return false;
    }
    let Some(target) = parse_hhmm(time) else {
        return false;
    };
    let Some(now_mins) = ist_weekday_minutes(now) else {
        return false;
    };
    now_mins >= target && today_ist_at(now) != last_day
}

fn today_ist_at(now: i64) -> i64 {
    (now + 19_800) / 86_400
}

fn data_path(mode: &str) -> PathBuf {
    let dir = PathBuf::from("data");
    let _ = std::fs::create_dir_all(&dir);
    dir.join(format!("equity_{mode}_engine.json"))
}

fn load(mode: &str) -> EngineState {
    let mut st = std::fs::read_to_string(data_path(mode))
        .ok()
        .and_then(|s| serde_json::from_str::<EngineState>(&s).ok())
        .unwrap_or_default();
    st.mode = mode.to_string();
    if st.created_at == 0 {
        st.created_at = now_secs();
    }
    // One-time rebase: confidence used to be a 0-100 "data coverage" number and
    // the gate sat at ~70. It is now the MEASURED replay win-rate (~40-60%), so a
    // 70 gate would block every honest call. Remap once and never again.
    //
    // We ALSO heal the broken legacy state where `confidence_rebased` was already
    // persisted as true while `min_confidence` still held the exact legacy 70
    // default (an earlier build defaulted the flag to true, so the migration was
    // skipped). Only the exact 70 default is healed so a deliberate high gate on
    // the new scale is never clobbered on restart.
    if !st.settings.confidence_rebased
        || (st.settings.min_confidence - 70.0).abs() < 0.5
    {
        st.settings.min_confidence = 52.0;
        st.settings.confidence_rebased = true;
        st.log.push(note(
            "Confidence ab measured win-rate par shift ho gayi - buy gate 52% par rebase kiya.",
        ));
    }
    if mode == "live" {
        if st.enabled {
            st.log.push(note("Server restarted - LIVE equity engine left disconnected. Re-arm it from Equity Research."));
        }
        st.enabled = false;
    }
    st
}

fn state(mode: &str) -> &'static Mutex<EngineState> {
    if mode == "live" {
        LIVE.get_or_init(|| Mutex::new(load("live")))
    } else {
        PAPER.get_or_init(|| Mutex::new(load("paper")))
    }
}

fn guard(mode: &str) -> MutexGuard<'static, EngineState> {
    match state(mode).lock() {
        Ok(g) => g,
        Err(e) => e.into_inner(),
    }
}

fn note(msg: impl Into<String>) -> String {
    format!("{} {}", ymd(today_ist()), msg.into())
}

fn save(mode: &str, st: &EngineState) {
    if let Ok(s) = serde_json::to_string_pretty(st) {
        let _ = std::fs::write(data_path(mode), s);
    }
}

fn log(st: &mut EngineState, msg: impl Into<String>) {
    st.log.push(note(msg));
    if st.log.len() > 300 {
        let drop = st.log.len() - 300;
        st.log.drain(0..drop);
    }
}

fn round2(v: f64) -> f64 {
    let r = (v * 100.0).round() / 100.0;
    if r.abs() < 0.005 { 0.0 } else { r }
}

/// Order the equity book by EXPECTED RETURN (upside to target) from highest to
/// lowest; buy-confidence only breaks ties. Kept as a pure helper so the same
/// ordering is unit-tested and reused without touching the live cycle.
fn order_candidates(c: &mut [(f64, f64, String, Value)]) {
    c.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal))
    });
}

/// What the buy loop should do with one ranked candidate.
#[derive(Debug, PartialEq, Eq)]
enum CandidateStep {
    /// Already holding this name - skip it and keep looking.
    Skip,
    /// No free slot or shared margin left - the book is full.
    Stop,
    /// Free slot and margin available - try to enter.
    Go,
}

fn candidate_step(already: bool, slots: usize, remaining: f64) -> CandidateStep {
    if already {
        CandidateStep::Skip
    } else if slots == 0 || remaining < 1_000.0 {
        CandidateStep::Stop
    } else {
        CandidateStep::Go
    }
}

fn num(v: Option<&Value>) -> f64 {
    v.and_then(|x| x.as_f64()).unwrap_or(0.0)
}

fn f(v: &Value, key: &str) -> f64 {
    num(v.get(key))
}

/// Percentage trailing stop: give back `pct` percent of the RUNNING profit.
/// Only meaningful once `peak` is positive, so the stop never arms off entry.
/// Delivery equity is long-only here, so the formula only needs the buy side.
fn equity_trail_stop(entry: f64, peak: f64, pct: f64) -> f64 {
    if pct <= 0.0 || peak <= 0.0 {
        return 0.0;
    }
    let giveback = peak * pct / 100.0;
    entry + peak - giveback
}

/// Price at which a round trip on `qty` exactly breaks even after delivery
/// charges. A trail stop is never allowed below this, so a trailing exit can
/// never book a NET loss - which is exactly what used to churn the book to death
/// (521 same-day "trail" exits, median +Rs 3, all eaten by charges).
fn equity_breakeven_price(entry: f64, qty: i64) -> f64 {
    if entry <= 0.0 || qty <= 0 {
        return entry;
    }
    // Charges are ~ the same on both legs, so approximate the per-share drag with
    // the round-trip cost at the entry price.
    let charges = equity_charges(entry, entry, qty);
    entry + charges / qty as f64
}

/// Net Take-Profit give-back floor: `None` until the peak NET profit has
/// crossed the rupee floor (feature not armed). Otherwise `giveback_pct` below
/// the peak, clamped to the floor so a locked trade never books below it.
fn equity_net_tp_floor(net_peak: f64, min: f64, giveback_pct: f64) -> Option<f64> {
    if min <= 0.0 || net_peak < min {
        return None;
    }
    let gb = if giveback_pct > 0.0 && giveback_pct < 100.0 {
        giveback_pct
    } else {
        25.0
    };
    Some((net_peak * (1.0 - gb / 100.0)).max(min))
}

/// Round-trip delivery charges for a long equity trade. Reuses the realtime
/// engine's rate tables so a paper Net TP target matches the real charge stack.
fn equity_charges(entry: f64, exit: f64, qty: i64) -> f64 {
    if entry <= 0.0 || exit <= 0.0 || qty <= 0 {
        return 0.0;
    }
    let gross = (exit - entry) * qty as f64;
    crate::realtime::round_trip_charges(entry, exit, qty as f64, "BUY", "EQUITY", "", gross)
        .unwrap_or(0.0)
}

fn esc_mode(mode: &str) -> Option<&'static str> {
    MODES.iter().copied().find(|m| *m == mode)
}

fn row_price(row: &Value) -> f64 {
    let p = f(row, "price");
    if p > 0.0 {
        p
    } else {
        f(row, "close")
    }
}

fn choose_qty(alloc: f64, px: f64, fixed_qty: i64, qty_override: Option<i64>) -> i64 {
    if let Some(q) = qty_override.filter(|q| *q > 0) {
        return q;
    }
    if fixed_qty > 0 {
        return fixed_qty;
    }
    if px <= 0.0 {
        return 0;
    }
    (alloc / px).floor() as i64
}

/// Total margin the equity engine may deploy = its shared-margin allotment
/// (see [`crate::shared_margin`]), still bounded by the amount not yet
/// invested. The shared allotment is the ONLY margin source; the legacy
/// per-engine `capital` field no longer authorises any size. An inactive
/// allotment (or an empty family pool) resolves to Rs 0, blocking every entry.
fn auto_distribute_total(cap: f64, remaining: f64) -> f64 {
    cap.min(remaining).max(0.0)
}

/// Equal share of the REMAINING margin across the REMAINING free slots. Dividing
/// by `max_positions` (as before) made each successive slot smaller, so the pool
/// was never fully deployed; dividing by the slots actually left keeps every fill
/// the same size and ends with the whole margin invested.
fn auto_distribute_alloc(cap: f64, _settings: &Settings, remaining: f64, free_slots: usize) -> f64 {
    let slots = free_slots.max(1) as f64;
    (auto_distribute_total(cap, remaining) / slots).max(0.0)
}

/// Round-trip delivery charges (brokerage + STT + exchange txn + SEBI + stamp +
/// GST) on a given turnover, using the realtime engine's rate tables.
fn delivery_charges_for_turnover(turnover: f64) -> f64 {
    if turnover <= 0.0 {
        return 0.0;
    }
    // Both legs carry the same turnover, so pass it as the per-leg price with a
    // unit quantity (the helper multiplies price by qty per side).
    crate::realtime::round_trip_charges(turnover, turnover, 1.0, "BUY", "EQUITY", "", 0.0)
        .unwrap_or(0.0)
        .max(0.0)
}

/// Turnover a slot can actually deploy once the round-trip charges are reserved
/// out of its allocation. Charges scale with turnover, so the budget is solved
/// iteratively; at sub-1% delivery rates two or three passes converge.
fn charges_aware_budget(alloc: f64, charges_aware: bool) -> f64 {
    if alloc <= 0.0 {
        return 0.0;
    }
    if !charges_aware {
        return alloc;
    }
    let mut turnover = alloc;
    for _ in 0..3 {
        turnover = (alloc - delivery_charges_for_turnover(turnover)).max(0.0);
    }
    turnover
}

/// Guard against a trade whose expected profit at target cannot clear its own
/// round-trip charges: a small profit haircut would then book a net loss. Needs
/// 1.25x the charges in gross profit (and at least the charges plus Rs 100).
fn charges_leave_room(px: f64, target: f64, qty: i64, charges_aware: bool) -> Result<(), String> {
    if !charges_aware || qty < 1 || px <= 0.0 || target <= px {
        return Ok(());
    }
    let turnover = px * qty as f64;
    let charges = delivery_charges_for_turnover(turnover);
    let gross = (target - px) * qty as f64;
    // A 25% buffer over the charges is enough: a Rs 100 absolute floor would
    // make a legitimate 1-share trade with an 18% target look unprofitable.
    let needed = (charges * 1.25).max(1.0);
    if gross < needed {
        return Err(format!(
            "expected profit Rs {:.0} cannot clear round-trip charges Rs {:.0}",
            gross, charges
        ));
    }
    Ok(())
}

fn engine_id_of(mode: &str) -> &'static str {
    if mode == "live" {
        "equitylive"
    } else {
        "equitypaper"
    }
}

/// Deployable capital for one equity engine = its shared-margin allotment. The
/// legacy `capital` setting is ignored entirely; the allotment is the single
/// source of truth, so an engine without an active allotment is blocked.
fn engine_capital(mode: &str, _capital: f64) -> f64 {
    crate::shared_margin::cap_for(engine_id_of(mode)).unwrap_or(0.0)
}

fn trade_stats(trades: &[Trade]) -> Value {
    let closed: Vec<&Trade> = trades.iter().filter(|t| !t.is_open()).collect();
    let open: Vec<&Trade> = trades.iter().filter(|t| t.is_open()).collect();
    let wins: Vec<&&Trade> = closed.iter().filter(|t| t.pnl > 0.0).collect();
    let losses: Vec<&&Trade> = closed.iter().filter(|t| t.pnl <= 0.0).collect();
    let realized: f64 = closed.iter().map(|t| t.pnl).sum();
    let closed_cost: f64 = closed.iter().map(|t| t.invested).sum();
    let gross_profit: f64 = closed.iter().filter(|t| t.pnl > 0.0).map(|t| t.pnl).sum();
    let gross_loss: f64 = closed.iter().filter(|t| t.pnl < 0.0).map(|t| -t.pnl).sum();
    let avg_win = if wins.is_empty() { 0.0 } else { gross_profit / wins.len() as f64 };
    let avg_loss = if losses.is_empty() { 0.0 } else { gross_loss / losses.len() as f64 };
    let avg_days = if closed.is_empty() {
        0.0
    } else {
        closed.iter().map(|t| t.days as f64).sum::<f64>() / closed.len() as f64
    };
    let best = closed.iter().map(|t| t.pnl_pct).fold(0.0, f64::max);
    let worst = closed.iter().map(|t| t.pnl_pct).fold(0.0, f64::min);
    let invested: f64 = open.iter().map(|t| t.invested).sum();
    let value: f64 = open.iter().map(|t| t.last_price.max(0.0) * t.qty as f64).sum();
    let unrealized = value - invested;
    let win_rate = if closed.is_empty() {
        0.0
    } else {
        wins.len() as f64 / closed.len() as f64 * 100.0
    };
    json!({
        "closed": closed.len(),
        "wins": wins.len(),
        "losses": losses.len(),
        "winRate": round2(win_rate),
        "realizedPnl": round2(realized),
        "realizedPnlPct": round2(if closed_cost > 0.0 { realized / closed_cost * 100.0 } else { 0.0 }),
        "unrealizedPnl": round2(unrealized),
        "unrealizedPct": round2(if invested > 0.0 { unrealized / invested * 100.0 } else { 0.0 }),
        "open": open.len(),
        "invested": round2(invested),
        "currentValue": round2(value),
        "avgWinPct": round2(if avg_win > 0.0 { avg_win } else { 0.0 }),
        "avgLossPct": round2(avg_loss),
        "avgDays": round2(avg_days),
        "bestPct": round2(best),
        "worstPct": round2(worst),
        "profitFactor": round2(if gross_loss > 0.0 { gross_profit / gross_loss } else if gross_profit > 0.0 { 99.0 } else { 0.0 }),
        "expectancy": round2(if closed.is_empty() { 0.0 } else { realized / closed.len() as f64 }),
    })
}

fn view(mode: &str) -> Value {
    let st = guard(mode);
    let mut trades = st.trades.clone();
    trades.sort_by(|a, b| {
        b.is_open()
            .cmp(&a.is_open())
            .then(b.opened_at.cmp(&a.opened_at))
    });
    let effective_capital = engine_capital(mode, st.settings.capital);
    let shared_cap = crate::shared_margin::cap_for(if mode == "live" { "equitylive" } else { "equitypaper" });
    // Per-trade charges are recomputed on read so the UI can show exactly what a
    // trade paid. Closed trades already store a NET pnl (charges deducted at
    // close), so netPnl there is just pnl; open trades mark the round trip at the
    // live price, which is the same number Net TP watches.
    let trades: Vec<Value> = trades
        .iter()
        .map(|t| {
            let exit = if t.is_open() { t.last_price } else { t.exit_price };
            let charges = equity_charges(t.entry_price, exit, t.qty);
            let net = if t.is_open() {
                (t.last_price - t.entry_price) * t.qty as f64 - charges
            } else {
                t.pnl
            };
            let mut v = serde_json::to_value(t).unwrap_or_else(|_| json!({}));
            if let Some(o) = v.as_object_mut() {
                o.insert("charges".into(), json!(round2(charges)));
                o.insert("netPnl".into(), json!(round2(net)));
            }
            v
        })
        .collect();
    json!({
        "mode": mode,
        "enabled": st.enabled,
        "settings": st.settings,
        "effectiveCapital": round2(effective_capital),
        "sharedCap": shared_cap.map(round2),
        "stats": trade_stats(&st.trades),
        "trades": trades,
        "log": st.log.iter().rev().take(80).collect::<Vec<_>>(),
        "lastRun": st.last_run,
        "lastError": st.last_error,
        "autoPushPending": st.settings.auto_push_symbols.len(),
    })
}

pub async fn engines(State(st): State<DhanState>) -> Json<Value> {
    Json(json!({
        "brokerConnected": st.is_connected().await,
        "paper": view("paper"),
        "live": view("live"),
        "serverTime": now_secs(),
    }))
}

pub async fn trades(State(_st): State<DhanState>) -> Json<Value> {
    Json(json!({ "paper": view("paper"), "live": view("live") }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToggleReq {
    pub mode: String,
    pub enabled: bool,
}

pub async fn toggle(State(st): State<DhanState>, Json(req): Json<ToggleReq>) -> Json<Value> {
    let Some(mode) = esc_mode(&req.mode) else {
        return Json(json!({"error": "unknown engine"}));
    };
    if req.enabled && mode == "live" && !st.is_connected().await {
        return Json(json!({"error": "Dhan is not connected. Connect the broker before arming LIVE equity orders."}));
    }
    {
        let mut s = guard(mode);
        s.enabled = req.enabled;
        s.last_error.clear();
        if req.enabled {
            log(&mut s, format!("Connected: {} equity engine armed.", mode.to_uppercase()));
        } else {
            log(&mut s, format!("Disconnected: {} equity engine paused. Open positions are untouched.", mode.to_uppercase()));
        }
        save(mode, &s);
    }
    if req.enabled {
        let dhan = st.clone();
        tokio::spawn(async move { run_cycle(&dhan, mode).await });
    }
    Json(json!({"ok": true, "engine": view(mode)}))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsReq {
    pub mode: String,
    pub capital: Option<f64>,
    pub per_trade_pct: Option<f64>,
    pub max_positions: Option<usize>,
    pub min_confidence: Option<f64>,
    pub min_upside_pct: Option<f64>,
    pub max_holding_days: Option<i64>,
    pub auto_exit: Option<bool>,
    pub allow_accumulate: Option<bool>,
    pub watch_symbols: Option<Vec<String>>,
    pub fixed_qty: Option<i64>,
    pub auto_distribute: Option<bool>,
    pub auto_buy: Option<bool>,
    pub charges_aware: Option<bool>,
    pub trail_sl_on: Option<bool>,
    pub trail_sl_pct: Option<f64>,
    pub net_tp_on: Option<bool>,
    pub net_tp_min: Option<f64>,
    pub net_tp_giveback_pct: Option<f64>,
    pub daily_exit_on: Option<bool>,
    pub daily_exit_time: Option<String>,
    /// `true` empties the auto-push pending list (UI "Clear" button).
    pub clear_auto_push: Option<bool>,
}

pub async fn settings(Json(req): Json<SettingsReq>) -> Json<Value> {
    let Some(mode) = esc_mode(&req.mode) else {
        return Json(json!({"error": "unknown engine"}));
    };
    let mut s = guard(mode);
    let before = serde_json::to_value(&s.settings).unwrap_or(Value::Null);
    if let Some(v) = req.capital {
        s.settings.capital = v.clamp(10_000.0, 100_000_000.0);
    }
    if let Some(v) = req.per_trade_pct {
        s.settings.per_trade_pct = v.clamp(1.0, 50.0);
    }
    if let Some(v) = req.max_positions {
        s.settings.max_positions = v.clamp(1, 50);
    }
    if let Some(v) = req.min_confidence {
        s.settings.min_confidence = v.clamp(0.0, 100.0);
    }
    if let Some(v) = req.min_upside_pct {
        s.settings.min_upside_pct = v.clamp(0.0, 300.0);
    }
    if let Some(v) = req.max_holding_days {
        s.settings.max_holding_days = v.clamp(1, 1_000);
    }
    if let Some(v) = req.auto_exit {
        s.settings.auto_exit = v;
    }
    if let Some(v) = req.allow_accumulate {
        s.settings.allow_accumulate = v;
    }
    if let Some(list) = req.watch_symbols {
        let mut seen = HashSet::new();
        s.settings.watch_symbols = list
            .into_iter()
            .map(|x| x.trim().to_uppercase())
            .filter(|x| !x.is_empty() && seen.insert(x.clone()))
            .take(200)
            .collect();
    }
    if let Some(v) = req.fixed_qty {
        s.settings.fixed_qty = v.clamp(0, 1_000_000);
    }
    if let Some(v) = req.auto_distribute {
        s.settings.auto_distribute = v;
    }
    if let Some(v) = req.auto_buy {
        s.settings.auto_buy = v;
    }
    if let Some(v) = req.charges_aware {
        s.settings.charges_aware = v;
    }
    if let Some(v) = req.trail_sl_on {
        s.settings.trail_sl_on = v;
    }
    if let Some(v) = req.trail_sl_pct {
        s.settings.trail_sl_pct = v.clamp(0.0, 95.0);
    }
    if let Some(v) = req.net_tp_on {
        s.settings.net_tp_on = v;
    }
    if let Some(v) = req.net_tp_min {
        s.settings.net_tp_min = v.clamp(0.0, 100_000_000.0);
    }
    if let Some(v) = req.net_tp_giveback_pct {
        s.settings.net_tp_giveback_pct = v.clamp(0.0, 99.0);
    }
    if let Some(v) = req.daily_exit_on {
        s.settings.daily_exit_on = v;
        // Re-arm on every toggle so switching the feature on fires today.
        if v {
            s.settings.daily_exit_day = 0;
        }
    }
    if let Some(t) = req.daily_exit_time {
        if let Some(mins) = parse_hhmm(&t) {
            s.settings.daily_exit_time = format!("{:02}:{:02}", mins / 60, mins % 60);
            // A time change must be able to run again the same day.
            s.settings.daily_exit_day = 0;
        }
    }
    if let Some(true) = req.clear_auto_push {
        s.settings.auto_push_symbols.clear();
    }
    // Only journal a real change: the UI autosaves on every field edit, so an
    // unconditional log line would flood the 300-entry activity log.
    let after = serde_json::to_value(&s.settings).unwrap_or(Value::Null);
    if after != before {
        log(&mut s, "Settings updated.");
        save(mode, &s);
    }
    drop(s);
    Json(json!({"ok": true, "engine": view(mode)}))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookReq {
    pub mode: String,
}

/// Reset the PAPER book's realised P&L: drops the closed trades that make up the
/// realised figure while leaving every open position untouched. Paper only -
/// live realised P&L is a real-money audit trail and must not be erasable.
pub async fn reset_pnl(Json(req): Json<BookReq>) -> Json<Value> {
    let Some(mode) = esc_mode(&req.mode) else {
        return Json(json!({"error": "unknown engine"}));
    };
    if mode == "live" {
        return Json(json!({"error": "LIVE realised P&L is a real-money record and cannot be reset"}));
    }
    let mut s = guard(mode);
    let before = s.trades.len();
    s.trades.retain(|t| t.is_open());
    let removed = before - s.trades.len();
    log(&mut s, format!("Realised P&L reset - {removed} closed trade(s) removed, open positions kept."));
    save(mode, &s);
    drop(s);
    Json(json!({"ok": true, "removed": removed, "engine": view(mode)}))
}

/// Clear every stock the PAPER book has added (open + closed) and reset the
/// realised P&L with it. Paper only - it must never silently drop real orders.
pub async fn clear_book(Json(req): Json<BookReq>) -> Json<Value> {
    let Some(mode) = esc_mode(&req.mode) else {
        return Json(json!({"error": "unknown engine"}));
    };
    if mode == "live" {
        return Json(json!({"error": "LIVE book cannot be cleared - exit real positions from the broker"}));
    }
    let mut s = guard(mode);
    let removed = s.trades.len();
    s.trades.clear();
    log(&mut s, format!("Book cleared - {removed} trade(s) removed, realised P&L reset."));
    save(mode, &s);
    drop(s);
    Json(json!({"ok": true, "removed": removed, "engine": view(mode)}))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionReq {
    pub mode: String,
    pub id: Option<String>,
    pub symbol: Option<String>,
    pub qty: Option<i64>,
}

pub async fn exit_trade(State(st): State<DhanState>, Json(req): Json<ActionReq>) -> Json<Value> {
    let Some(mode) = esc_mode(&req.mode) else {
        return Json(json!({"error": "unknown engine"}));
    };
    let Some(id) = req.id.as_deref() else {
        return Json(json!({"error": "trade id required"}));
    };
    let trade = {
        let s = guard(mode);
        s.trades.iter().find(|t| t.id == id && t.is_open()).cloned()
    };
    let Some(trade) = trade else {
        return Json(json!({"error": "open trade not found"}));
    };
    let px = price_for(&st, &trade.symbol, &trade.exchange, trade.last_price).await;
    match close_trade(&st, mode, &trade, px, "Manual exit").await {
        Ok(msg) => Json(json!({"ok": true, "message": msg, "engine": view(mode)})),
        Err(e) => Json(json!({"error": e})),
    }
}

pub async fn enter_trade(State(st): State<DhanState>, Json(req): Json<ActionReq>) -> Json<Value> {
    let Some(mode) = esc_mode(&req.mode) else {
        return Json(json!({"error": "unknown engine"}));
    };
    let Some(symbol) = req.symbol.as_deref() else {
        return Json(json!({"error": "symbol required"}));
    };
    let (as_of, rows) = equity::engine_snapshot();
    let Some(row) = rows
        .iter()
        .find(|r| r.get("symbol").and_then(|x| x.as_str()) == Some(symbol))
        .cloned()
    else {
        return Json(json!({"error": "symbol is not in the current equity screen"}));
    };
    let res = enter_one(&st, mode, &row, as_of.as_str(), true, req.qty).await;
    match res {
        Ok(msg) => Json(json!({"ok": true, "message": msg, "engine": view(mode)})),
        Err(e) => Json(json!({"error": e})),
    }
}

fn call_of(row: &Value) -> &Value {
    row.get("call").unwrap_or(&Value::Null)
}

fn tradable(row: &Value, s: &Settings) -> Option<(String, f64, f64, f64, f64)> {
    let call = call_of(row);
    let status = call.get("status").and_then(|x| x.as_str()).unwrap_or("");
    let ok = match status {
        "Strong Buy" | "Buy" => true,
        "Accumulate" => s.allow_accumulate,
        _ => false,
    };
    if !ok {
        return None;
    }
    let conf = num(call.get("confidence"));
    let upside = num(call.get("upsidePct"));
    if conf < s.min_confidence || upside < s.min_upside_pct {
        return None;
    }
    let px = row_price(row);
    let high = num(call.get("entryHigh"));
    if px <= 0.0 {
        return None;
    }
    if high > 0.0 && px > high * 1.03 {
        return None;
    }
    let target = num(call.get("targetPrice"));
    let stop = num(call.get("stopPrice"));
    Some((status.to_string(), conf, target, stop, px))
}

fn quote_ltp(q: &HashMap<String, Value>, sid: i64, exch: &str) -> Option<f64> {
    q.get(&crate::market::quote_key(sid, exch))
        .and_then(|v| v.get("ltp"))
        .and_then(|v| v.as_f64())
        .filter(|p| *p > 0.0)
}

async fn price_for(st: &DhanState, symbol: &str, exchange: &str, fallback: f64) -> f64 {
    if let Some(sid) = scrip::get().and_then(|s| s.equity_sid(symbol)) {
        if let Some(q) = st.fetch_rest_quotes(&[(sid, "NSE_EQ".into())]).await {
            if let Some(px) = quote_ltp(&q, sid, "NSE_EQ") {
                return px;
            }
        }
    }
    let yahoo = equity::yahoo_live_prices(&[(symbol.to_string(), exchange.to_string())]).await;
    if let Some(px) = yahoo.get(symbol).copied().filter(|p| *p > 0.0) {
        return px;
    }
    fallback
}

async fn broker_ids(st: &DhanState) -> Option<(dhan_hq::DhanClient, String)> {
    Some((st.session_client().await?, st.session_client_id().await?))
}

async fn place_market(
    client: &dhan_hq::DhanClient,
    client_id: &str,
    sid: i64,
    side: TransactionType,
    qty: i64,
    mode: &str,
    symbol: &str,
) -> Result<(String, f64), String> {
    let req = OrderRequest {
        dhan_client_id: client_id.to_string(),
        correlation_id: Some(format!("EQ{mode}{}{}", now_secs() % 1_000_000, symbol)
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .take(25)
            .collect()),
        transaction_type: side.clone(),
        exchange_segment: ExchangeSegment::NseEq,
        product_type: ProductType::Cnc,
        order_type: OrderType::Market,
        validity: Validity::Day,
        security_id: sid.to_string(),
        quantity: qty,
        disclosed_quantity: None,
        price: None,
        trigger_price: None,
        after_market_order: None,
        amo_time: None,
        bo_profit_value: None,
        bo_stop_loss_value: None,
    };
    let resp = client.place_order(&req).await.map_err(|e| e.to_string())?;
    let mut fill = 0.0;
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(650)).await;
        match client.trades_for_order(&resp.order_id).await {
            Ok(list) => {
                let mut q = 0i64;
                let mut notional = 0.0;
                for t in list {
                    if t.traded_quantity > 0 && t.traded_price > 0.0 {
                        q += t.traded_quantity;
                        notional += t.traded_price * t.traded_quantity as f64;
                    }
                }
                if q > 0 {
                    fill = notional / q as f64;
                    break;
                }
            }
            Err(_) => {}
        }
    }
    Ok((resp.order_id, fill))
}

async fn close_trade(
    st: &DhanState,
    mode: &str,
    trade: &Trade,
    px: f64,
    reason: &str,
) -> Result<String, String> {
    let mut fill = px;
    let mut order_id = String::new();
    if mode == "live" {
        let (client, cid) = broker_ids(st).await.ok_or("Dhan disconnected - cannot place exit order")?;
        let sid = scrip::get()
            .and_then(|s| s.equity_sid(&trade.symbol))
            .ok_or("NSE security id unavailable for this symbol")?;
        st.dhan_order_throttle().await;
        let (oid, fpx) = place_market(&client, &cid, sid, TransactionType::Sell, trade.qty, mode, &trade.symbol).await?;
        order_id = oid;
        if fpx > 0.0 {
            fill = fpx;
        }
    }
    if fill <= 0.0 {
        return Err("no usable exit price".into());
    }
    let mut s = guard(mode);
    let Some(t) = s.trades.iter_mut().find(|t| t.id == trade.id) else {
        return Err("trade disappeared".into());
    };
    if !t.is_open() {
        return Err("trade is already closed".into());
    }
    t.exit_price = round2(fill);
    t.exited_at = now_secs();
    t.exit_date = ymd(today_ist());
    t.exit_reason = reason.to_string();
    // Book the NET result (gross move minus the real round-trip delivery
    // charges). Recording the gross number was why the paper book looked like a
    // profit while it was actually losing money to charges.
    let gross = (fill - t.entry_price) * t.qty as f64;
    let charges = equity_charges(t.entry_price, fill, t.qty);
    t.pnl = round2(gross - charges);
    t.net_now = t.pnl;
    t.pnl_pct = round2(if t.entry_price > 0.0 {
        (t.pnl / (t.entry_price * t.qty as f64)) * 100.0
    } else {
        0.0
    });
    t.days = (today_ist() - (t.opened_at + 19_800) / 86_400).max(0);
    if !order_id.is_empty() {
        t.order_id = format!("{} / exit {order_id}", t.order_id);
    }
    t.note = reason.to_string();
    let msg = format!("{} closed at {:.2} ({}) P&L {:.0}", t.symbol, fill, reason, t.pnl);
    log(&mut s, msg.clone());
    save(mode, &s);
    Ok(msg)
}

async fn enter_one(
    st: &DhanState,
    mode: &str,
    row: &Value,
    _as_of: &str,
    manual: bool,
    qty_override: Option<i64>,
) -> Result<String, String> {
    let (symbol, name, exch, status, conf, target, stop, row_px, settings, open, slots, remaining, capital, pushed) = {
        let s = guard(mode);
        let symbol = row.get("symbol").and_then(|x| x.as_str()).unwrap_or("").to_string();
        if symbol.is_empty() {
            return Err("row has no symbol".into());
        }
        if !manual && s.trades.iter().any(|t| t.is_open() && t.symbol == symbol) {
            return Err("already in an open position".into());
        }
        if manual && s.trades.iter().any(|t| t.is_open() && t.symbol == symbol) {
            return Err("already in an open position".into());
        }
        // A name pushed by Equity Research is an explicit operator instruction:
        // it skips the watch list and the research-call filters, and only needs a
        // free slot + shared margin.
        let pushed = !manual && s.settings.auto_push_symbols.iter().any(|w| w == &symbol);
        if !manual && !pushed {
            if !s.settings.watch_symbols.is_empty()
                && !s.settings.watch_symbols.iter().any(|w| w == &symbol)
            {
                return Err("not in the selected-symbols list".into());
            }
        }
        let (status, conf, target, stop, px) = if manual {
            let call = call_of(row);
            let status = call
                .get("status")
                .and_then(|x| x.as_str())
                .filter(|x| !x.is_empty())
                .unwrap_or("Manual")
                .to_string();
            (
                status,
                num(call.get("confidence")),
                num(call.get("targetPrice")),
                num(call.get("stopPrice")),
                row_price(row),
            )
        } else if pushed {
            // Rank/pricing still come from the row; only the entry filters are
            // waived. A row without a call uses "Auto (sent by Equity Research)"
            // as the status so the book shows why it was bought.
            let call = call_of(row);
            let status = call
                .get("status")
                .and_then(|x| x.as_str())
                .filter(|x| !x.is_empty())
                .unwrap_or("Auto (Equity Research)")
                .to_string();
            (
                status,
                num(call.get("confidence")),
                num(call.get("targetPrice")),
                num(call.get("stopPrice")),
                row_price(row),
            )
        } else {
            let Some(v) = tradable(row, &s.settings) else {
                return Err("does not pass the current entry rules".into());
            };
            v
        };
        let open = s.trades.iter().filter(|t| t.is_open()).count();
        let slots = s.settings.max_positions.saturating_sub(open);
        let invested: f64 = s.trades.iter().filter(|t| t.is_open()).map(|t| t.invested).sum();
        let capital = engine_capital(mode, s.settings.capital);
        let remaining = (capital - invested).max(0.0);
        let name = row.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let exch = row.get("exchange").and_then(|x| x.as_str()).unwrap_or("NSE").to_string();
        (symbol, name, exch, status, conf, target, stop, px, s.settings.clone(), open, slots, remaining, capital, pushed)
    };
    if manual {
        if open >= settings.max_positions {
            return Err("position limit reached".into());
        }
        if remaining < 1_000.0 {
            return Err("no shared margin left".into());
        }
    } else if slots == 0 || remaining < 1_000.0 {
        return Err("no free slot or shared margin".into());
    }
    // Both paths size off the shared allotment: auto-distribute splits it across
    // the free slots, otherwise the operator's per-trade % of it is used.
    let alloc = if settings.auto_distribute {
        auto_distribute_alloc(capital, &settings, remaining, slots)
    } else {
        (capital * settings.per_trade_pct / 100.0)
            .min(remaining)
            .max(0.0)
    };
    // Never let one name exceed the per-name cap: this is the difference between
    // a single bad stop costing a slice of a slot and it costing half the book.
    let name_cap = if settings.max_position_pct > 0.0 {
        (capital * settings.max_position_pct / 100.0).max(0.0)
    } else {
        alloc
    };
    let alloc = alloc.min(name_cap).max(0.0);
    let px = price_for(st, &symbol, &exch, row_px).await;
    if px <= 0.0 {
        return Err("no usable entry price".into());
    }
    let fixed_qty = if settings.auto_distribute { 0 } else { settings.fixed_qty };
    // Reserve the round-trip charges out of the slot budget so the shares bought
    // plus their eventual exit costs still fit the allotment.
    let budget = charges_aware_budget(alloc, settings.charges_aware);
    let qty = choose_qty(budget, px, fixed_qty, qty_override);
    if qty < 1 {
        return Err("allocation too small for one share".into());
    }
    // Automatic entries must not pick a name whose target profit is smaller than
    // its own broker charges; a manual override - or an explicit push from
    // Equity Research - is the operator's call.
    if !manual && !pushed {
        let guard_target = if target > px { target } else { round2(px * 1.18) };
        charges_leave_room(px, guard_target, qty, settings.charges_aware)?;
    }
    if qty as f64 * px > remaining + 1.0 {
        return Err(format!(
            "quantity {} needs Rs {:.0} but only Rs {:.0} shared margin is free",
            qty,
            qty as f64 * px,
            remaining
        ));
    }
    let mut entry_order = String::new();
    let mut fill = px;
    if mode == "live" {
        let (client, cid) = broker_ids(st).await.ok_or("Dhan disconnected")?;
        let sid = scrip::get()
            .and_then(|s| s.equity_sid(&symbol))
            .ok_or("NSE security id unavailable (BSE-only symbols cannot be ordered here)")?;
        st.dhan_order_throttle().await;
        let (oid, fpx) = place_market(&client, &cid, sid, TransactionType::Buy, qty, mode, &symbol).await?;
        entry_order = oid;
        if fpx > 0.0 {
            fill = fpx;
        }
    }
    if fill <= 0.0 {
        fill = px;
    }
    let target = if target > fill { target } else { round2(fill * 1.18) };
    let stop = if stop > 0.0 && stop < fill { stop } else { round2(fill * 0.93) };
    let trade = Trade {
        id: format!("eq{}-{}-{}", mode, now_secs(), symbol),
        mode: mode.to_string(),
        symbol: symbol.clone(),
        name,
        exchange: exch,
        status: status.clone(),
        confidence: conf,
        entry_price: round2(fill),
        qty,
        invested: round2(fill * qty as f64),
        target: round2(target),
        stop: round2(stop),
        last_price: round2(fill),
        peak_price: round2(fill),
        opened_at: now_secs(),
        opened_date: ymd(today_ist()),
        order_id: entry_order,
        note: if mode == "live" { "LIVE CNC delivery order".into() } else { "Paper fill from live/cached price".into() },
        ..Default::default()
    };
    let mut s = guard(mode);
    if s.trades.iter().any(|t| t.is_open() && t.symbol == symbol) {
        return Err("already in an open position".into());
    }
    if s.trades.iter().filter(|t| t.is_open()).count() >= s.settings.max_positions {
        return Err("position limit reached".into());
    }
    // Re-check the shared margin against the book as it is NOW (the sizing above
    // was computed before the price lookup / broker call, a window in which the
    // allotment can shrink). Without this a settings change mid-cycle could
    // over-deploy the pool.
    let live_cap = engine_capital(mode, s.settings.capital);
    let live_invested: f64 = s.trades.iter().filter(|t| t.is_open()).map(|t| t.invested).sum();
    if trade.invested > (live_cap - live_invested).max(0.0) + 1.0 {
        return Err(format!(
            "entry needs Rs {:.0} but only Rs {:.0} shared margin is free",
            trade.invested,
            (live_cap - live_invested).max(0.0)
        ));
    }
    let msg = format!(
        "{} {} qty {} @ {:.2} (target {:.2}, stop {:.2})",
        if mode == "live" { "LIVE BUY" } else { "Paper BUY" },
        symbol,
        qty,
        fill,
        trade.target,
        trade.stop
    );
    s.trades.push(trade);
    log(&mut s, msg.clone());
    save(mode, &s);
    Ok(msg)
}


/// Should this row become an entry candidate, and with what confidence?
///
/// Auto-pushed names are bought on slot + margin alone: they skip the Buy call,
/// confidence / upside / price-band gates and the watch list, because the
/// operator explicitly asked Equity Research to send them. Everything else still
/// has to pass the normal entry rules. Kept pure so both behaviours are
/// unit-tested without a running engine.
fn candidate_confidence(sym: &str, row: &Value, settings: &Settings) -> Option<f64> {
    let pushed = settings.auto_push_symbols.iter().any(|w| w == sym);
    if pushed {
        return Some(num(call_of(row).get("confidence")));
    }
    if !settings.watch_symbols.is_empty() && !settings.watch_symbols.iter().any(|w| w == sym) {
        return None;
    }
    tradable(row, settings).map(|(_, conf, _, _, _)| conf)
}

async fn run_cycle(st: &DhanState, mode: &str) {
    let flag = if mode == "live" { &LIVE_RUNNING } else { &PAPER_RUNNING };
    if flag.swap(true, Ordering::SeqCst) {
        return;
    }
    run_cycle_inner(st, mode).await;
    flag.store(false, Ordering::SeqCst);
}

async fn run_cycle_inner(st: &DhanState, mode: &str) {
    let enabled = guard(mode).enabled;
    if !enabled {
        return;
    }
    if mode == "live" && !st.is_connected().await {
        let mut s = guard(mode);
        s.enabled = false;
        s.last_error = "Dhan disconnected - LIVE engine auto-paused. Re-arm after reconnecting.".into();
        let msg = s.last_error.clone();
        log(&mut s, msg);
        save(mode, &s);
        return;
    }

    let (as_of, rows) = equity::engine_snapshot();
    let row_map: HashMap<String, Value> = rows
        .iter()
        .filter_map(|r| {
            r.get("symbol")
                .and_then(|x| x.as_str())
                .map(|s| (s.to_string(), r.clone()))
        })
        .collect();

    let opens: Vec<Trade> = {
        let s = guard(mode);
        s.trades.iter().filter(|t| t.is_open()).cloned().collect()
    };

    let mut quote_map: HashMap<String, f64> = HashMap::new();
    let mut ids: Vec<(i64, String)> = Vec::new();
    let mut sym_for_sid: HashMap<i64, String> = HashMap::new();
    if let Some(sc) = scrip::get() {
        let mut seen = HashSet::new();
        for t in &opens {
            if seen.insert(t.symbol.clone()) {
                if let Some(sid) = sc.equity_sid(&t.symbol) {
                    ids.push((sid, "NSE_EQ".into()));
                    sym_for_sid.insert(sid, t.symbol.clone());
                }
            }
        }
    }
    if !ids.is_empty() {
        if let Some(q) = st.fetch_rest_quotes(&ids).await {
            for (sid, sym) in &sym_for_sid {
                if let Some(px) = quote_ltp(&q, *sid, "NSE_EQ") {
                    quote_map.insert(sym.clone(), px);
                }
            }
        }
    }
    // Dhan REST is frequently rate-limited (429) while option chains are being
    // polled. Without a second source the paper book would mark every position
    // at its entry price and report P&L 0 forever, so fill the gaps from Yahoo.
    let missing: Vec<(String, String)> = opens
        .iter()
        .filter(|t| !quote_map.contains_key(&t.symbol))
        .map(|t| (t.symbol.clone(), t.exchange.clone()))
        .collect();
    if !missing.is_empty() {
        for (symbol, px) in equity::yahoo_live_prices(&missing).await {
            quote_map.insert(symbol, px);
        }
    }

    {
        let mut s = guard(mode);
        let settings = s.settings.clone();
        for t in s.trades.iter_mut().filter(|t| t.is_open()) {
            let px = quote_map
                .get(&t.symbol)
                .copied()
                .filter(|p| *p > 0.0)
                .or_else(|| row_map.get(&t.symbol).map(row_price).filter(|p| *p > 0.0))
                .unwrap_or(t.last_price);
            if px > 0.0 {
                t.last_price = round2(px);
                if px > t.peak_price {
                    t.peak_price = round2(px);
                }
                // Percentage trail + Net TP bookkeeping runs on every mark, so
                // the persisted stop and net peak are always current before the
                // exit checks below read them.
                if settings.trail_sl_on && t.entry_price > 0.0 {
                    let peak = (t.peak_price - t.entry_price).max(0.0);
                    let stop = equity_trail_stop(t.entry_price, peak, settings.trail_sl_pct);
                    // Only arm once the trailing level has climbed ABOVE the
                    // charge break-even. A trail that arms below it exits at a
                    // net loss on a tiny wiggle (the 521 same-day churn trades);
                    // until it clears costs the fixed stop is the only risk exit.
                    let be = equity_breakeven_price(t.entry_price, t.qty);
                    if stop >= be && stop > t.trail_stop {
                        t.trail_stop = round2(stop);
                    }
                }
                // Net P&L is always computed so the UI can show it even when
                // Net TP is off; only the peak tracking is gated by the switch.
                let net = round2(
                    (px - t.entry_price) * t.qty as f64 - equity_charges(t.entry_price, px, t.qty),
                );
                t.net_now = net;
                if settings.net_tp_on && net > t.net_peak {
                    t.net_peak = net;
                }
            }
        }
        s.last_run = now_secs();
        save(mode, &s);
    }

    let price_of = |sym: &str| -> f64 {
        quote_map
            .get(sym)
            .copied()
            .filter(|p| *p > 0.0)
            .or_else(|| row_map.get(sym).map(row_price).filter(|p| *p > 0.0))
            .unwrap_or(0.0)
    };

    let open_now: Vec<Trade> = {
        let s = guard(mode);
        s.trades.iter().filter(|t| t.is_open()).cloned().collect()
    };
    let settings = guard(mode).settings.clone();
    // Daily square-off: sell every position that was NOT opened today (CNC
    // delivery shares are not sellable on the buy date), leaving the freed
    // margin to the buy loop further down this same cycle. It fires once per
    // IST day, only inside the cash session, so a failed/blocked sell retries
    // on the next 45-second tick instead of being silently skipped.
    let daily_due = daily_exit_due(
        now_secs(),
        settings.daily_exit_on,
        &settings.daily_exit_time,
        settings.daily_exit_day,
        matches!(ist_weekday_minutes(now_secs()), Some(m) if (9 * 60 + 15..=15 * 60 + 30).contains(&m)),
    );
    let today = ymd(today_ist());
    for t in open_now {
        let px = price_of(&t.symbol);
        if px <= 0.0 {
            continue;
        }
        let call = row_map.get(&t.symbol).map(call_of);
        let status = call
            .and_then(|c| c.get("status"))
            .and_then(|x| x.as_str())
            .unwrap_or("");
        let days = (today_ist() - (t.opened_at + 19_800) / 86_400).max(0);
        // Net Take-Profit: only fires once the net peak has crossed the rupee
        // floor AND the live net profit has given back the configured percent of
        // that peak. The floor is recomputed here (not cached) so a settings
        // change takes effect on the very next mark.
        let net_now = round2(
            (px - t.entry_price) * t.qty as f64 - equity_charges(t.entry_price, px, t.qty),
        );
        let net_peak = t.net_peak.max(net_now);
        let hit_net_tp = settings.net_tp_on
            && equity_net_tp_floor(net_peak, settings.net_tp_min, settings.net_tp_giveback_pct)
                .map(|floor| net_now < floor)
                .unwrap_or(false);
        let hit_trail = t.trail_stop > 0.0 && px <= t.trail_stop;
        let reason = if daily_due && t.opened_date != today {
            Some("Daily square-off (scheduled)")
        } else if px <= t.stop {
            Some("Stop loss hit")
        } else if px >= t.target {
            Some("Target achieved")
        } else if hit_trail {
            Some("Trailing stop hit")
        } else if hit_net_tp {
            Some("Net take-profit")
        } else if settings.auto_exit && status == "Avoid" {
            Some("Call downgraded to Avoid")
        } else if settings.max_holding_days > 0 && days >= settings.max_holding_days {
            Some("Max holding period reached")
        } else {
            None
        };
        if let Some(reason) = reason {
            if let Err(e) = close_trade(st, mode, &t, px, reason).await {
                let mut s = guard(mode);
                s.last_error = e.clone();
                log(&mut s, format!("Exit failed for {}: {e}", t.symbol));
                save(mode, &s);
            }
        }
    }

    // Mark the day done only once every sellable position is actually closed.
    // A position whose sell failed (or has no price) keeps the day unmarked, so
    // the next tick retries instead of leaving a stale holding overnight.
    if daily_due {
        let mut s = guard(mode);
        let left = s.trades.iter().filter(|t| t.is_open() && t.opened_date != today).count();
        if left == 0 {
            s.settings.daily_exit_day = today_ist();
            log(&mut s, format!("Daily square-off complete for {today}."));
            save(mode, &s);
        }
    }

    // Names sold by TODAY's daily square-off are excluded for the rest of the
    // day: rebuying them minutes later would just pay the delivery + DP charges
    // twice for the same holding. They are fully eligible again tomorrow, so a
    // good stock genuinely can be bought, sold and re-bought over the days.
    let sold_today: HashSet<String> = {
        let s = guard(mode);
        s.trades
            .iter()
            .filter(|t| {
                !t.is_open()
                    && t.exit_date == today
                    && t.exit_reason.starts_with("Daily square-off")
            })
            .map(|t| t.symbol.clone())
            .collect()
    };

    let mut candidates: Vec<(f64, f64, String, Value)> = row_map
        .iter()
        .filter_map(|(sym, row)| {
            if sold_today.contains(sym) {
                return None;
            }
            let conf = candidate_confidence(sym, row, &settings)?;
            // Rank by EXPECTED RETURN (upside to target) first, so the engine
            // always buys the highest-possible-return names before the rest;
            // buy-confidence breaks ties.
            let ret = equity::row_expected_return(row);
            Some((ret, conf, sym.clone(), row.clone()))
        })
        .collect();
    order_candidates(&mut candidates);

    // Auto Buy Mode gates NEW entries only: position management above (marks,
    // trail, target/stop exits) keeps running while it is OFF.
    if !settings.auto_buy {
        let mut s = guard(mode);
        s.last_run = now_secs();
        save(mode, &s);
        return;
    }

    for (_, _, sym, row) in candidates {
        let (slots, remaining, already) = {
            let s = guard(mode);
            let open = s.trades.iter().filter(|t| t.is_open()).count();
            let already = s.trades.iter().any(|t| t.is_open() && t.symbol == sym);
            let invested: f64 = s.trades.iter().filter(|t| t.is_open()).map(|t| t.invested).sum();
            (
                s.settings.max_positions.saturating_sub(open),
                (engine_capital(mode, s.settings.capital) - invested).max(0.0),
                already,
            )
        };
        match candidate_step(already, slots, remaining) {
            // A held symbol is only a reason to SKIP that name, never to stop the
            // whole cycle. Breaking here was why the engine stopped buying as
            // soon as the best-ranked candidate happened to be an open position.
            CandidateStep::Skip => continue,
            CandidateStep::Stop => break,
            CandidateStep::Go => {}
        }
        if !guard(mode).enabled {
            break;
        }
        let was_pushed = guard(mode).settings.auto_push_symbols.iter().any(|w| w == &sym);
        match enter_one(st, mode, &row, as_of.as_str(), false, None).await {
            Ok(_) => {
                if was_pushed {
                    // Bought: stop retrying this name. Anything not bought (no
                    // slot / no margin this cycle) stays pending on purpose.
                    let mut s = guard(mode);
                    s.settings.auto_push_symbols.retain(|w| w != &sym);
                    save(mode, &s);
                }
            }
            Err(_) => {}
        }
    }

    {
        let mut s = guard(mode);
        s.last_run = now_secs();
        save(mode, &s);
    }
}

/// Maximum number of names a book keeps queued for auto-buy. Bounds both the
/// state file and the number of orders a single cycle can attempt.
const PENDING_CAP: usize = 200;

/// Append the incoming names that are not already pending, preserving order and
/// never exceeding `cap`. Pure so the dedupe / cap behaviour is unit-tested.
fn merge_pending(existing: &[String], incoming: &[String], cap: usize) -> Vec<String> {
    let mut out: Vec<String> = existing.to_vec();
    for sym in incoming {
        if out.len() >= cap {
            break;
        }
        if !out.iter().any(|x| x == sym) {
            out.push(sym.clone());
        }
    }
    out
}

/// Hand freshly scanned symbols to BOTH equity books.
///
/// Returns `(added_paper, added_live)`: only names that were not already
/// pending are appended, so a re-scan of the same screen adds nothing. The list
/// is persisted with the engine state, so a server restart keeps the pending
/// push instead of silently dropping it.
pub fn accept_pushed_symbols(symbols: &[String]) -> (usize, usize) {
    let clean: Vec<String> = {
        let mut seen = HashSet::new();
        symbols
            .iter()
            .map(|x| x.trim().to_uppercase())
            .filter(|x| !x.is_empty() && seen.insert(x.clone()))
            .collect()
    };
    if clean.is_empty() {
        return (0, 0);
    }
    let mut out = (0, 0);
    for (i, mode) in MODES.iter().enumerate() {
        let mut s = guard(mode);
        let merged = merge_pending(&s.settings.auto_push_symbols, &clean, PENDING_CAP);
        let added = merged.len().saturating_sub(s.settings.auto_push_symbols.len());
        s.settings.auto_push_symbols = merged;
        if added > 0 {
            log(
                &mut s,
                format!(
                    "Equity Research ne {added} naya stock bheja (auto send) - slot + margin mile to buy hoga."
                ),
            );
            save(mode, &s);
        }
        if i == 0 {
            out.0 = added;
        } else {
            out.1 = added;
        }
    }
    out
}

/// Compact per-mode equity state for the diagnostics snapshot: whether the book
/// is armed, how many positions are open, how many pushed names are still
/// waiting, and the daily square-off configuration. Read-only.
pub fn diagnostic_block(mode: &str) -> Value {
    let Some(mode) = esc_mode(mode) else {
        return json!({"error": "unknown engine"});
    };
    let s = guard(mode);
    let open = s.trades.iter().filter(|t| t.is_open()).count();
    let invested: f64 = s.trades.iter().filter(|t| t.is_open()).map(|t| t.invested).sum();
    let empty_rows = s.settings.auto_push_symbols.len();
    json!({
        "mode": mode,
        "enabled": s.enabled,
        "openPositions": open,
        "closedTrades": s.trades.len().saturating_sub(open),
        "invested": round2(invested),
        "capital": round2(s.settings.capital),
        "effectiveCapital": round2(engine_capital(mode, s.settings.capital)),
        "autoBuy": s.settings.auto_buy,
        "autoPush": !s.settings.auto_push_symbols.is_empty(),
        "pendingPush": empty_rows,
        "dailyExitOn": s.settings.daily_exit_on,
        "dailyExitTime": s.settings.daily_exit_time,
        "dailyExitDay": s.settings.daily_exit_day,
        "lastRun": s.last_run,
        "lastError": s.last_error,
    })
}

/// Count of symbols still waiting for a free slot / margin, for the UI badge.
pub fn pending_push_count(mode: &str) -> usize {
    guard(mode).settings.auto_push_symbols.len()
}

/// Run one buy cycle right now for every armed engine (used after a push so the
/// new names are considered immediately instead of waiting for the 45s tick).
pub async fn run_armed_now(st: &DhanState) {
    for mode in MODES {
        if guard(mode).enabled {
            run_cycle(st, mode).await;
        }
    }
}

pub fn spawn_engine(st: DhanState) {
    let _ = state("paper");
    let _ = state("live");
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(75)).await;
        loop {
            for mode in MODES {
                if guard(mode).enabled {
                    run_cycle(&st, mode).await;
                }
            }
            tokio::time::sleep(Duration::from_secs(45)).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_symbol_is_skipped_without_stopping_the_cycle() {
        // Regression: the loop used to `break` on an already-held symbol, so a
        // full book (or a held top-ranked name) froze every later candidate.
        assert_eq!(candidate_step(true, 5, 500_000.0), CandidateStep::Skip);
        assert_eq!(candidate_step(true, 0, 0.0), CandidateStep::Skip);
        // Genuinely full book / exhausted margin still stops.
        assert_eq!(candidate_step(false, 0, 500_000.0), CandidateStep::Stop);
        assert_eq!(candidate_step(false, 3, 999.0), CandidateStep::Stop);
        // Free slot + margin enters.
        assert_eq!(candidate_step(false, 3, 1_000.0), CandidateStep::Go);
    }

    fn push_row(status: &str, conf: f64) -> Value {
        json!({
            "symbol": "NEWCO",
            "price": 100.0,
            "call": { "status": status, "confidence": conf, "upsidePct": 2.0, "targetPrice": 102.0, "stopPrice": 95.0 }
        })
    }

    #[test]
    fn a_pushed_symbol_skips_the_buy_call_and_confidence_gates() {
        // This is the whole point of the feature: a name Equity Research sent is
        // bought on slot + margin alone. With the normal rules an "Avoid" call at
        // 5% confidence and only 2% upside would be rejected three ways.
        let mut s = Settings::default();
        s.min_confidence = 70.0;
        s.min_upside_pct = 12.0;
        let row = push_row("Avoid", 5.0);
        assert_eq!(candidate_confidence("NEWCO", &row, &s), None, "normal gate rejects it");

        s.auto_push_symbols = vec!["NEWCO".into()];
        assert!(
            candidate_confidence("NEWCO", &row, &s).is_some(),
            "a pushed name must be a candidate regardless of the call"
        );
    }

    #[test]
    fn a_pushed_symbol_skips_the_watch_list() {
        // "Only these symbols" must not silently drop an explicitly pushed name.
        let mut s = Settings::default();
        s.watch_symbols = vec!["ITC".into()];
        let row = push_row("Strong Buy", 90.0);
        assert_eq!(candidate_confidence("NEWCO", &row, &s), None);
        s.auto_push_symbols = vec!["NEWCO".into()];
        assert!(candidate_confidence("NEWCO", &row, &s).is_some());
    }

    #[test]
    fn a_symbol_that_is_not_pushed_still_needs_a_good_call() {
        // The bypass must be scoped to the pushed name only; every other row
        // keeps the normal rules.
        let s = Settings::default();
        let row = push_row("Avoid", 99.0);
        assert_eq!(candidate_confidence("OTHER", &row, &s), None);
    }

    #[test]
    fn pending_merge_dedupes_and_caps() {
        let existing = vec!["ITC".to_string()];
        // A re-scan that repeats a pending name must not queue it twice.
        let out = merge_pending(&existing, &["ITC".into(), "BEL".into()], 10);
        assert_eq!(out, vec!["ITC".to_string(), "BEL".to_string()]);

        // Duplicates INSIDE one push are collapsed too.
        let out = merge_pending(&[], &["BEL".into(), "BEL".into()], 10);
        assert_eq!(out, vec!["BEL".to_string()]);

        // The cap bounds the queue: pending names are kept, extras are dropped.
        let out = merge_pending(&["A".into(), "B".into()], &["C".into(), "D".into()], 3);
        assert_eq!(out, vec!["A".to_string(), "B".to_string(), "C".to_string()]);
    }

    #[test]
    fn auto_push_symbols_default_empty_and_round_trip() {
        // Pending pushes are durable: a restart must keep the names the operator
        // already handed over, and a fresh install must start with none.
        assert!(Settings::default().auto_push_symbols.is_empty());
        let s: Settings = serde_json::from_value(json!({
            "autoPushSymbols": ["ITC", "BEL"]
        }))
        .unwrap();
        assert_eq!(s.auto_push_symbols, vec!["ITC".to_string(), "BEL".to_string()]);
    }

    #[test]
    fn daily_square_off_fires_once_after_the_set_time() {
        // Mon 2026-11-02, IST midnight, as epoch seconds.
        let at = |h: i64, m: i64| 1_793_557_800i64 + h * 3_600 + m * 60;
        // Before the clock: not due.
        assert!(!daily_exit_due(at(15, 19), true, "15:20", 0, true));
        // At/after the clock, first time today: due.
        assert!(daily_exit_due(at(15, 20), true, "15:20", 0, true));
        assert!(daily_exit_due(at(15, 29), true, "15:20", 0, true));
        // Already ran today: never twice.
        let ran = today_ist_at(at(15, 20));
        assert!(!daily_exit_due(at(15, 25), true, "15:20", ran, true));
        // Next day at the same clock: due again.
        assert!(daily_exit_due(at(24, 0) + 15 * 3_600 + 20 * 60, true, "15:20", ran, true));
        // Market shut: nothing is sold.
        assert!(!daily_exit_due(at(15, 20), true, "15:20", 0, false));
        // Weekend never fires.
        assert!(!daily_exit_due(at(15, 20) + 5 * 86_400, true, "15:20", 0, true));
        // Bad clock string never arms.
        assert!(!daily_exit_due(at(15, 20), true, "abc", 0, true));
    }

    #[test]
    fn daily_square_off_never_fires_when_the_switch_is_off() {
        // The operator's "Daily square-off all positions" checkbox is the only
        // arming switch. An UNCHECKED box must leave every open position alone -
        // no matter the clock, the day mark, or whether the session is open.
        let at = |h: i64, m: i64| 1_793_557_800i64 + h * 3_600 + m * 60;
        assert!(!daily_exit_due(at(15, 20), false, "15:20", 0, true));
        assert!(!daily_exit_due(at(15, 29), false, "15:20", 0, true));
        assert!(!daily_exit_due(at(24, 0) + 15 * 3_600 + 20 * 60, false, "15:20", 0, true));
        // Off must stay off even when it never ran today.
        assert!(!daily_exit_due(at(15, 25), false, "15:20", today_ist_at(at(15, 20)), true));
    }

    #[test]
    fn daily_square_off_defaults_off_and_parses_hhmm() {
        let s = Settings::default();
        assert!(!s.daily_exit_on);
        assert_eq!(s.daily_exit_time, "15:20");
        assert_eq!(parse_hhmm("15:20"), Some(15 * 60 + 20));
        assert_eq!(parse_hhmm("9:05"), Some(9 * 60 + 5));
        assert_eq!(parse_hhmm("24:00"), None);
        assert_eq!(parse_hhmm("hello"), None);
        assert_eq!(parse_hhmm(""), None);
    }

    #[test]
    fn daily_square_off_settings_round_trip() {
        let s: Settings = serde_json::from_value(json!({
            "dailyExitOn": true,
            "dailyExitTime": "14:55"
        }))
        .unwrap();
        assert!(s.daily_exit_on);
        assert_eq!(s.daily_exit_time, "14:55");
    }

    #[test]
    fn candidates_are_ordered_by_expected_return_high_to_low() {
        // The equity books must buy / list the highest-possible-return name
        // first. Confidence only breaks ties, never overrides upside.
        let mut c = vec![
            (8.0, 95.0, "LOW_RET".to_string(), json!({})),
            (30.0, 40.0, "HIGH_RET".to_string(), json!({})),
            (12.0, 60.0, "MID_A".to_string(), json!({})),
            (12.0, 80.0, "MID_B".to_string(), json!({})),
        ];
        order_candidates(&mut c);
        let syms: Vec<&str> = c.iter().map(|x| x.2.as_str()).collect();
        assert_eq!(syms, vec!["HIGH_RET", "MID_B", "MID_A", "LOW_RET"]);
    }

    #[test]
    fn quote_ltp_uses_shared_quote_key_shape() {
        let mut q = HashMap::new();
        q.insert("123".to_string(), json!({"ltp": 456.75}));
        q.insert("IDX_I:9".to_string(), json!({"ltp": 100.0}));
        assert_eq!(quote_ltp(&q, 123, "NSE_EQ"), Some(456.75));
        assert_eq!(quote_ltp(&q, 9, "NSE_EQ"), None);
        assert_eq!(quote_ltp(&q, 9, "IDX_I"), Some(100.0));
        assert_eq!(quote_ltp(&q, 123, "NSE_EQ"), Some(456.75));
    }

    #[test]
    fn qty_prefers_manual_then_global_then_capital() {
        assert_eq!(choose_qty(50_000.0, 100.0, 0, None), 500);
        assert_eq!(choose_qty(50_000.0, 100.0, 150, None), 150);
        assert_eq!(choose_qty(50_000.0, 100.0, 150, Some(7)), 7);
        assert_eq!(choose_qty(50_000.0, 100.0, 0, Some(0)), 500);
        assert_eq!(choose_qty(50_000.0, 0.0, 0, None), 0);
        assert_eq!(choose_qty(0.0, 100.0, 0, None), 0);
    }

    #[test]
    fn auto_distribute_splits_remaining_across_free_slots() {
        let s = Settings::default();
        // 400k across 8 free slots = 50k each; the remaining capital caps it.
        assert_eq!(auto_distribute_alloc(400_000.0, &s, 400_000.0, 8), 50_000.0);
        assert_eq!(auto_distribute_alloc(400_000.0, &s, 120_000.0, 8), 15_000.0);
        // The key fix: the pool is divided by the FREE slots, not max_positions,
        // so a partially filled book still deploys its whole remaining margin.
        assert_eq!(auto_distribute_alloc(400_000.0, &s, 300_000.0, 6), 50_000.0);
        assert_eq!(auto_distribute_alloc(400_000.0, &s, 100_000.0, 1), 100_000.0);
        assert_eq!(auto_distribute_alloc(400_000.0, &s, 0.0, 8), 0.0);
        // No allotment (Rs 0) => nothing to distribute.
        assert_eq!(auto_distribute_alloc(0.0, &s, 400_000.0, 8), 0.0);
        // A zero free-slot count must not divide by zero.
        assert_eq!(auto_distribute_alloc(400_000.0, &s, 400_000.0, 0), 400_000.0);
    }

    #[test]
    fn charges_aware_sizing_actually_uses_the_whole_slot() {
        // Regression for "poora margin deploy nahi hota": with 8 slots and a
        // 500k pool each slot gets 62.5k, and the reserved charges must not eat
        // more than a fraction of a percent of it.
        let s = Settings::default();
        let alloc = auto_distribute_alloc(500_000.0, &s, 500_000.0, 8);
        assert_eq!(alloc, 62_500.0);
        let budget = charges_aware_budget(alloc, true);
        let spent = (budget / 100.0).floor() * 100.0;
        assert!(spent > 60_000.0, "slot only sized Rs {spent} of Rs {alloc}");
    }

    #[test]
    fn charges_aware_budget_reserves_the_round_trip_cost() {
        // Delivery charges are ~0.13% per side; the reservation must be small but
        // strictly positive, and switching the feature off must not reserve.
        let budget = charges_aware_budget(100_000.0, true);
        assert!(budget < 100_000.0);
        assert!(budget > 99_500.0, "budget {budget} should stay near the slot");
        assert_eq!(charges_aware_budget(100_000.0, false), 100_000.0);
        assert_eq!(charges_aware_budget(0.0, true), 0.0);
        // Higher turnover carries proportionally more charges.
        let small = 100_000.0 - charges_aware_budget(100_000.0, true);
        let big = 1_000_000.0 - charges_aware_budget(1_000_000.0, true);
        assert!(big > small, "charges must scale with turnover");
    }

    #[test]
    fn charges_leave_room_rejects_a_target_that_barely_covers_costs() {
        // 1 share at 100 with a target of 100.10 clears ~0.02 paise of gross but
        // owes ~0.27 in round-trip charges => automatic entry must be refused.
        assert!(charges_leave_room(100.0, 100.10, 1, true).is_err());
        // A real swing (target +18%) clears the charges comfortably.
        assert!(charges_leave_room(100.0, 118.0, 1, true).is_ok());
        // Switch OFF / non-positive targets are never blocked.
        assert!(charges_leave_room(100.0, 100.10, 1, false).is_ok());
        assert!(charges_leave_room(0.0, 118.0, 1, true).is_ok());
    }

    #[test]
    fn auto_distribute_total_is_cap_bounded_by_remaining() {
        assert_eq!(auto_distribute_total(250_000.0, 400_000.0), 250_000.0);
        assert_eq!(auto_distribute_total(250_000.0, 90_000.0), 90_000.0);
        assert_eq!(auto_distribute_total(250_000.0, -5.0), 0.0);
        assert_eq!(auto_distribute_total(0.0, 400_000.0), 0.0);
    }

    #[test]
    fn trail_stop_arms_only_in_profit_and_gives_back_pct() {
        // No peak -> not armed, so the fixed stop owns the trade.
        assert_eq!(equity_trail_stop(100.0, 0.0, 25.0), 0.0);
        assert_eq!(equity_trail_stop(100.0, 10.0, 0.0), 0.0);
        // Peak Rs 20 profit, give back 25% -> stop at 100 + 20 - 5 = 115.
        assert!((equity_trail_stop(100.0, 20.0, 25.0) - 115.0).abs() < 1e-9);
        // 50% give-back of the same peak.
        assert!((equity_trail_stop(100.0, 20.0, 50.0) - 110.0).abs() < 1e-9);
    }

    #[test]
    fn net_tp_floor_needs_minimum_and_gives_back_pct() {
        // Peak below the rupee floor -> feature not armed.
        assert!(equity_net_tp_floor(500.0, 1_000.0, 25.0).is_none());
        // Zero floor disables it entirely.
        assert!(equity_net_tp_floor(5_000.0, 0.0, 25.0).is_none());
        // Peak Rs 4000, give back 25% -> floor Rs 3000.
        let floor = equity_net_tp_floor(4_000.0, 1_000.0, 25.0).unwrap();
        assert!((floor - 3_000.0).abs() < 1e-9);
        // Give-back is clamped to the floor so a locked trade cannot book below.
        let clamped = equity_net_tp_floor(1_200.0, 1_000.0, 90.0).unwrap();
        assert!((clamped - 1_000.0).abs() < 1e-9);
        // Out-of-range give-back falls back to the built-in 25%.
        let dflt = equity_net_tp_floor(4_000.0, 1_000.0, 0.0).unwrap();
        assert!((dflt - 3_000.0).abs() < 1e-9);
    }

    #[test]
    fn delivery_charges_are_positive_and_scale_with_qty() {
        let one = equity_charges(100.0, 110.0, 100);
        let two = equity_charges(100.0, 110.0, 200);
        assert!(one > 0.0);
        assert!(two > one, "charges must scale with turnover");
        // No usable inputs -> no charges rather than a bogus number.
        assert_eq!(equity_charges(0.0, 110.0, 100), 0.0);
        assert_eq!(equity_charges(100.0, 110.0, 0), 0.0);
    }

    #[test]
    fn trail_and_net_tp_defaults_are_off() {
        let s = Settings::default();
        assert!(!s.trail_sl_on);
        assert_eq!(s.trail_sl_pct, 25.0);
        assert!(!s.net_tp_on);
        assert_eq!(s.net_tp_min, 0.0);
        assert_eq!(s.net_tp_giveback_pct, 25.0);
    }

    #[test]
    fn auto_buy_and_charges_aware_default_on_and_roundtrip() {
        // Auto Buy is the engine's normal behaviour, so it defaults ON; a cache
        // written before the flags existed must still load with sane defaults.
        let d = Settings::default();
        assert!(d.auto_buy);
        assert!(d.charges_aware);
        let parsed: Settings = serde_json::from_value(json!({"autoBuy": false, "chargesAware": false})).unwrap();
        assert!(!parsed.auto_buy);
        assert!(!parsed.charges_aware);
        let old: Settings = serde_json::from_value(json!({"maxPositions": 8})).unwrap();
        assert!(old.auto_buy && old.charges_aware);
    }

    #[test]
    fn auto_distribute_settings_roundtrip_defaults_off() {
        let s = Settings::default();
        assert!(!s.auto_distribute);
        let parsed: Settings = serde_json::from_str(r#"{"capital":100000}"#).unwrap();
        assert!(!parsed.auto_distribute);
        let parsed: Settings = serde_json::from_str(r#"{"autoDistribute":true}"#).unwrap();
        assert!(parsed.auto_distribute);
    }
}
