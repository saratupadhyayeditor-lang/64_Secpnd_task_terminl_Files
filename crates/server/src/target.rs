//! Trend Target Research engine.
//!
//! Multi-factor, confluence-based target discovery for indices and MCX
//! commodities. The engine reads the live 5-minute candle series plus the option
//! chain (OI, PCR, max pain, ATM IV) and combines:
//!
//!   * trend      - EMA20/50/200, regression slope, 15-minute higher timeframe
//!   * structure  - swing pivots, previous session high/low, VWAP, round strikes
//!   * patterns   - engulfing, pin bar, inside-bar and star measured moves
//!   * options    - OI walls (max CE/PE open interest), PCR, max pain, IV move
//!   * evidence   - a walk-forward hit-rate of the same target/stop rule
//!
//! Every factor votes on a price level. Nearby votes are clustered, the best
//! cluster above spot becomes the BUY target, and the nearest cluster below (or
//! one ATR) becomes the invalidation. Selling/short setups are never produced. The confidence combines trend
//! strength, cluster agreement, factor agreement and the historical hit-rate.
//!
//! `spawn_refresher` keeps this cache warm for the Target Lab tab; engines with
//! `targetAutoOn` freeze the cached plan at entry (or on the first matching tick
//! for an already-open trade) and cut the position when the UNDERLYING reaches
//! the target (not the option premium), so the exit is independent of premium
//! decay and strike selection.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use algo_core::model::Candle;
use algo_core::option as bs;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use crate::broker::DhanState;
use crate::market;
use crate::optionchain;
use crate::scrip;

pub const PLAN_TTL_MS: i64 = 45 * 60 * 1000;
const REFRESH_SECS: u64 = 600;

struct Watch {
    sid: i64,
    seg: &'static str,
    inst: &'static str,
    name: &'static str,
    key: &'static str,
}

const WATCH: &[Watch] = &[
    Watch { sid: 13, seg: "IDX_I", inst: "INDEX", name: "NIFTY 50", key: "NIFTY" },
    Watch { sid: 25, seg: "IDX_I", inst: "INDEX", name: "BANK NIFTY", key: "BANKNIFTY" },
    Watch { sid: 27, seg: "IDX_I", inst: "INDEX", name: "FINNIFTY", key: "FINNIFTY" },
    Watch { sid: 442, seg: "IDX_I", inst: "INDEX", name: "MIDCPNIFTY", key: "MIDCPNIFTY" },
    Watch { sid: 51, seg: "IDX_I", inst: "INDEX", name: "SENSEX", key: "SENSEX" },
    Watch { sid: 574829, seg: "MCX_COMM", inst: "FUTCOM", name: "COPPER", key: "COPPER" },
    Watch { sid: 573422, seg: "MCX_COMM", inst: "FUTCOM", name: "CRUDEOIL", key: "CRUDEOIL" },
    Watch { sid: 573423, seg: "MCX_COMM", inst: "FUTCOM", name: "CRUDEOILM", key: "CRUDEOILM" },
    Watch { sid: 495213, seg: "MCX_COMM", inst: "FUTCOM", name: "GOLD", key: "GOLD" },
    Watch { sid: 571445, seg: "MCX_COMM", inst: "FUTCOM", name: "GOLDM", key: "GOLDM" },
    Watch { sid: 570751, seg: "MCX_COMM", inst: "FUTCOM", name: "NATGASMINI", key: "NATGASMINI" },
    Watch { sid: 570750, seg: "MCX_COMM", inst: "FUTCOM", name: "NATURALGAS", key: "NATURALGAS" },
    Watch { sid: 495214, seg: "MCX_COMM", inst: "FUTCOM", name: "SILVER", key: "SILVER" },
    Watch { sid: 483080, seg: "MCX_COMM", inst: "FUTCOM", name: "SILVERM", key: "SILVERM" },
    Watch { sid: 574834, seg: "MCX_COMM", inst: "FUTCOM", name: "ZINC", key: "ZINC" },
];

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

fn cache() -> &'static Mutex<Vec<Value>> {
    static CACHE: OnceLock<Mutex<Vec<Value>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(Vec::new()))
}

fn cache_put(row: Value) {
    let key = row.get("underlying").and_then(|v| v.as_str()).unwrap_or("").to_string();
    if key.is_empty() {
        return;
    }
    if let Ok(mut g) = cache().lock() {
        if let Some(slot) = g
            .iter_mut()
            .find(|r| r.get("underlying").and_then(|v| v.as_str()) == Some(key.as_str()))
        {
            *slot = row;
        } else {
            g.push(row);
        }
    }
}

fn cacheable(row: &Value) -> bool {
    row.get("spot").and_then(|v| v.as_f64()).unwrap_or(0.0) > 0.0
}

/// Minimal entry-time plan for a position's canonical underlying ("NIFTY",
/// "BANKNIFTY", "CRUDEOIL", ...). `None` while the cache is cold or stale.
/// True when the Target Lab refresher watches this underlying, i.e. when a
/// cached research plan can exist. Strict entry confirmation is limited to this
/// universe so stock/F&O legs (which the cache never covers) are left untouched.
pub fn covers_underlying(underlying: &str) -> bool {
    let key = scrip::fno_underlying(underlying);
    if !key.is_empty() && WATCH.iter().any(|w| w.key == key) {
        return true;
    }
    match underlying.split(['-', ' ']).next() {
        Some(short) if !short.eq_ignore_ascii_case(underlying) => {
            let k = scrip::fno_underlying(short);
            !k.is_empty() && WATCH.iter().any(|w| w.key == k)
        }
        _ => false,
    }
}

pub fn plan_for_underlying(underlying: &str) -> Option<Value> {
    let key = scrip::fno_underlying(underlying);
    let now = now_ms();
    let g = cache().lock().ok()?;
    let row = g
        .iter()
        .find(|r| r.get("underlying").and_then(|v| v.as_str()) == Some(key.as_str()))?;
    let as_of = row.get("asOf").and_then(|v| v.as_i64()).unwrap_or(0);
    if as_of <= 0 || now - as_of > PLAN_TTL_MS {
        return None;
    }
    let dir = row.get("direction").and_then(|v| v.as_i64()).unwrap_or(0);
    let target = row.get("target").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let conf = row.get("confidence").and_then(|v| v.as_f64()).unwrap_or(0.0);
    if dir.abs() != 1 || target <= 0.0 || conf <= 0.0 {
        return None;
    }
    Some(json!({
        "underlying": key,
        "display": row.get("symbol").cloned().unwrap_or(Value::Null),
        "securityId": row.get("securityId").cloned().unwrap_or(Value::Null),
        "exchangeSegment": row.get("exchangeSegment").cloned().unwrap_or(Value::Null),
        "direction": dir,
        "target": target,
        "target2": row.get("target2").cloned().unwrap_or(Value::Null),
        "stop": row.get("stop").cloned().unwrap_or(Value::Null),
        "rr": row.get("rr").cloned().unwrap_or(Value::Null),
        "confidence": conf,
        "spot": row.get("spot").cloned().unwrap_or(Value::Null),
        "atr": row.get("atr").cloned().unwrap_or(Value::Null),
        "movePct": row.get("movePct").cloned().unwrap_or(Value::Null),
        "expiry": row.get("expiry").cloned().unwrap_or(Value::Null),
        "chain": row.get("chain").cloned().unwrap_or(Value::Null),
        "backtest": row.get("backtest").cloned().unwrap_or(Value::Null),
        "asOf": as_of,
        "expiresAt": as_of + PLAN_TTL_MS,
    }))
}

pub fn cache_rows() -> Vec<Value> {
    cache()
        .lock()
        .map(|g| {
            let mut rows = g.clone();
            rows.sort_by(|a, b| {
                let ka = a.get("underlying").and_then(|v| v.as_str()).unwrap_or("");
                let kb = b.get("underlying").and_then(|v| v.as_str()).unwrap_or("");
                ka.cmp(kb)
            });
            rows
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Endpoint
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct AnalyzeReq {
    #[serde(default)]
    pub security_id: i64,
    #[serde(default)]
    pub exchange_segment: String,
    #[serde(default)]
    pub instrument: String,
    #[serde(default)]
    pub symbol_name: String,
    #[serde(default)]
    pub timeframe: String,
    /// `auto` (default) | `long` | `short`
    #[serde(default)]
    pub direction: String,
}

pub async fn analyze(
    State(st): State<DhanState>,
    Json(req): Json<AnalyzeReq>,
) -> impl IntoResponse {
    let watch = WATCH.iter().find(|w| w.sid == req.security_id);
    let (sid, seg, inst, name, key) = match watch {
        Some(w) => (w.sid, w.seg.to_string(), w.inst.to_string(), w.name.to_string(), w.key.to_string()),
        None => {
            if req.security_id <= 0 {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "ok": false, "error": "security_id is required" })),
                )
                    .into_response();
            }
            let name = if req.symbol_name.trim().is_empty() {
                req.security_id.to_string()
            } else {
                req.symbol_name.trim().to_string()
            };
            let key = scrip::fno_underlying(&name);
            (
                req.security_id,
                if req.exchange_segment.trim().is_empty() {
                    "NSE_FNO".to_string()
                } else {
                    req.exchange_segment.to_uppercase()
                },
                if req.instrument.trim().is_empty() { "INDEX".to_string() } else { req.instrument.to_uppercase() },
                name,
                key,
            )
        }
    };
    let tf = if req.timeframe.trim().is_empty() { "5min" } else { req.timeframe.trim() };
    let row = build_row(&st, sid, &seg, &inst, &name, &key, tf, &req.direction).await;
    if cacheable(&row) {
        cache_put(row.clone());
    }
    Json(row).into_response()
}

/// Force one refresh cycle in the background; the tab polls `/api/target/cache`.
pub async fn refresh(State(st): State<DhanState>) -> impl IntoResponse {
    tokio::spawn(async move { refresh_cycle(st).await });
    Json(json!({ "ok": true, "status": "refreshing" }))
}

pub async fn cache_handler() -> impl IntoResponse {
    Json(json!({
        "ok": true,
        "at": now_ms(),
        "rows": cache_rows(),
    }))
}

// ---------------------------------------------------------------------------
// Background refresher
// ---------------------------------------------------------------------------

pub fn spawn_refresher(st: DhanState) {
    tokio::spawn(async move {
        // Stream the three NIFTY heavyweights so the 1-second momentum sampler
        // works without extra REST calls.
        st.subscribe_options(&[
            (1333, "NSE_EQ".to_string()),
            (2885, "NSE_EQ".to_string()),
            (4963, "NSE_EQ".to_string()),
        ])
        .await;
        tokio::spawn(sample_heavies(st.clone()));
        tokio::time::sleep(Duration::from_secs(40)).await;
        loop {
            let mut ok = 0usize;
            if st.is_connected().await && !st.rate_limited() {
                ok = refresh_cycle(st.clone()).await;
            }
            // A cold/failed cycle (feed still waking up, Dhan throttle) must not
            // wait the full 10 minutes: retry quickly until the cache warms.
            let wait = if ok == 0 { 30 } else { REFRESH_SECS };
            tokio::time::sleep(Duration::from_secs(wait)).await;
        }
    });
}

static BUSY: AtomicBool = AtomicBool::new(false);

async fn refresh_cycle(st: DhanState) -> usize {
    struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            BUSY.store(false, Ordering::Relaxed);
        }
    }
    // Manual "refresh now" can land while the 10-minute cycle is running.
    // Overlapping cycles would double-fetch the chain and trip Dhan throttling.
    if BUSY.swap(true, Ordering::Relaxed) {
        return 0;
    }
    let _guard = Guard;
    let mut ok = 0usize;
    for w in WATCH {
        if st.rate_limited() {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        let row = build_row(&st, w.sid, w.seg, w.inst, w.name, w.key, "5min", "auto").await;
        if cacheable(&row) {
            cache_put(row);
            ok += 1;
        }
        tokio::time::sleep(Duration::from_millis(1200)).await;
    }
    ok
}

async fn build_row(
    st: &DhanState,
    sid: i64,
    seg: &str,
    inst: &str,
    name: &str,
    key: &str,
    tf: &str,
    direction: &str,
) -> Value {
    let candles = match st.fetch_candles(sid, seg, inst, tf).await {
        Ok(c) if c.len() >= 40 => c,
        Ok(_) => {
            return json!({
                "ok": false, "underlying": key, "symbol": name, "securityId": sid,
                "exchangeSegment": seg, "asOf": now_ms(),
                "error": "not enough candles for a target",
            })
        }
        Err(e) => {
            return json!({
                "ok": false, "underlying": key, "symbol": name, "securityId": sid,
                "exchangeSegment": seg, "asOf": now_ms(),
                "error": e.to_string(),
            })
        }
    };
    let spot = candles.last().map(|c| c.close).unwrap_or(0.0);
    let (chain, expiry) = option_data(st, sid, seg, name, spot).await;
    // NIFTY heavyweight confirmation: three liquid index drivers, 5-minute
    // bars (Dhan intraday), used for momentum z-score and index divergence.
    let comps = if key == "NIFTY" && !tick_source_ready() {
        fetch_components(st).await
    } else {
        Vec::new()
    };
    let mut row = compute(name, key, sid, seg, &candles, &chain, &expiry, spot, direction, &comps);
    let daily = crate::research::daily_cached(st, sid, seg, inst).await;
    let lot = scrip::get()
        .and_then(|sc| sc.lot_for(name, seg))
        .map(|(l, _)| l)
        .filter(|l| *l > 0.0)
        .unwrap_or_else(|| optionchain::lot_size_for(name, sid));
    crate::research::enrich(
        &mut row,
        &crate::research::Enrich {
            key,
            commodity: inst == "FUTCOM",
            candles: &candles,
            daily: &daily,
            chain: &chain,
            lot,
            roll_basis_pct: None,
        },
    );
    row["asOf"] = json!(now_ms());
    row
}

const NIFTY_HEAVIES: [(&str, i64); 3] = [
    ("HDFCBANK", 1333),
    ("RELIANCE", 2885),
    ("ICICIBANK", 4963),
];

async fn fetch_components(st: &DhanState) -> Vec<CompSeries> {
    let mut out = Vec::new();
    for (name, sid) in NIFTY_HEAVIES {
        if st.rate_limited() {
            break;
        }
        if let Ok(cs) = st.fetch_candles(sid, "NSE_EQ", "EQUITY", "5min").await {
            if cs.len() >= 22 {
                out.push(CompSeries { name, candles: cs });
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    out
}

async fn option_data(
    st: &DhanState,
    sid: i64,
    seg: &str,
    name: &str,
    spot: f64,
) -> (Vec<bs::ChainRow>, String) {
    if market::commodity_has_options(sid) == Some(false) {
        return (Vec::new(), String::new());
    }
    let (fno_sid, fno_seg) = scrip::get()
        .map(|sc| sc.resolve_underlying(name, sid, seg, ""))
        .unwrap_or((sid, seg.to_string()));
    let mut expiries = optionchain::chain_expiries(fno_sid, &fno_seg, name);
    if expiries.is_empty() {
        if let Ok(list) = st.fetch_option_expiries(fno_sid, &fno_seg).await {
            expiries = list;
        }
    }
    let Some(expiry) = expiries.into_iter().find(|e| !e.trim().is_empty()) else {
        return (Vec::new(), String::new());
    };
    match st.fetch_option_chain(fno_sid, &fno_seg, &expiry).await {
        Ok(data) => {
            let mut rows = optionchain::dhan_rows(&data, market::now_secs());
            // The REST chain can omit the spot; the candle close is authoritative
            // here. Drop rows with no strike.
            rows.retain(|r| r.strike > 0.0);
            if spot <= 0.0 {
                return (rows, expiry);
            }
            (rows, expiry)
        }
        Err(_) => (Vec::new(), expiry),
    }
}

// ---------------------------------------------------------------------------
// Math helpers
// ---------------------------------------------------------------------------

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn nz(v: f64) -> f64 {
    if v.is_finite() {
        v
    } else {
        0.0
    }
}

fn round2(v: f64) -> f64 {
    (nz(v) * 100.0).round() / 100.0
}

fn ema_last(vals: &[f64], n: usize) -> f64 {
    if vals.is_empty() || n == 0 {
        return 0.0;
    }
    let k = 2.0 / (n as f64 + 1.0);
    let mut e = vals[0];
    for v in &vals[1..] {
        e = v * k + e * (1.0 - k);
    }
    e
}

fn atr(cs: &[Candle], n: usize) -> f64 {
    if cs.len() < 2 || n == 0 {
        return 0.0;
    }
    let start = cs.len().saturating_sub(n + 1);
    let mut sum = 0.0;
    let mut count = 0usize;
    let mut prev_close = cs[start].close;
    for c in &cs[start + 1..] {
        let tr = (c.high - c.low)
            .max((c.high - prev_close).abs())
            .max((c.low - prev_close).abs());
        sum += tr;
        count += 1;
        prev_close = c.close;
    }
    if count == 0 {
        0.0
    } else {
        sum / count as f64
    }
}

fn slope(vals: &[f64], n: usize) -> f64 {
    let m = vals.len().min(n);
    if m < 3 {
        return 0.0;
    }
    let slice = &vals[vals.len() - m..];
    let xm = (m as f64 - 1.0) / 2.0;
    let ym: f64 = slice.iter().sum::<f64>() / m as f64;
    let mut num = 0.0;
    let mut den = 0.0;
    for (i, y) in slice.iter().enumerate() {
        let dx = i as f64 - xm;
        num += dx * (y - ym);
        den += dx * dx;
    }
    if den == 0.0 {
        0.0
    } else {
        num / den
    }
}

fn median(v: &mut [f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let m = v.len() / 2;
    if v.len() % 2 == 0 {
        (v[m - 1] + v[m]) / 2.0
    } else {
        v[m]
    }
}

fn resample(cs: &[Candle], factor: usize) -> Vec<Candle> {
    if factor <= 1 || cs.is_empty() {
        return cs.to_vec();
    }
    let mut out = Vec::with_capacity(cs.len() / factor + 1);
    let mut i = 0;
    while i < cs.len() {
        let end = (i + factor).min(cs.len());
        let part = &cs[i..end];
        let first = part[0];
        let last = part[part.len() - 1];
        let mut hi = f64::MIN;
        let mut lo = f64::MAX;
        let mut vol = 0.0;
        for c in part {
            hi = hi.max(c.high);
            lo = lo.min(c.low);
            vol += c.volume;
        }
        out.push(Candle {
            time: first.time,
            open: first.open,
            high: hi,
            low: lo,
            close: last.close,
            volume: vol,
        });
        i = end;
    }
    out
}

#[derive(Clone)]
struct Pivot {
    price: f64,
    idx: usize,
    vol: f64,
    high: bool,
}

fn pivots(cs: &[Candle], k: usize) -> Vec<Pivot> {
    let mut out = Vec::new();
    if cs.len() < 2 * k + 1 {
        return out;
    }
    for i in k..cs.len() - k {
        let win = &cs[i - k..=i + k];
        let is_high = win.iter().all(|c| cs[i].high >= c.high);
        let is_low = win.iter().all(|c| cs[i].low <= c.low);
        if is_high && !is_low {
            out.push(Pivot { price: cs[i].high, idx: i, vol: cs[i].volume, high: true });
        } else if is_low && !is_high {
            out.push(Pivot { price: cs[i].low, idx: i, vol: cs[i].volume, high: false });
        }
    }
    out
}

fn session_stats(cs: &[Candle]) -> (f64, f64, f64) {
    let Some(last) = cs.last() else { return (0.0, 0.0, 0.0) };
    let day = last.time.div_euclid(86400);
    let start = cs
        .iter()
        .position(|c| c.time.div_euclid(86400) == day)
        .unwrap_or(cs.len().saturating_sub(80));
    let today = &cs[start..];
    if today.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let hi = today.iter().map(|c| c.high).fold(f64::MIN, f64::max);
    let lo = today.iter().map(|c| c.low).fold(f64::MAX, f64::min);
    let mut pv = 0.0;
    let mut vv = 0.0;
    for c in today {
        let typical = (c.high + c.low + c.close) / 3.0;
        let v = if c.volume > 0.0 { c.volume } else { 1.0 };
        pv += typical * v;
        vv += v;
    }
    (hi, lo, if vv > 0.0 { pv / vv } else { last.close })
}

fn round_step(name: &str, spot: f64) -> f64 {
    let n = name.to_uppercase();
    if n.contains("BANK") || n.contains("SENSEX") {
        return if spot > 40000.0 { 100.0 } else { 50.0 };
    }
    if n.contains("MIDCP") {
        return 25.0;
    }
    if n.contains("NIFTY") || n.contains("FINNIFTY") {
        return 50.0;
    }
    if spot > 100000.0 {
        500.0
    } else if spot > 20000.0 {
        100.0
    } else if spot > 2000.0 {
        10.0
    } else if spot > 200.0 {
        1.0
    } else {
        0.5
    }
}

// ---------------------------------------------------------------------------
// Core model
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Candidate {
    price: f64,
    weight: f64,
    kind: &'static str,
    detail: String,
    dir: i32,
}

#[derive(Default)]
struct ChainStats {
    has: bool,
    pcr: f64,
    ce_oi: f64,
    pe_oi: f64,
    ce_chg: f64,
    pe_chg: f64,
    ce_wall: f64,
    pe_wall: f64,
    max_pain: f64,
    atm_iv: f64,
    iv_skew: f64,
    exp_move: f64,
    liquid: bool,
}

// ---------------------------------------------------------------------------
// Price-action / execution edge
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct CompSeries {
    name: &'static str,
    candles: Vec<Candle>,
}

#[derive(Clone, Copy, Default)]
struct GapEdge {
    low: f64,
    high: f64,
    fill: f64,
    bars_ago: usize,
}

#[derive(Clone, Copy, Default)]
struct SweepEdge {
    level: f64,
    bars_ago: usize,
    reclaim_atr: f64,
}

#[derive(Clone, Copy, Default)]
struct RangeEdge {
    hi: f64,
    lo: f64,
    ready: bool,
    breakout: bool,
    retest: bool,
    failed: bool,
    breakdown: bool,
    retest_down: bool,
    failed_down: bool,
}

#[derive(Clone, Copy, Default)]
struct VolumeEdge {
    z: f64,
    up_share: f64,
    buy_pressure: f64,
    delta: f64,
}

#[derive(Clone, Debug, Default)]
struct CompEdge {
    available: bool,
    lead: bool,
    bearish: bool,
    ret_pct: f64,
    z: f64,
    vol_z: f64,
    breadth: f64,
    divergence_pct: f64,
    confirmed: bool,
    names: Vec<String>,
    source: String,
    samples: usize,
}

#[derive(Clone, Default)]
struct Edge {
    range: RangeEdge,
    bull_sweep: Option<SweepEdge>,
    bear_sweep: Option<SweepEdge>,
    bull_fvg: Option<GapEdge>,
    bear_fvg: Option<GapEdge>,
    vol: VolumeEdge,
    comp: CompEdge,
}

/// Candle timestamps already carry IST wall-clock as a naive UTC value, so the
/// day/time-of-day helpers must not add the 5:30 offset again.
fn candle_day(t: i64) -> i64 {
    t.div_euclid(86400)
}

fn candle_tod(t: i64) -> i64 {
    t.rem_euclid(86400)
}

fn std_dev(v: &[f64]) -> f64 {
    if v.len() < 2 {
        return 0.0;
    }
    let m = v.iter().sum::<f64>() / v.len() as f64;
    (v.iter().map(|x| (x - m) * (x - m)).sum::<f64>() / (v.len() - 1) as f64).sqrt()
}

fn z_last(v: &[f64]) -> f64 {
    if v.len() < 2 {
        return 0.0;
    }
    let m = v.iter().sum::<f64>() / v.len() as f64;
    let s = std_dev(v);
    if s <= 1e-9 {
        0.0
    } else {
        (*v.last().unwrap() - m) / s
    }
}

/// How much of a fair-value gap later price has already traded back through.
/// 0 = untouched, 1 = fully rebalanced.
fn fvg_fill(cs: &[Candle], from: usize, low: f64, high: f64, bull: bool) -> f64 {
    let span = (high - low).max(1e-9);
    let mut best = 0.0f64;
    for c in &cs[from.min(cs.len())..] {
        if bull {
            if c.low <= low {
                return 1.0;
            }
            best = best.max(((high - c.low) / span).clamp(0.0, 1.0));
        } else {
            if c.high >= high {
                return 1.0;
            }
            best = best.max(((c.high - low) / span).clamp(0.0, 1.0));
        }
    }
    best
}

/// Unfilled bullish gap below spot (support) and unfilled bearish gap above
/// spot (resistance / buy-target magnet).
fn fvg_stats(cs: &[Candle], a: f64, spot: f64) -> (Option<GapEdge>, Option<GapEdge>) {
    let n = cs.len();
    if n < 3 || a <= 0.0 {
        return (None, None);
    }
    let start = n.saturating_sub(120).max(2);
    let mut bull: Option<GapEdge> = None;
    let mut bear: Option<GapEdge> = None;
    for i in start..n {
        let a1 = cs[i - 2];
        let c = cs[i];
        // Bullish FVG: candle i's low leaves a gap above candle i-2's high.
        if c.low > a1.high + a * 0.05 {
            let (lo, hi) = (a1.high, c.low);
            let fill = fvg_fill(cs, i + 1, lo, hi, true);
            if fill < 0.98 && hi < spot && spot - hi <= a * 4.0 {
                let cand = GapEdge { low: lo, high: hi, fill, bars_ago: n - 1 - i };
                if bull.map(|g| cand.high > g.high).unwrap_or(true) {
                    bull = Some(cand);
                }
            }
        }
        // Bearish FVG: candle i's high leaves a gap below candle i-2's low.
        if c.high + a * 0.05 < a1.low {
            let (lo, hi) = (c.high, a1.low);
            let fill = fvg_fill(cs, i + 1, lo, hi, false);
            if fill < 0.98 && lo > spot && lo - spot <= a * 5.0 {
                let cand = GapEdge { low: lo, high: hi, fill, bars_ago: n - 1 - i };
                if bear.map(|g| cand.low < g.low).unwrap_or(true) {
                    bear = Some(cand);
                }
            }
        }
    }
    (bull, bear)
}

/// Stop-run / liquidity sweep: wick through a 20-bar extreme that closes back
/// inside the range. Bullish = sweep below lows then reclaim; bearish = sweep
/// above highs then rejection.
fn sweep_stats(cs: &[Candle], a: f64) -> (Option<SweepEdge>, Option<SweepEdge>) {
    let n = cs.len();
    if n < 23 || a <= 0.0 {
        return (None, None);
    }
    let mut bull: Option<SweepEdge> = None;
    let mut bear: Option<SweepEdge> = None;
    for i in (n - 20).max(20)..n {
        let prior_low = cs[i - 20..i].iter().map(|c| c.low).fold(f64::MAX, f64::min);
        let prior_high = cs[i - 20..i].iter().map(|c| c.high).fold(f64::MIN, f64::max);
        let rng = (cs[i].high - cs[i].low).max(1e-9);
        if cs[i].low < prior_low - a * 0.03
            && cs[i].close > prior_low
            && cs[i].close > cs[i].low + rng * 0.55
        {
            bull = Some(SweepEdge {
                level: prior_low,
                bars_ago: n - 1 - i,
                reclaim_atr: (cs[i].close - prior_low) / a,
            });
        }
        if cs[i].high > prior_high + a * 0.03
            && cs[i].close < prior_high
            && cs[i].close < cs[i].high - rng * 0.55
        {
            bear = Some(SweepEdge {
                level: prior_high,
                bars_ago: n - 1 - i,
                reclaim_atr: (prior_high - cs[i].close) / a,
            });
        }
    }
    (bull, bear)
}

/// Opening range = 09:15-10:00 IST high/low, then breakout/retest/failure of
/// that range for the rest of the session.
fn opening_range(cs: &[Candle]) -> RangeEdge {
    let Some(last) = cs.last() else { return RangeEdge::default() };
    let day = candle_day(last.time);
    let mut hi = f64::MIN;
    let mut lo = f64::MAX;
    let mut n = 0usize;
    for c in cs.iter().filter(|c| candle_day(c.time) == day) {
        let tod = candle_tod(c.time);
        if (33300..36000).contains(&tod) {
            hi = hi.max(c.high);
            lo = lo.min(c.low);
            n += 1;
        }
    }
    if n < 2 || lo <= 0.0 || hi <= lo {
        return RangeEdge::default();
    }
    let mut r = RangeEdge {
        hi,
        lo,
        ready: candle_tod(last.time) >= 36000,
        ..Default::default()
    };
    let width = (hi - lo).max(1e-9);
    let bars: Vec<&Candle> = cs
        .iter()
        .filter(|c| candle_day(c.time) == day && candle_tod(c.time) > 36000)
        .collect();
    let Some(lastc) = bars.last() else { return r };
    let ever_break = bars.iter().any(|c| c.close > hi);
    let ever_break_dn = bars.iter().any(|c| c.close < lo);
    // Current state, not history: a failed breakout that was later reclaimed
    // must not keep penalising the setup.
    r.breakout = lastc.close > hi;
    r.failed = lastc.close < lo || (ever_break && lastc.close < hi - width * 0.15);
    r.breakdown = lastc.close < lo;
    r.failed_down =
        lastc.close > hi || (ever_break_dn && lastc.close > lo + width * 0.15);
    if r.breakout {
        let from = bars.iter().rposition(|c| c.close > hi).unwrap_or(bars.len() - 1);
        r.retest = bars[from..]
            .iter()
            .any(|c| c.low <= hi * 1.0005 && c.close >= hi * 0.9995);
    }
    if r.breakdown {
        let from = bars.iter().rposition(|c| c.close < lo).unwrap_or(bars.len() - 1);
        r.retest_down = bars[from..]
            .iter()
            .any(|c| c.high >= lo * 0.9995 && c.close <= lo * 1.0005);
    }
    r
}

/// 5-minute volume z-score plus buying-aggression proxy (up-volume share and
/// close-location weighted delta).
fn volume_stats(cs: &[Candle]) -> VolumeEdge {
    let n = cs.len();
    if n < 5 {
        return VolumeEdge::default();
    }
    let m = 20.min(n);
    let win = &cs[n - m..];
    let vols: Vec<f64> = win.iter().map(|c| c.volume.max(0.0)).collect();
    let z = z_last(&vols);
    let tot: f64 = vols.iter().sum();
    let mut up = 0.0;
    let mut delta = 0.0;
    for c in win {
        let v = c.volume.max(0.0);
        if c.close > c.open {
            up += v;
        }
        let rng = (c.high - c.low).max(1e-9);
        delta += ((c.close - c.open) / rng) * v;
    }
    let up_share = if tot > 0.0 { up / tot } else { 0.5 };
    let delta_norm = if tot > 0.0 { (delta / tot).clamp(-1.0, 1.0) } else { 0.0 };
    let buy = (50.0
        + 20.0 * (z / 2.0).tanh()
        + 25.0 * (up_share - 0.5) * 2.0
        + 25.0 * delta_norm)
        .clamp(0.0, 100.0);
    VolumeEdge { z, up_share, buy_pressure: buy, delta: delta_norm }
}

#[derive(Clone, Copy)]
struct TickSample {
    at_ms: i64,
    ltp: f64,
    volume: f64,
}

static TICKS: OnceLock<Mutex<HashMap<i64, VecDeque<TickSample>>>> = OnceLock::new();

fn tick_store() -> &'static Mutex<HashMap<i64, VecDeque<TickSample>>> {
    TICKS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn q_len_ok(q: Option<&VecDeque<TickSample>>) -> bool {
    q.map(|q| q.len() >= 60).unwrap_or(false)
}

fn tick_source_ready() -> bool {
    let Ok(g) = tick_store().lock() else { return false };
    NIFTY_HEAVIES.iter().all(|(_, sid)| {
        g.get(sid)
            .and_then(|q| q.back())
            .map(|t| q_len_ok(g.get(sid)) && now_ms() - t.at_ms < 30_000)
            .unwrap_or(false)
    })
}

/// One-second sampler for the three NIFTY heavyweights. The feed subscription
/// is registered once; ticks then land in `TICKS` without extra REST calls.
async fn sample_heavies(st: DhanState) {
    loop {
        if let Ok(q) = st.market.quotes.lock() {
            if let Ok(mut g) = tick_store().lock() {
                for (_, sid) in NIFTY_HEAVIES {
                    let Some(v) = q.get(&market::quote_key(sid, "NSE_EQ")) else { continue };
                    let ltp = v.get("ltp").and_then(|x| x.as_f64()).unwrap_or(0.0);
                    let vol = v.get("volume").and_then(|x| x.as_f64()).unwrap_or(0.0);
                    if ltp <= 0.0 {
                        continue;
                    }
                    let q1 = g.entry(sid).or_default();
                    let same = q1
                        .back()
                        .map(|t| (t.ltp - ltp).abs() < 1e-9 && (t.volume - vol).abs() < 1e-6)
                        .unwrap_or(false);
                    if same {
                        continue;
                    }
                    q1.push_back(TickSample { at_ms: now_ms(), ltp, volume: vol });
                    // 10 minutes of 1-second samples is enough for a 5-minute
                    // momentum read and keeps memory bounded.
                    while q1.len() > 700 {
                        q1.pop_front();
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn tick_component_stats(index_ret: f64) -> CompEdge {
    let Ok(g) = tick_store().lock() else { return CompEdge::default() };
    let mut e = CompEdge::default();
    let mut ret_sum = 0.0;
    let mut w_sum = 0.0;
    let mut z_sum = 0.0;
    let mut vz_sum = 0.0;
    let mut up = 0usize;
    let mut used = 0usize;
    let mut min_samples = usize::MAX;
    const W: [f64; 3] = [0.09, 0.10, 0.07];
    for (i, (name, sid)) in NIFTY_HEAVIES.iter().enumerate() {
        let Some(q) = g.get(sid) else { continue };
        if q.len() < 60 {
            continue;
        }
        let n = q.len();
        let last = q[n - 1];
        if now_ms() - last.at_ms > 120_000 {
            continue;
        }
        let back = q[n.saturating_sub(300)].ltp;
        if back <= 0.0 {
            continue;
        }
        let ret = (last.ltp / back - 1.0) * 100.0;
        // 15-second returns keep microstructure noise out of the z-score.
        let step = 15usize;
        let mut rets = Vec::new();
        for j in step..n {
            let prev = q[j - step].ltp;
            if prev > 0.0 {
                rets.push((q[j].ltp / prev - 1.0) * 100.0);
            }
        }
        let mut vols = Vec::new();
        for j in 1..n {
            let d = q[j].volume - q[j - 1].volume;
            if d >= 0.0 {
                vols.push(d);
            }
        }
        let w = W.get(i).copied().unwrap_or(0.08);
        ret_sum += ret * w;
        w_sum += w;
        z_sum += z_last(&rets) * w;
        vz_sum += z_last(&vols) * w;
        if ret > 0.0 {
            up += 1;
        }
        used += 1;
        min_samples = min_samples.min(n);
        e.names.push(name.to_string());
    }
    if used == 0 || w_sum <= 0.0 {
        return e;
    }
    let ret_w = ret_sum / w_sum;
    let z = z_sum / w_sum;
    let vz = vz_sum / w_sum;
    let breadth = up as f64 / used as f64;
    e.available = true;
    e.ret_pct = ret_w;
    e.z = z;
    e.vol_z = vz;
    e.breadth = breadth;
    e.divergence_pct = ret_w - index_ret;
    e.confirmed = vz > 0.5 && breadth >= 0.66 && ret_w > 0.0;
    e.lead = z > 0.8 && breadth >= 0.66 && ret_w > 0.0;
    e.bearish = z < -0.8 && breadth <= 0.34 && ret_w < 0.0;
    e.source = "1s ticks".to_string();
    e.samples = if min_samples == usize::MAX { 0 } else { min_samples };
    e
}

/// NIFTY heavyweight momentum: weighted 5-minute return, return z-score,
/// volume z-score and breadth. `divergence_pct` is heavies minus index; a
/// positive divergence with volume is the index-arbitrage lead.
fn component_stats(comps: &[CompSeries], index_ret: f64) -> CompEdge {
    const W: [f64; 3] = [0.09, 0.10, 0.07];
    let tick = tick_component_stats(index_ret);
    if tick.available {
        return tick;
    }
    let mut e = CompEdge::default();
    if comps.is_empty() {
        return e;
    }
    let mut ret_sum = 0.0;
    let mut w_sum = 0.0;
    let mut z_sum = 0.0;
    let mut vz_sum = 0.0;
    let mut up = 0usize;
    let mut used = 0usize;
    for (i, c) in comps.iter().enumerate() {
        let cs = &c.candles;
        if cs.len() < 22 {
            continue;
        }
        let n = cs.len();
        let ret = (cs[n - 1].close / cs[n - 2].close - 1.0) * 100.0;
        let rets: Vec<f64> = (n - 21..n)
            .map(|j| (cs[j].close / cs[j - 1].close - 1.0) * 100.0)
            .collect();
        let vols: Vec<f64> = (n - 20..n).map(|j| cs[j].volume.max(0.0)).collect();
        let w = W.get(i).copied().unwrap_or(0.08);
        ret_sum += ret * w;
        w_sum += w;
        z_sum += z_last(&rets) * w;
        vz_sum += z_last(&vols) * w;
        if ret > 0.0 {
            up += 1;
        }
        used += 1;
        e.names.push(c.name.to_string());
    }
    if used == 0 || w_sum <= 0.0 {
        return e;
    }
    let ret_w = ret_sum / w_sum;
    let z = z_sum / w_sum;
    let vz = vz_sum / w_sum;
    let breadth = up as f64 / used as f64;
    e.available = true;
    e.ret_pct = ret_w;
    e.z = z;
    e.vol_z = vz;
    e.breadth = breadth;
    e.divergence_pct = ret_w - index_ret;
    e.confirmed = vz > 0.5 && breadth >= 0.66 && ret_w > 0.0;
    e.lead = z > 0.8 && breadth >= 0.66 && ret_w > 0.0;
    e.bearish = z < -0.8 && breadth <= 0.34 && ret_w < 0.0;
    e.source = "5min bars".to_string();
    e.samples = used;
    e
}

fn edge_stats(cs: &[Candle], comps: &[CompSeries], spot: f64, a: f64) -> Edge {
    let index_ret = {
        let n = cs.len();
        if n >= 2 {
            (cs[n - 1].close / cs[n - 2].close - 1.0) * 100.0
        } else {
            0.0
        }
    };
    let (bull_fvg, bear_fvg) = fvg_stats(cs, a, spot);
    let (bull_sweep, bear_sweep) = sweep_stats(cs, a);
    Edge {
        range: opening_range(cs),
        bull_sweep,
        bear_sweep,
        bull_fvg,
        bear_fvg,
        vol: volume_stats(cs),
        comp: component_stats(comps, index_ret),
    }
}

fn gap_json(g: &GapEdge) -> Value {
    json!({
        "low": round2(g.low),
        "high": round2(g.high),
        "fillPct": round2(g.fill * 100.0),
        "barsAgo": g.bars_ago,
    })
}

fn sweep_json(s: &SweepEdge) -> Value {
    json!({
        "level": round2(s.level),
        "barsAgo": s.bars_ago,
        "reclaimAtr": round2(s.reclaim_atr),
    })
}

fn chain_stats(chain: &[bs::ChainRow], spot: f64, expiry: &str) -> ChainStats {
    let mut s = ChainStats::default();
    if chain.is_empty() || spot <= 0.0 {
        return s;
    }
    s.has = true;
    let lo = spot * 0.88;
    let hi = spot * 1.12;
    let mut ce_wall = 0.0;
    let mut ce_wall_oi = 0.0;
    let mut pe_wall = 0.0;
    let mut pe_wall_oi = 0.0;
    let mut ivs: Vec<f64> = Vec::new();
    for r in chain {
        if r.strike < lo || r.strike > hi {
            continue;
        }
        s.ce_oi += r.ce.oi.max(0.0);
        s.pe_oi += r.pe.oi.max(0.0);
        s.ce_chg += r.ce.chg_oi;
        s.pe_chg += r.pe.chg_oi;
        if r.ce.iv > 0.0 {
            ivs.push(r.ce.iv);
        }
        if r.pe.iv > 0.0 {
            ivs.push(r.pe.iv);
        }
        if r.strike >= spot && r.ce.oi > ce_wall_oi {
            ce_wall_oi = r.ce.oi;
            ce_wall = r.strike;
        }
        if r.strike <= spot && r.pe.oi > pe_wall_oi {
            pe_wall_oi = r.pe.oi;
            pe_wall = r.strike;
        }
    }
    s.ce_wall = ce_wall;
    s.pe_wall = pe_wall;
    if s.ce_oi > 0.0 && s.pe_oi > 0.0 {
        s.pcr = s.pe_oi / s.ce_oi;
        s.liquid = s.ce_oi.min(s.pe_oi) >= 500.0;
    }
    // Max pain: the strike where option buyers lose the most / writers pay the
    // least, a well-known expiry magnet.
    let mut best_pain = f64::MAX;
    for r in chain {
        if r.strike < lo || r.strike > hi {
            continue;
        }
        let k = r.strike;
        let mut pain = 0.0;
        for x in chain {
            if x.strike < lo || x.strike > hi {
                continue;
            }
            pain += x.ce.oi.max(0.0) * (x.strike - k).max(0.0)
                + x.pe.oi.max(0.0) * (k - x.strike).max(0.0);
        }
        if pain < best_pain {
            best_pain = pain;
            s.max_pain = k;
        }
    }
    let mut sorted_iv = ivs;
    let iv = median(&mut sorted_iv);
    let ivp = if iv > 0.0 && iv < 1.0 { iv * 100.0 } else { iv };
    // 25-delta-ish skew and ATM IV: average put IV minus average call IV around
    // the money. The ATM average is the meaningful expected-move input; the
    // median of the whole chain is only the fallback.
    let near: Vec<&bs::ChainRow> = chain
        .iter()
        .filter(|r| (r.strike - spot).abs() <= spot * 0.03)
        .collect();
    let mut near_iv = 0.0;
    if !near.is_empty() {
        let mut pc = 0.0;
        let mut pn = 0.0;
        let mut cc = 0.0;
        let mut cn = 0.0;
        for r in &near {
            if r.pe.iv > 0.0 {
                pc += r.pe.iv;
                pn += 1.0;
            }
            if r.ce.iv > 0.0 {
                cc += r.ce.iv;
                cn += 1.0;
            }
        }
        if pn > 0.0 && cn > 0.0 {
            s.iv_skew = pc / pn - cc / cn;
        }
        if pn + cn > 0.0 {
            near_iv = (pc + cc) / (pn + cn);
        }
    }
    let atm = if near_iv > 0.0 {
        if near_iv < 1.0 { near_iv * 100.0 } else { near_iv }
    } else {
        ivp
    };
    s.atm_iv = atm;
    let t = bs::ttm_years(expiry, market::now_secs());
    if atm > 0.0 {
        s.exp_move = spot * (atm / 100.0) * t.sqrt();
    }
    s
}

struct PatternHit {
    dir: i32,
    target: f64,
    name: &'static str,
    strength: f64,
}

fn patterns(cs: &[Candle], atr_v: f64) -> Vec<PatternHit> {
    let mut out = Vec::new();
    let n = cs.len();
    if n < 3 {
        return out;
    }
    let c1 = cs[n - 1];
    let c2 = cs[n - 2];
    let c3 = cs[n - 3];
    let body = |c: &Candle| (c.close - c.open).abs();
    let range = |c: &Candle| (c.high - c.low).max(0.0001);
    let bull1 = c1.close > c1.open;
    let bull2 = c2.close > c2.open;
    // Engulfing
    if bull1 && !bull2 && c1.close >= c2.open && c1.open <= c2.close && body(&c1) > body(&c2) {
        out.push(PatternHit {
            dir: 1,
            target: c1.close + body(&c1) * 0.8 + atr_v * 0.25,
            name: "bullish engulfing",
            strength: 0.8,
        });
    }
    if !bull1 && bull2 && c1.open >= c2.close && c1.close <= c2.open && body(&c1) > body(&c2) {
        out.push(PatternHit {
            dir: -1,
            target: c1.close - body(&c1) * 0.8 - atr_v * 0.25,
            name: "bearish engulfing",
            strength: 0.8,
        });
    }
    // Pin bars
    let lower = c1.open.min(c1.close) - c1.low;
    let upper = c1.high - c1.open.max(c1.close);
    if lower > body(&c1) * 2.0 && upper < range(&c1) * 0.25 {
        out.push(PatternHit {
            dir: 1,
            target: c1.high + range(&c1) * 0.6,
            name: "hammer / bullish pin",
            strength: 0.7,
        });
    }
    if upper > body(&c1) * 2.0 && lower < range(&c1) * 0.25 {
        out.push(PatternHit {
            dir: -1,
            target: c1.low - range(&c1) * 0.6,
            name: "shooting star / bearish pin",
            strength: 0.7,
        });
    }
    // Inside bar: measured move of the mother bar projected from the break side.
    if c1.high <= c2.high && c1.low >= c2.low {
        let r = range(&c2);
        if c1.close >= c2.close {
            out.push(PatternHit {
                dir: 1,
                target: c2.high + r,
                name: "inside-bar bullish break",
                strength: 0.55,
            });
        } else {
            out.push(PatternHit {
                dir: -1,
                target: c2.low - r,
                name: "inside-bar bearish break",
                strength: 0.55,
            });
        }
    }
    // Morning / evening star (3 bars).
    let b3 = body(&c3);
    let b2 = body(&c2);
    if !bull1 && bull2 && b2 < b3 * 0.5 && c1.close > (c3.open + c3.close) / 2.0 {
        out.push(PatternHit {
            dir: 1,
            target: c1.close + b3,
            name: "morning star",
            strength: 0.75,
        });
    }
    if bull1 && !bull2 && b2 < b3 * 0.5 && c1.close < (c3.open + c3.close) / 2.0 {
        out.push(PatternHit {
            dir: -1,
            target: c1.close - b3,
            name: "evening star",
            strength: 0.75,
        });
    }
    out
}

/// Walk-forward evidence for the SAME rule the live plan uses.
///
/// Live plan geometry (see `compute`): entry on an EMA20/EMA50 trend, target at
/// the nearest structural level at least `max(0.8*ATR, 0.12% spot)` away (falling
/// back to a 1 ATR projection when no level exists), and a stop at 1.15 ATR
/// (never tighter than 1.0 ATR). This function replays exactly that geometry on
/// historical bars, in the requested direction, so the hit-rate it reports is the
/// hit-rate the live rule actually earned - not a different rule.
fn backtest(cs: &[Candle], dir: i64) -> Value {
    let n = cs.len();
    if dir == 0 || n < 140 {
        return json!({ "trades": 0, "wins": 0, "hitRate": 0.0, "avgR": 0.0, "evR": 0.0 });
    }
    let closes: Vec<f64> = cs.iter().map(|c| c.close).collect();
    let mut trades = 0usize;
    let mut wins = 0usize;
    let mut r_sum = 0.0f64;
    let mut i = 60usize;
    while i + 60 < n {
        let e20 = ema_last(&closes[..i], 20);
        let e50 = ema_last(&closes[..i], 50);
        let a = atr(&cs[..i], 14);
        if a <= 0.0 || e20 <= 0.0 || e50 <= 0.0 {
            i += 3;
            continue;
        }
        let entry = closes[i - 1];
        let (trend_ok, min_dist) = if dir > 0 {
            (e20 > e50 && entry > e20, (a * 0.8).max(entry * 0.0012))
        } else {
            (e20 < e50 && entry < e20, (a * 0.8).max(entry * 0.0012))
        };
        if !trend_ok {
            i += 3;
            continue;
        }
        // Nearest structural level in the trade direction beyond min_dist, the
        // same "swing pivot" the live candidate set is built from.
        let pv = pivots(&cs[..i], 3);
        let structural: Option<f64> = if dir > 0 {
            pv.iter()
                .filter(|p| p.high && p.price > entry + min_dist)
                .map(|p| p.price)
                .reduce(f64::min)
        } else {
            pv.iter()
                .filter(|p| !p.high && p.price < entry - min_dist)
                .map(|p| p.price)
                .reduce(f64::max)
        };
        let target = match structural {
            Some(t) if t.is_finite() => t,
            _ => {
                if dir > 0 {
                    entry + a * 2.0
                } else {
                    entry - a * 2.0
                }
            }
        };
        // Stop mirrors the live 1.15 ATR (>= 1.0 ATR) floor.
        let stop = if dir > 0 { entry - a * 1.15 } else { entry + a * 1.15 };
        let risk = (entry - stop).abs();
        if risk <= 0.0 {
            i += 3;
            continue;
        }
        let reward = (target - entry).abs();
        let r_win = reward / risk;
        trades += 1;
        // 60-bar horizon: the target is now a real structural level (not a
        // 45-bar high), so it needs more room than the old 40 bars.
        for c in &cs[i..(i + 60).min(n)] {
            let (hit_stop, hit_target) = if dir > 0 {
                (c.low <= stop, c.high >= target)
            } else {
                (c.high >= stop, c.low <= target)
            };
            if hit_stop {
                r_sum -= 1.0;
                break;
            }
            if hit_target {
                wins += 1;
                r_sum += r_win;
                break;
            }
        }
        i += 3;
    }
    let hit_rate = if trades > 0 { wins as f64 * 100.0 / trades as f64 } else { 0.0 };
    let avg_r = if trades > 0 { r_sum / trades as f64 } else { 0.0 };
    json!({
        "trades": trades,
        "wins": wins,
        "hitRate": round2(hit_rate),
        "avgR": round2(avg_r),
        "evR": round2(avg_r),
        "direction": dir,
    })
}

fn compute(
    name: &str,
    key: &str,
    sid: i64,
    seg: &str,
    cs: &[Candle],
    chain: &[bs::ChainRow],
    expiry: &str,
    spot: f64,
    direction: &str,
    comps: &[CompSeries],
) -> Value {
    let closes: Vec<f64> = cs.iter().map(|c| c.close).collect();
    let a = atr(cs, 14).max(spot * 0.0004);
    let edge = edge_stats(cs, comps, spot, a);
    let e20 = ema_last(&closes, 20);
    let e50 = ema_last(&closes, 50);
    let e200 = ema_last(&closes, 200);
    let sl = slope(&closes, 20);
    let htf = resample(cs, 3);
    let htf_closes: Vec<f64> = htf.iter().map(|c| c.close).collect();
    let h20 = ema_last(&htf_closes, 20);
    let h50 = ema_last(&htf_closes, 50);

    let mut votes = Vec::new();
    let push_vote = |votes: &mut Vec<Value>, label: &str, bull: bool, bear: bool, detail: String| {
        let side = if bull { 1 } else if bear { -1 } else { 0 };
        votes.push(json!({ "name": label, "side": side, "detail": detail }));
        side as f64
    };
    let mut score = 0.0;
    score += push_vote(&mut votes, "Price vs EMA20", spot > e20, spot < e20, format!("spot {:.2} / EMA20 {:.2}", spot, e20));
    score += push_vote(&mut votes, "EMA20 vs EMA50", e20 > e50, e20 < e50, format!("{:.2} vs {:.2}", e20, e50));
    score += push_vote(&mut votes, "EMA50 vs EMA200", e50 > e200, e50 < e200, format!("{:.2} vs {:.2}", e50, e200));
    score += push_vote(
        &mut votes,
        "Regression slope",
        sl > a * 0.08,
        sl < -a * 0.08,
        format!("{:.3} per bar vs ATR {:.2}", sl, a),
    );
    score += push_vote(
        &mut votes,
        "15-min trend",
        h20 > h50 && h20 > 0.0,
        h20 < h50 && h20 > 0.0,
        format!("HTF EMA20 {:.2} / EMA50 {:.2}", h20, h50),
    );
    if edge.comp.available {
        score += push_vote(
            &mut votes,
            "Heavyweight momentum",
            edge.comp.lead,
            edge.comp.bearish,
            format!(
                "heavies {:+.2}% vs index {:+.2}% (z {:.1}, breadth {:.0}%)",
                edge.comp.ret_pct,
                edge.comp.divergence_pct,
                edge.comp.z,
                edge.comp.breadth * 100.0
            ),
        );
    }
    // Buy-only book: orders are always BUY, but either direction. A bullish
    // setup targets with a BUY CALL, a bearish setup with a BUY PUT. A neutral
    // or conflicting trend yields no target (NO BUY), and an explicit sell
    // request is always refused.
    let trend_strength = (score.abs() / 5.0).min(1.0);
    let flow = direction.trim().to_ascii_lowercase();
    let sell_request = matches!(flow.as_str(), "short" | "bear" | "sell");
    let long_request = matches!(flow.as_str(), "long" | "bull" | "buy");
    let dir = if sell_request {
        0
    } else if long_request || score >= 1.0 {
        1
    } else if score <= -1.0 {
        -1
    } else {
        0
    };

    let (day_hi, day_lo, vwap) = session_stats(cs);
    let mut cands: Vec<Candidate> = Vec::new();
    let mut medv = cs[cs.len().saturating_sub(80)..].iter().map(|c| c.volume).collect::<Vec<_>>();
    let mv = median(&mut medv);

    for p in pivots(cs, 3).into_iter().rev().take(40) {
        let recency = 1.0 + (1.0 - (cs.len() - p.idx) as f64 / cs.len() as f64) * 0.8;
        let vol_bonus = if mv > 0.0 && p.vol > mv * 1.5 { 0.5 } else { 0.0 };
        let touches = pivots(cs, 3)
            .iter()
            .filter(|q| q.high == p.high && (q.price - p.price).abs() <= a * 0.2)
            .count() as f64;
        let w = (1.0 + touches.min(4.0) * 0.35 + vol_bonus) * recency;
        cands.push(Candidate {
            price: p.price,
            weight: w.min(3.5),
            kind: if p.high { "swing high" } else { "swing low" },
            detail: format!("{} touches near {:.2}", touches as i64, p.price),
            dir: if p.high { -1 } else { 1 },
        });
    }
    if day_hi > 0.0 {
        cands.push(Candidate { price: day_hi, weight: 1.5, kind: "session high", detail: "today's high".into(), dir: -1 });
    }
    if day_lo > 0.0 {
        cands.push(Candidate { price: day_lo, weight: 1.5, kind: "session low", detail: "today's low".into(), dir: 1 });
    }
    if vwap > 0.0 {
        cands.push(Candidate { price: vwap, weight: 1.1, kind: "VWAP", detail: "volume weighted average".into(), dir: if spot > vwap { 1 } else { -1 } });
    }
    if e20 > 0.0 {
        cands.push(Candidate { price: e20, weight: 0.6, kind: "EMA20", detail: "dynamic support/resistance".into(), dir: if spot > e20 { 1 } else { -1 } });
    }
    if e50 > 0.0 {
        cands.push(Candidate { price: e50, weight: 0.6, kind: "EMA50", detail: "dynamic support/resistance".into(), dir: if spot > e50 { 1 } else { -1 } });
    }
    let step = round_step(name, spot);
    if step > 0.0 {
        let up = ((spot / step).ceil()) * step;
        let dn = ((spot / step).floor()) * step;
        if up > spot + a * 0.2 {
            cands.push(Candidate { price: up, weight: 0.7, kind: "round strike", detail: format!("{:.0} level", up), dir: -1 });
        }
        if dn > 0.0 && dn < spot - a * 0.2 {
            cands.push(Candidate { price: dn, weight: 0.7, kind: "round strike", detail: format!("{:.0} level", dn), dir: 1 });
        }
    }
    for p in patterns(cs, a) {
        cands.push(Candidate {
            price: p.target,
            weight: 1.0 + p.strength,
            kind: "candle pattern",
            detail: p.name.to_string(),
            dir: p.dir,
        });
    }

    let dh = if expiry.trim().is_empty() { 0.0 } else { bs::ttm_years(expiry, market::now_secs()) * 365.0 };
    let ch = chain_stats(chain, spot, expiry);
    if ch.has {
        if ch.ce_wall > 0.0 {
            cands.push(Candidate { price: ch.ce_wall, weight: 2.4, kind: "CE OI wall", detail: format!("max call OI at {:.0}", ch.ce_wall), dir: -1 });
        }
        if ch.pe_wall > 0.0 {
            cands.push(Candidate { price: ch.pe_wall, weight: 2.4, kind: "PE OI wall", detail: format!("max put OI at {:.0}", ch.pe_wall), dir: 1 });
        }
        if ch.max_pain > 0.0 {
            cands.push(Candidate { price: ch.max_pain, weight: 1.4, kind: "max pain", detail: format!("expiry magnet {:.0}", ch.max_pain), dir: 0 });
        }
        if ch.exp_move > 0.0 {
            cands.push(Candidate { price: spot + ch.exp_move * 0.75, weight: 0.9, kind: "IV expected move", detail: format!("{:.1}% IV move", ch.exp_move / spot * 100.0), dir: 1 });
            cands.push(Candidate { price: spot - ch.exp_move * 0.75, weight: 0.9, kind: "IV expected move", detail: format!("{:.1}% IV move", ch.exp_move / spot * 100.0), dir: -1 });
        }
    }

    // Price-action edge levels: unfilled gaps, reclaimed sweeps and the
    // opening-range measured move.
    if let Some(g) = edge.bull_fvg {
        let size = ((g.high - g.low) / a).min(1.5);
        cands.push(Candidate {
            price: g.high,
            weight: 1.0 + size + if g.bars_ago < 20 { 0.3 } else { 0.0 },
            kind: "bullish FVG support",
            detail: format!("unfilled {:.2}-{:.2} ({:.0}% left)", g.low, g.high, (1.0 - g.fill) * 100.0),
            dir: 1,
        });
        cands.push(Candidate {
            price: g.low,
            weight: 1.0 + size,
            kind: "bearish FVG target",
            detail: format!("downside gap target {:.2}-{:.2} ({:.0}% left)", g.low, g.high, (1.0 - g.fill) * 100.0),
            dir: -1,
        });
    }
    if let Some(g) = edge.bear_fvg {
        let size = ((g.high - g.low) / a).min(1.5);
        cands.push(Candidate {
            price: g.low,
            weight: 1.0 + size,
            kind: "bearish FVG resistance",
            detail: format!("overhead gap {:.2}-{:.2}", g.low, g.high),
            dir: -1,
        });
    }
    if let Some(s) = edge.bull_sweep {
        if s.bars_ago <= 12 {
            cands.push(Candidate {
                price: s.level,
                weight: 1.4 + s.reclaim_atr.min(1.0),
                kind: "liquidity sweep low",
                detail: format!("stop-run below {:.2} reclaimed ({} bars ago)", s.level, s.bars_ago),
                dir: 1,
            });
        }
    }
    if let Some(s) = edge.bear_sweep {
        if s.bars_ago <= 12 {
            cands.push(Candidate {
                price: s.level,
                weight: 1.3,
                kind: "liquidity sweep high",
                detail: format!("stop-run above {:.2} rejected ({} bars ago)", s.level, s.bars_ago),
                dir: -1,
            });
        }
    }
    if edge.range.ready && edge.range.breakout && !edge.range.failed {
        let width = edge.range.hi - edge.range.lo;
        let measured = edge.range.hi + width;
        if measured > spot + a * 0.2 {
            cands.push(Candidate {
                price: measured,
                weight: 1.8,
                kind: "opening range measured move",
                detail: format!("OR {:.0}-{:.0} breakout projection", edge.range.lo, edge.range.hi),
                dir: 1,
            });
        }
        if edge.range.hi > 0.0 {
            cands.push(Candidate {
                price: edge.range.hi,
                weight: 1.2,
                kind: "opening range high",
                detail: format!("OR high {:.0} retest support", edge.range.hi),
                dir: 1,
            });
        }
    }
    if edge.range.ready && edge.range.breakdown && !edge.range.failed_down {
        let width = edge.range.hi - edge.range.lo;
        let measured = edge.range.lo - width;
        if measured > 0.0 && measured < spot - a * 0.2 {
            cands.push(Candidate {
                price: measured,
                weight: 1.8,
                kind: "opening range measured move down",
                detail: format!("OR {:.0}-{:.0} breakdown projection", edge.range.lo, edge.range.hi),
                dir: -1,
            });
        }
        if edge.range.lo > 0.0 {
            cands.push(Candidate {
                price: edge.range.lo,
                weight: 1.2,
                kind: "opening range low",
                detail: format!("OR low {:.0} retest resistance", edge.range.lo),
                dir: -1,
            });
        }
    }

    let min_dist = (a * 0.8).max(spot * 0.0012);
    let mut levels = cands.clone();
    levels.sort_by(|x, y| x.price.partial_cmp(&y.price).unwrap_or(std::cmp::Ordering::Equal));
    let mut clusters: Vec<(f64, f64, Vec<Candidate>)> = Vec::new();
    for c in levels {
        if let Some(last) = clusters.last_mut() {
            let gap = (c.price - last.0).abs();
            if gap <= (a * 0.35).max(spot * 0.0008) {
                let wsum = last.1 + c.weight;
                last.0 = if wsum > 0.0 { (last.0 * last.1 + c.price * c.weight) / wsum } else { last.0 };
                last.1 = wsum;
                last.2.push(c);
                continue;
            }
        }
        clusters.push((c.price, c.weight, vec![c]));
    }

    let side_clusters: Vec<&(f64, f64, Vec<Candidate>)> = clusters
        .iter()
        .filter(|(p, _, _)| match dir {
            1 => *p > spot + min_dist,
            -1 => *p < spot - min_dist,
            _ => false,
        })
        .collect();
    let mut best: Option<&(f64, f64, Vec<Candidate>)> = None;
    let mut best_score = 0.0;
    for cl in &side_clusters {
        let dist = (cl.0 - spot).abs() / a;
        // Target quality gate: a target closer than 0.8*ATR (or 0.12% of spot)
        // is inside normal noise and is eaten by costs. Skip it entirely and
        // look for a real structural level further out.
        if dist < 0.8 {
            continue;
        }
        // A cluster made only of weak single-touch candidates (total weight <
        // 2.0) is not a real structure - penalise it hard so a stronger level
        // further out wins unless there is nothing better.
        let weak_penalty = if cl.1 < 2.0 { 25.0 } else { 0.0 };
        let agree = cl.2.iter().filter(|c| c.dir == dir || c.dir == 0).count() as f64 / cl.2.len().max(1) as f64;
        let utility = cl.1 * 10.0 + agree * 8.0 - ((dist - 1.2).max(0.0)) * 4.0 - weak_penalty;
        if utility > best_score {
            best_score = utility;
            best = Some(cl);
        }
    }

    let mut warnings: Vec<String> = Vec::new();
    let bear_confirmed =
        edge.comp.vol_z > 0.5 && edge.comp.breadth <= 0.34 && edge.comp.ret_pct < 0.0;
    if dir == 0 {
        warnings.push("No buy setup - trend is not directional".into());
    }
    if !ch.has {
        warnings.push("Option chain unavailable - OI/PCR/IV factors skipped".into());
    } else if !ch.liquid {
        warnings.push("Thin option OI - OI walls may be unreliable".into());
    }
    if ch.atm_iv > 60.0 {
        warnings.push(format!("Elevated IV {:.1}% - premium decay risk", ch.atm_iv));
    }
    if dir >= 0 {
        if edge.range.failed {
            warnings.push("Opening-range breakout failed - stand aside".into());
        } else if edge.range.breakout && !edge.range.retest {
            warnings.push("Opening-range breakout without retest - chase risk".into());
        }
        if let Some(s) = edge.bear_sweep {
            if s.bars_ago <= 8 {
                warnings.push(format!("Liquidity sweep above {:.2} rejected - upside trap", s.level));
            }
        }
        if let Some(s) = edge.bull_sweep {
            if s.bars_ago <= 8 {
                warnings.push(format!("Stop-run below {:.2} reclaimed - bullish", s.level));
            }
        }
        if let Some(g) = edge.bear_fvg {
            if g.low - spot < a {
                warnings.push(format!("Bearish FVG overhead at {:.2}-{:.2}", g.low, g.high));
            }
        }
        if edge.vol.buy_pressure < 40.0 {
            warnings.push(format!("Weak buying pressure {:.0}/100", edge.vol.buy_pressure));
        }
        if edge.comp.available && edge.comp.bearish {
            warnings.push("Heavyweights diverging lower - index upside unsupported".into());
        }
        if edge.comp.available && edge.comp.lead && !edge.comp.confirmed {
            warnings.push("Heavyweight buying lacks volume confirmation".into());
        }
    } else {
        if edge.range.failed_down {
            warnings.push("Opening-range breakdown failed - stand aside".into());
        } else if edge.range.breakdown && !edge.range.retest_down {
            warnings.push("Opening-range breakdown without retest - chase risk".into());
        }
        if let Some(s) = edge.bull_sweep {
            if s.bars_ago <= 8 {
                warnings.push(format!("Liquidity sweep below {:.2} reclaimed - downside trap", s.level));
            }
        }
        if let Some(s) = edge.bear_sweep {
            if s.bars_ago <= 8 {
                warnings.push(format!("Stop-run above {:.2} rejected - bearish", s.level));
            }
        }
        if let Some(g) = edge.bull_fvg {
            if spot - g.high < a {
                warnings.push(format!("Bullish FVG support below at {:.2}-{:.2}", g.low, g.high));
            }
        }
        if edge.vol.buy_pressure > 60.0 {
            warnings.push(format!("Weak selling pressure {:.0}/100", 100.0 - edge.vol.buy_pressure));
        }
        if edge.comp.available && edge.comp.lead {
            warnings.push("Heavyweights diverging higher - index downside unsupported".into());
        }
        if edge.comp.available && edge.comp.bearish && !bear_confirmed {
            warnings.push("Heavyweight selling lacks volume confirmation".into());
        }
    }
    if dh > 0.0 && dh <= 1.2 {
        warnings.push("Expiry day - IV and OI behaviour unstable".into());
    }
    if let Some(b) = best {
        let (target, _, group) = b;
        for c in group {
            if dir > 0 && ch.pcr > 1.25 && c.kind == "CE OI wall" {
                warnings.push("PCR disagrees with the buy CALL setup - reduce size".into());
            }
            if dir < 0 && ch.pcr < 0.75 && c.kind == "PE OI wall" {
                warnings.push("PCR disagrees with the buy PUT setup - reduce size".into());
            }
        }
        let move_ = (target - spot).abs();
        if move_ < a {
            warnings.push("Target under 1 ATR - likely eaten by costs".into());
        }
    }

    let (mut target, mut target2, factors, cluster_weight) = match best {
        Some((p, w, group)) => {
            let mut kinds: Vec<Value> = group
                .iter()
                .map(|c| json!({ "kind": c.kind, "detail": c.detail, "weight": round2(c.weight) }))
                .collect();
            kinds.sort_by(|a, b| {
                b["weight"].as_f64().unwrap_or(0.0).partial_cmp(&a["weight"].as_f64().unwrap_or(0.0)).unwrap_or(std::cmp::Ordering::Equal)
            });
            let ext = spot + (p - spot) * 1.618;
            (*p, ext, kinds, *w)
        }
        None => (0.0, 0.0, Vec::new(), 0.0),
    };
    // A one-way trend with no overhead/support structure to cluster still gets
    // an ATR projection so the plan is actionable in both directions.
    if spot > a && dir != 0 && target <= 0.0 {
        target = if dir > 0 { spot + a * 2.0 } else { spot - a * 2.0 };
        target2 = if dir > 0 { spot + a * 3.236 } else { spot - a * 3.236 };
    }

    let mut stop = 0.0;
    let mut rr = 0.0;
    if target > 0.0 && dir != 0 {
        // Stop: 1.15 ATR by default, snapped to a nearby opposite cluster only
        // when it lies in a sane band. Never tighter than 1.0 ATR - a sub-ATR
        // stop is inside normal noise and gets hit before any real move.
        let floor = a * 1.0;
        if dir > 0 {
            let opposite: Vec<&(f64, f64, Vec<Candidate>)> = clusters
                .iter()
                .filter(|(p, _, _)| *p < spot - a * 0.3)
                .collect();
            let mut chosen = spot - a * 1.15;
            let want = opposite.iter().map(|c| c.0).fold(f64::MIN, f64::max);
            if want > spot - a * 1.8 && want < spot - a * 0.35 {
                chosen = want;
            }
            stop = chosen.min(spot - floor);
        } else {
            let opposite: Vec<&(f64, f64, Vec<Candidate>)> = clusters
                .iter()
                .filter(|(p, _, _)| *p > spot + a * 0.3)
                .collect();
            let mut chosen = spot + a * 1.15;
            let want = opposite.iter().map(|c| c.0).fold(f64::MAX, f64::min);
            if want < spot + a * 1.8 && want > spot + a * 0.35 {
                chosen = want;
            }
            stop = chosen.max(spot + floor);
        }
        let risk = (spot - stop).abs();
        if risk > 0.0 {
            rr = (target - spot).abs() / risk;
        }
        if rr < 1.2 {
            warnings.push(format!("Reward:risk {:.2} is below 1.2", rr));
        }
    }

    let bt = backtest(cs, dir as i64);
    let hit_rate = bt.get("hitRate").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let bt_trades = bt.get("trades").and_then(|v| v.as_i64()).unwrap_or(0);
    let bt_ev = bt.get("evR").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let bt_mult = if bt_trades >= 10 {
        (0.9 + (hit_rate / 100.0) * 0.2).clamp(0.85, 1.1)
    } else {
        1.0
    };
    let edge_ok = bt_trades < 10 || bt_ev > 0.0;
    let factor_agree = if factors.is_empty() {
        0.0
    } else {
        factors
            .iter()
            .filter(|f| {
                let k = f.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                if dir > 0 {
                    matches!(k, "swing low" | "PE OI wall" | "session low" | "VWAP" | "candle pattern" | "IV expected move" | "max pain" | "EMA20" | "EMA50" | "round strike" | "bullish FVG support" | "liquidity sweep low" | "opening range measured move" | "opening range high")
                } else {
                    matches!(k, "swing high" | "CE OI wall" | "session high" | "VWAP" | "candle pattern" | "IV expected move" | "max pain" | "EMA20" | "EMA50" | "round strike" | "bearish FVG resistance" | "bearish FVG target" | "liquidity sweep high" | "opening range measured move down" | "opening range low")
                }
            })
            .count() as f64
            / factors.len() as f64
    };
    let sr_score = (cluster_weight / 4.5).min(1.0);
    let data_quality = if !ch.has {
        0.55
    } else if ch.liquid {
        1.0
    } else {
        0.75
    };
    let mut confidence = (34.0 * trend_strength + 30.0 * sr_score + 20.0 * factor_agree + 16.0 * data_quality) * bt_mult;
    if rr < 1.0 {
        confidence *= 0.8;
    }
    let mut edge_bonus = 0.0f64;
    if dir > 0 {
        if edge.range.breakout {
            edge_bonus += if edge.range.retest { 2.5 } else { 1.0 };
        }
        if edge.range.failed {
            edge_bonus -= 3.0;
        }
        if let Some(s) = edge.bull_sweep {
            if s.bars_ago <= 8 {
                edge_bonus += 2.0 * (1.0 + s.reclaim_atr.min(1.0));
            }
        }
        if let Some(s) = edge.bear_sweep {
            if s.bars_ago <= 8 {
                edge_bonus -= 2.0;
            }
        }
        edge_bonus += ((edge.vol.buy_pressure - 50.0) / 25.0).clamp(-2.0, 2.0);
        if edge.comp.lead {
            edge_bonus += 2.0;
        }
        if edge.comp.bearish {
            edge_bonus -= 2.0;
        }
        if edge.comp.lead && edge.comp.confirmed {
            edge_bonus += 1.0;
        }
    } else if dir < 0 {
        if edge.range.breakdown {
            edge_bonus += if edge.range.retest_down { 2.5 } else { 1.0 };
        }
        if edge.range.failed_down {
            edge_bonus -= 3.0;
        }
        if let Some(s) = edge.bear_sweep {
            if s.bars_ago <= 8 {
                edge_bonus += 2.0 * (1.0 + s.reclaim_atr.min(1.0));
            }
        }
        if let Some(s) = edge.bull_sweep {
            if s.bars_ago <= 8 {
                edge_bonus -= 2.0;
            }
        }
        edge_bonus += ((50.0 - edge.vol.buy_pressure) / 25.0).clamp(-2.0, 2.0);
        if edge.comp.bearish {
            edge_bonus += 2.0;
        }
        if edge.comp.lead {
            edge_bonus -= 2.0;
        }
        if edge.comp.bearish && bear_confirmed {
            edge_bonus += 1.0;
        }
    }
    edge_bonus = edge_bonus.clamp(-6.0, 6.0);
    if dir != 0 && target > 0.0 {
        confidence += edge_bonus;
    }
    // Expected-value gate: when there is enough measured evidence, a plan whose
    // walk-forward expected value per trade is not positive is rejected outright
    // (NO BUY) instead of being shown with a misleading confidence. This is what
    // stops structurally losing setups (e.g. RR too small for the hit-rate) from
    // ever being traded.
    let mut rejected_reason: Option<String> = None;
    if dir != 0 && target > 0.0 && bt_trades >= 20 && !edge_ok {
        rejected_reason = Some(format!(
            "Measured EV {:.2}R over {} trades is not positive - plan rejected",
            bt_ev, bt_trades
        ));
        warnings.push(rejected_reason.clone().unwrap_or_default());
    }
    if dir == 0 || target <= 0.0 {
        confidence = 0.0;
        edge_bonus = 0.0;
    }
    let confidence = if rejected_reason.is_some() {
        0.0
    } else {
        confidence.clamp(0.0, 100.0)
    };

    if dir > 0 && ch.has && ch.pcr < 0.75 {
        warnings.push("Low PCR (call-heavy) - upside may be capped".into());
    }
    if dir < 0 && ch.has && ch.pcr > 1.25 {
        warnings.push("High PCR (put-heavy) - downside may be cushioned".into());
    }


    let mut out = json!({
        "ok": target > 0.0 && dir != 0 && rejected_reason.is_none(),
        "rejected": rejected_reason.is_some(),
        "rejectReason": rejected_reason.clone().unwrap_or_default(),
        "underlying": key,
        "symbol": name,
        "securityId": sid,
        "exchangeSegment": seg,
        "asOf": now_ms(),
        "spot": round2(spot),
        "timeframe": "5min",
        "direction": dir,
        "directionText": match dir {
            1 => "BUY CALL",
            -1 => "BUY PUT",
            _ => "NO BUY",
        },
        "target": round2(target),
        "target2": round2(target2),
        "stop": round2(stop),
        "rr": round2(rr),
        "confidence": round2(confidence),
        "atr": round2(a),
        "movePct": if spot > 0.0 && target > 0.0 { round2((target - spot).abs() / spot * 100.0) } else { 0.0 },
        "expiry": expiry,
        "trend": {
            "score": round2(score),
            "strength": round2(trend_strength * 100.0),
            "ema20": round2(e20),
            "ema50": round2(e50),
            "ema200": round2(e200),
            "htfEma20": round2(h20),
            "htfEma50": round2(h50),
            "votes": votes,
        },
        "chain": {
            "available": ch.has,
            "liquid": ch.liquid,
            "pcr": round2(ch.pcr),
            "ceOi": round2(ch.ce_oi),
            "peOi": round2(ch.pe_oi),
            "ceChgOi": round2(ch.ce_chg),
            "peChgOi": round2(ch.pe_chg),
            "ceWall": round2(ch.ce_wall),
            "peWall": round2(ch.pe_wall),
            "maxPain": round2(ch.max_pain),
            "atmIv": round2(ch.atm_iv),
            "ivSkew": round2(ch.iv_skew),
            "expectedMove": round2(ch.exp_move),
            "dteDays": round2(dh),
        },
        "edge": {
            "openingRange": {
                "ready": edge.range.ready,
                "high": round2(edge.range.hi),
                "low": round2(edge.range.lo),
                "breakout": edge.range.breakout,
                "retest": edge.range.retest,
                "failed": edge.range.failed,
                "breakdown": edge.range.breakdown,
                "retestDown": edge.range.retest_down,
                "failedDown": edge.range.failed_down,
            },
            "liquidity": {
                "bullSweep": edge.bull_sweep.as_ref().map(sweep_json),
                "bearSweep": edge.bear_sweep.as_ref().map(sweep_json),
            },
            "fvg": {
                "bullish": edge.bull_fvg.as_ref().map(gap_json),
                "bearish": edge.bear_fvg.as_ref().map(gap_json),
            },
            "volume": {
                "z": round2(edge.vol.z),
                "upShare": round2(edge.vol.up_share * 100.0),
                "buyPressure": round2(edge.vol.buy_pressure),
                "delta": round2(edge.vol.delta),
            },
            "components": {
                "available": edge.comp.available,
                "names": edge.comp.names,
                "lead": edge.comp.lead,
                "bearish": edge.comp.bearish,
                "retPct": round2(edge.comp.ret_pct),
                "z": round2(edge.comp.z),
                "volumeZ": round2(edge.comp.vol_z),
                "breadthPct": round2(edge.comp.breadth * 100.0),
                "divergencePct": round2(edge.comp.divergence_pct),
                "confirmed": edge.comp.confirmed,
                "source": edge.comp.source,
                "samples": edge.comp.samples,
            },
            "bonus": round2(edge_bonus),
        },
        "factors": factors,
        "levels": clusters.iter().map(|(p, w, g)| json!({
            "price": round2(*p),
            "weight": round2(*w),
            "kinds": g.iter().map(|c| c.kind).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "backtest": bt,
        "warnings": warnings,
    });
    if confidence <= 0.0 {
        out["target"] = json!(0.0);
        out["target2"] = json!(0.0);
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(t: i64, o: f64, h: f64, l: f64, c: f64, v: f64) -> Candle {
        Candle { time: t, open: o, high: h, low: l, close: c, volume: v }
    }

    fn trend_series(n: usize, start: f64, step: f64) -> Vec<Candle> {
        (0..n)
            .map(|i| {
                let base = start + step * i as f64;
                candle(i as i64 * 300, base, base + 4.0, base - 3.0, base + 1.0, 1000.0)
            })
            .collect()
    }

    #[test]
    fn research_universe_covers_only_indices_and_mcx() {
        assert!(covers_underlying("NIFTY"));
        assert!(covers_underlying("NIFTY 50"));
        assert!(covers_underlying("NIFTY-Oct2026-25000-CE"));
        assert!(covers_underlying("BANKNIFTY"));
        assert!(covers_underlying("CRUDEOIL-17Nov2026-FUT"));
        assert!(!covers_underlying("RELIANCE"));
        assert!(!covers_underlying("BAJAJ-AUTO"));
        assert!(!covers_underlying(""));
    }

    #[test]
    fn ema_and_slope_follow_an_uptrend() {
        let vals: Vec<f64> = (0..60).map(|i| 100.0 + i as f64).collect();
        assert!(ema_last(&vals, 20) > ema_last(&vals, 50));
        assert!(slope(&vals, 20) > 0.9);
        assert!(slope(&vals, 20) < 1.1);
    }

    #[test]
    fn atr_uses_true_range_and_is_positive() {
        let cs = trend_series(40, 100.0, 1.0);
        let a = atr(&cs, 14);
        assert!(a > 5.0 && a < 9.0, "atr {a}");
    }

    #[test]
    fn swing_pivots_detect_highs_and_lows() {
        let cs = vec![
            candle(0, 10.0, 11.0, 9.0, 10.0, 1.0),
            candle(1, 10.0, 12.0, 9.0, 10.0, 1.0),
            candle(2, 10.0, 15.0, 10.5, 10.0, 1.0),
            candle(3, 10.0, 12.0, 9.0, 10.0, 1.0),
            candle(4, 10.0, 11.0, 9.0, 10.0, 1.0),
            candle(5, 10.0, 11.0, 7.0, 10.0, 1.0),
            candle(6, 10.0, 11.0, 9.0, 10.0, 1.0),
            candle(7, 10.0, 11.0, 9.5, 10.0, 1.0),
        ];
        let p = pivots(&cs, 2);
        assert!(p.iter().any(|x| x.high && (x.price - 15.0).abs() < 0.01));
        assert!(p.iter().any(|x| !x.high && (x.price - 7.0).abs() < 0.01));
    }

    #[test]
    fn uptrend_target_is_above_spot_with_positive_rr() {
        let mut cs = trend_series(220, 1000.0, 0.6);
        // A clean swing high above the last price gives the target a structure.
        let n = cs.len();
        cs[n - 6] = candle(cs[n - 6].time, 1100.0, 1120.0, 1095.0, 1100.0, 3000.0);
        let out = compute("NIFTY 50", "NIFTY", 13, "IDX_I", &cs, &[], "", cs[n - 1].close, "auto", &[]);
        assert_eq!(out["direction"], json!(1));
        assert!(out["target"].as_f64().unwrap_or(0.0) > out["spot"].as_f64().unwrap_or(0.0));
        assert!(out["stop"].as_f64().unwrap_or(0.0) < out["spot"].as_f64().unwrap_or(0.0));
        assert!(out["confidence"].as_f64().unwrap_or(0.0) > 20.0);
    }

    #[test]
    fn downtrend_gets_a_buy_put_target_below_spot() {
        let cs = trend_series(220, 2000.0, -0.7);
        let n = cs.len();
        let out = compute("BANK NIFTY", "BANKNIFTY", 25, "IDX_I", &cs, &[], "", cs[n - 1].close, "auto", &[]);
        assert_eq!(out["direction"], json!(-1));
        assert!(out["target"].as_f64().unwrap_or(0.0) > 0.0);
        assert!(out["target"].as_f64().unwrap_or(0.0) < out["spot"].as_f64().unwrap_or(0.0));
        assert!(out["stop"].as_f64().unwrap_or(0.0) > out["spot"].as_f64().unwrap_or(0.0));
        assert!(out["confidence"].as_f64().unwrap_or(0.0) > 20.0);
        assert_eq!(out["directionText"], json!("BUY PUT"));
    }

    #[test]
    fn sell_request_never_gets_a_target() {
        let cs = trend_series(220, 1000.0, 0.6);
        let n = cs.len();
        let out = compute("NIFTY 50", "NIFTY", 13, "IDX_I", &cs, &[], "", cs[n - 1].close, "sell", &[]);
        assert_eq!(out["direction"], json!(0));
        assert_eq!(out["target"], json!(0.0));
    }

    #[test]
    fn chain_pcr_and_oi_walls_are_detected() {
        let mut chain = Vec::new();
        for (i, strike) in [98.0, 99.0, 100.0, 101.0, 102.0].iter().enumerate() {
            let mut row = bs::ChainRow {
                strike: *strike,
                ..Default::default()
            };
            row.ce.oi = if i == 3 { 5000.0 } else { 1000.0 };
            row.pe.oi = if i == 1 { 4000.0 } else { 1000.0 };
            row.ce.iv = 15.0;
            row.pe.iv = 16.0;
            chain.push(row);
        }
        let s = chain_stats(&chain, 100.0, "2026-11-26");
        assert!(s.has);
        assert!((s.pcr - 8000.0 / 9000.0).abs() < 0.01);
        assert!((s.ce_wall - 101.0).abs() < 0.01);
        assert!((s.pe_wall - 99.0).abs() < 0.01);
        assert!((s.atm_iv - 15.5).abs() < 0.01);
        assert!(s.exp_move > 0.0);
    }

    #[test]
    fn bullish_engulfing_projects_a_target_above_the_bar() {
        let cs = vec![
            candle(0, 100.0, 101.0, 99.0, 100.0, 100.0),
            candle(1, 101.0, 102.0, 97.0, 98.0, 100.0),
            candle(2, 97.0, 103.0, 96.0, 102.0, 200.0),
        ];
        let p = patterns(&cs, 2.0);
        assert!(p.iter().any(|x| x.dir == 1 && x.target > 102.0));
    }

    #[test]
    fn neutral_market_has_no_target() {
        let cs: Vec<Candle> = (0..160)
            .map(|i| {
                let base = 100.0 + ((i as f64) * 0.7).sin() * 0.4;
                candle(i as i64 * 300, base, base + 0.5, base - 0.5, base, 500.0)
            })
            .collect();
        let out = compute("NIFTY 50", "NIFTY", 13, "IDX_I", &cs, &[], "", 100.0, "auto", &[]);
        if out["direction"].as_i64() == Some(0) {
            assert_eq!(out["confidence"], json!(0.0));
            assert_eq!(out["target"], json!(0.0));
        }
    }

    #[test]
    fn detects_an_unfilled_bullish_fair_value_gap() {
        let cs = vec![
            candle(0, 100.0, 100.0, 95.0, 98.0, 100.0),
            candle(1, 98.0, 103.0, 98.0, 102.0, 120.0),
            candle(2, 103.0, 108.0, 104.0, 106.0, 200.0),
            candle(3, 106.0, 109.0, 105.0, 108.0, 180.0),
        ];
        let (bull, bear) = fvg_stats(&cs, 2.0, 110.0);
        let g = bull.expect("bullish gap");
        // The nearest (highest) unfilled gap wins: candle 1 high to candle 3 low.
        assert!((g.low - 103.0).abs() < 0.01 && (g.high - 105.0).abs() < 0.01);
        assert!(g.fill < 0.98);
        assert!(bear.is_none());
    }

    #[test]
    fn detects_a_reclaimed_liquidity_sweep() {
        let mut cs: Vec<Candle> = (0..25)
            .map(|i| candle(i * 300, 100.0, 101.0, 99.0, 100.0, 1000.0))
            .collect();
        cs[24] = candle(24 * 300, 99.0, 103.0, 95.0, 102.0, 4000.0);
        let (bull, bear) = sweep_stats(&cs, 2.0);
        assert!(bull.is_some(), "sweep not detected");
        assert!(bear.is_none());
        let s = bull.unwrap();
        assert!((s.level - 99.0).abs() < 0.01);
        assert_eq!(s.bars_ago, 0);
    }

    #[test]
    fn opening_range_tracks_breakout_and_retest() {
        let day = 20000i64;
        let at = |tod: i64| day * 86400 + tod;
        let cs = vec![
            candle(at(33300), 100.0, 110.0, 100.0, 108.0, 500.0),
            candle(at(33900), 108.0, 112.0, 101.0, 110.0, 500.0),
            candle(at(34500), 110.0, 111.0, 102.0, 109.0, 500.0),
            candle(at(35400), 109.0, 113.0, 103.0, 112.0, 500.0),
            candle(at(36300), 112.0, 116.0, 112.0, 115.0, 900.0),
            candle(at(36900), 115.0, 116.0, 113.0, 115.5, 800.0),
        ];
        let r = opening_range(&cs);
        assert!(r.ready);
        assert!((r.hi - 113.0).abs() < 0.01 && (r.lo - 100.0).abs() < 0.01);
        assert!(r.breakout && r.retest && !r.failed);
    }

    #[test]
    fn opening_range_tracks_breakdown_and_retest() {
        let day = 20000i64;
        let at = |tod: i64| day * 86400 + tod;
        let cs = vec![
            candle(at(33300), 100.0, 110.0, 100.0, 108.0, 500.0),
            candle(at(33900), 108.0, 112.0, 101.0, 110.0, 500.0),
            candle(at(34500), 110.0, 111.0, 102.0, 109.0, 500.0),
            candle(at(35400), 109.0, 113.0, 103.0, 112.0, 500.0),
            candle(at(36300), 105.0, 105.5, 97.0, 98.0, 900.0),
            candle(at(36900), 98.0, 100.2, 97.5, 99.8, 800.0),
        ];
        let r = opening_range(&cs);
        assert!(r.ready);
        assert!((r.hi - 113.0).abs() < 0.01 && (r.lo - 100.0).abs() < 0.01);
        assert!(r.breakdown && r.retest_down && !r.failed_down);
        assert!(!r.breakout);
    }

    #[test]
    fn volume_z_and_buy_pressure_react_to_a_climax() {
        let mut cs: Vec<Candle> = (0..25)
            .map(|i| candle(i * 300, 100.0, 101.0, 99.0, 100.0, 1000.0))
            .collect();
        cs[24] = candle(24 * 300, 100.0, 106.0, 100.0, 106.0, 6000.0);
        let v = volume_stats(&cs);
        assert!(v.z > 2.0, "z {}", v.z);
        assert!(v.buy_pressure > 60.0, "pressure {}", v.buy_pressure);
    }

    #[test]
    fn heavyweights_lead_the_index_with_volume() {
        let mk = |name: &'static str| {
            let mut cs = Vec::new();
            let mut px = 1000.0;
            for i in 0..25 {
                let prev = px;
                px *= if i == 24 { 1.010 } else { 1.001 };
                let v = if i == 24 { 5000.0 } else { 1000.0 };
                cs.push(candle(i * 300, prev, px, prev * 0.999, px, v));
            }
            CompSeries { name, candles: cs }
        };
        let comps = vec![mk("HDFCBANK"), mk("RELIANCE"), mk("ICICIBANK")];
        let c = component_stats(&comps, 0.2);
        assert!(c.available);
        assert!(c.lead && c.confirmed, "{:?}", c);
        assert!(c.breadth > 0.99);
    }

    #[test]
    fn plan_is_cached_and_honours_ttl() {
        let row = json!({
            "underlying": "TESTKEY",
            "symbol": "TEST",
            "securityId": 1,
            "exchangeSegment": "IDX_I",
            "direction": 1,
            "target": 123.0,
            "stop": 100.0,
            "confidence": 70.0,
            "asOf": now_ms(),
        });
        cache_put(row);
        let p = plan_for_underlying("TESTKEY").expect("plan");
        assert_eq!(p["direction"], json!(1));
        assert_eq!(p["target"], json!(123.0));
        let put = json!({
            "underlying": "TESTPUT",
            "symbol": "TESTPUT",
            "securityId": 2,
            "exchangeSegment": "IDX_I",
            "direction": -1,
            "target": 77.0,
            "stop": 100.0,
            "confidence": 70.0,
            "asOf": now_ms(),
        });
        cache_put(put);
        let p = plan_for_underlying("TESTPUT").expect("put plan");
        assert_eq!(p["direction"], json!(-1));
        assert_eq!(p["target"], json!(77.0));
        let stale = json!({
            "underlying": "TESTSTALE", "direction": -1, "target": 9.0,
            "confidence": 80.0, "asOf": now_ms() - PLAN_TTL_MS - 1000,
        });
        cache_put(stale);
        assert!(plan_for_underlying("TESTSTALE").is_none());
    }
}
