//! Shared margin pools across the trading engine tabs.
//!
//! There are TWO pools, one per engine family:
//!   * `paper`    - Paper Trade 1/2/3 + Equity Paper Engine
//!   * `realtime` - Realtime Trading Engine 1/2/3 + Realtime Equity Engine
//!
//! The operator sets each pool's total ONCE, in the app header, so every paper
//! tab draws from the same single paper margin and every live tab from the same
//! single real margin. Each tab may then be allotted a slice of its pool -
//! either a percentage of the pool total or a rupee amount - and that slice is a
//! hard cap: a tab can never lock more margin than its allotment, regardless of
//! its own capital / margin settings.
//!
//! Inactive (checkbox OFF) allotments are inert, so a tab keeps its legacy
//! behaviour. A pool total of `0` disables that pool.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, OnceLock};

use axum::response::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Paper-family engine tabs, sharing one pool.
pub const PAPER_ENGINES: [&str; 7] = ["paper", "paper2", "paper3", "paper4", "autoexp", "aitrader", "equitypaper"];
/// Real/Dhan-family engine tabs, sharing one pool.
pub const REALTIME_ENGINES: [&str; 5] = ["realtime", "realtime2", "realtime3", "aitraderlive", "equitylive"];

/// Every engine tab that participates in a shared pool.
pub const ENGINE_IDS: [&str; 12] = [
    "realtime",
    "realtime2",
    "realtime3",
    "paper",
    "paper2",
    "paper3",
    "paper4",
    "autoexp",
    "aitrader",
    "aitraderlive",
    "equitypaper",
    "equitylive",
];

const STATE_FILE: &str = "algodhan_shared_margin.json";

/// Bankroll seeded for the PAPER pool on a fresh install. Without it a new
/// install starts with an empty paper pool, every paper allotment resolves to
/// Rs 0 and EVERY paper entry is hard-blocked by `margin_block_for_qty` with
/// "no shared margin allotted" - which looks exactly like "auto trading is
/// broken" to a first-time operator. The REALTIME pool is deliberately NOT
/// seeded: real money must come from the Dhan balance, never a guess.
pub const DEFAULT_PAPER_TOTAL: f64 = 1_000_000.0;

/// Which pool an engine draws from.
pub fn group_of(engine: &str) -> &'static str {
    if PAPER_ENGINES.contains(&engine) {
        "paper"
    } else {
        "realtime"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Allot {
    pub enabled: bool,
    /// `pct` = share of the pool total, `rs` = fixed rupee amount.
    pub mode: String,
    pub pct: f64,
    pub amount: f64,
}

impl Default for Allot {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: "pct".into(),
            pct: 0.0,
            amount: 0.0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SharedMargin {
    /// Total margin for the whole PAPER family, shared by every paper tab.
    pub paper_total: f64,
    /// Manual fallback for the REALTIME family total (used only when
    /// `real_source` is `manual`, or before Dhan funds are known).
    pub real_total: f64,
    /// Where the REALTIME pool total comes from: `dhan` (live account available
    /// balance, the default) or `manual`.
    pub real_source: String,
    /// Last available balance fetched from Dhan (real money). Cached so the
    /// pool survives a restart and a brief broker outage.
    pub real_live: f64,
    /// Epoch seconds of the last successful Dhan funds fetch.
    pub real_fetched_at: i64,
    pub engines: BTreeMap<String, Allot>,
    /// One-time marker: the legacy per-tab "Margin Rs / Margin to use %" values
    /// have already been converted into allotments.
    pub legacy_migrated: bool,
    /// One-time marker: the PAPER family has already been seeded with a starting
    /// bankroll + enabled allotments (see [`seed_paper_defaults`]). Without this
    /// an operator who deliberately empties the paper pool would have it
    /// re-seeded at every restart.
    pub paper_seeded: bool,
}

impl Default for SharedMargin {
    fn default() -> Self {
        Self {
            paper_total: 0.0,
            real_total: 0.0,
            real_source: "dhan".into(),
            real_live: 0.0,
            real_fetched_at: 0,
            engines: BTreeMap::new(),
            legacy_migrated: false,
            paper_seeded: false,
        }
    }
}

/// Reading shape: also accepts the old single `total` key so a file written by
/// the previous version upgrades to "both pools = old total".
#[derive(Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
struct RawSharedMargin {
    paper_total: f64,
    real_total: f64,
    real_source: Option<String>,
    real_live: f64,
    real_fetched_at: i64,
    total: f64,
    engines: BTreeMap<String, Allot>,
    legacy_migrated: bool,
    #[serde(default)]
    paper_seeded: bool,
}

impl SharedMargin {
    /// Effective total for a pool. The REALTIME pool is real money: once Dhan
    /// has answered, the live available balance IS the total (even when it is
    /// zero - an empty account must not silently fall back to a manual number).
    /// The manual value is only used before the first successful fetch, or when
    /// the operator explicitly selects manual.
    fn total_of(&self, group: &str) -> f64 {
        if group.eq_ignore_ascii_case("paper") {
            self.paper_total
        } else if self.real_source.eq_ignore_ascii_case("manual") {
            self.real_total
        } else if self.real_fetched_at > 0 {
            self.real_live
        } else {
            self.real_total
        }
    }
}

/// Record a successful Dhan funds fetch. A genuine ₹0 balance is a valid answer
/// (empty account) and is stored as such, so the realtime pool never trades
/// more than the real account holds. Only successful `Ok` fetches reach here.
pub fn set_live_funds(available: f64) {
    if !available.is_finite() || available < 0.0 {
        return;
    }
    let mut s = state();
    let changed = (s.real_live - available).abs() >= 1.0 || s.real_fetched_at == 0;
    let now = now_secs();
    if !changed {
        // Same balance: refresh the age stamp in memory (no disk write).
        s.real_fetched_at = now;
        return;
    }
    s.real_live = available;
    s.real_fetched_at = now;
    persist(&s);
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Rupee cap an allotment grants out of a pool total. `None` only when the
/// allotment is inactive/absent (see [`effective_cap`] for the resolved value an
/// engine actually uses). An active allotment on an empty pool is Rs 0.
pub fn allotment_cap(total: f64, allot: Option<&Allot>) -> Option<f64> {
    let a = allot?;
    if !a.enabled {
        return None;
    }
    if total <= 0.0 {
        return Some(0.0);
    }
    let raw = if a.mode.eq_ignore_ascii_case("rs") {
        a.amount
    } else {
        total * (a.pct.clamp(0.0, 100.0)) / 100.0
    };
    Some(raw.clamp(0.0, total))
}

fn persist(v: &SharedMargin) {
    if let Ok(bytes) = serde_json::to_vec_pretty(v) {
        let path = data_path();
        let tmp = path.with_extension("json.tmp");
        if std::fs::write(&tmp, &bytes).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
    }
}

static SHARED: OnceLock<Mutex<SharedMargin>> = OnceLock::new();

fn state() -> MutexGuard<'static, SharedMargin> {
    SHARED
        .get_or_init(|| Mutex::new(load()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn data_dir() -> PathBuf {
    let dir = std::env::var("ALGODHAN_DATA_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data"));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

fn data_path() -> PathBuf {
    data_dir().join(STATE_FILE)
}

fn load() -> SharedMargin {
    let raw = std::fs::read_to_string(data_path())
        .ok()
        .and_then(|s| serde_json::from_str::<RawSharedMargin>(&s).ok())
        .unwrap_or_default();
    let mut s = SharedMargin {
        paper_total: raw.paper_total,
        real_total: raw.real_total,
        real_source: raw
            .real_source
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "dhan".into()),
        real_live: raw.real_live,
        real_fetched_at: raw.real_fetched_at,
        engines: raw.engines,
        legacy_migrated: raw.legacy_migrated,
        paper_seeded: raw.paper_seeded,
    };
    // Legacy upgrade: a single pool total becomes both group totals.
    if s.paper_total <= 0.0 && s.real_total <= 0.0 && raw.total > 0.0 {
        s.paper_total = raw.total;
        s.real_total = raw.total;
    }
    if !s.legacy_migrated {
        migrate_legacy_allotments(&mut s, &data_dir());
        s.legacy_migrated = true;
        persist(&s);
    }
    // A fresh install (or an install upgraded from a release that shipped an
    // empty paper pool) has no paper bankroll and no paper allotments, so every
    // paper entry is blocked by the margin gate. Seed a sane paper-only starting
    // point ONCE; the operator can change every number afterwards and it is
    // never overwritten again.
    if !s.paper_seeded {
        // Re-seed when the paper family is unusable: no pool total at all, or a
        // funded pool where every single tab is switched OFF (which blocks every
        // paper entry just as hard). A pool with at least one live allotment is
        // left exactly as the operator saved it.
        let any_allotted = PAPER_ENGINES.iter().any(|id| {
            s.engines
                .get(*id)
                .map(|a| a.enabled && promised_rupees(s.paper_total, a) > 0.0)
                .unwrap_or(false)
        });
        if s.paper_total <= 0.0 || !any_allotted {
            seed_paper_defaults(&mut s);
        }
        s.paper_seeded = true;
        persist(&s);
    }
    s
}

/// Give the PAPER family a usable starting state on first run:
///
///   * an empty paper pool total becomes [`DEFAULT_PAPER_TOTAL`]
///   * every paper tab without an active allotment gets `100%` of the pool
///     (they all share one pool, so [`scaled_cap`] splits it back down and the
///     family can never over-commit)
///
/// The REALTIME family is untouched - it must follow the real Dhan balance.
fn seed_paper_defaults(s: &mut SharedMargin) {
    if s.paper_total <= 0.0 {
        s.paper_total = DEFAULT_PAPER_TOTAL;
    }
    // How much of the pool the operator (or a previous seed) has ALREADY
    // promised, as a fraction of the total. A tab counts as "seeded" only when
    // it is enabled AND its promise is positive.
    let mut promised_frac = 0.0;
    let mut need: Vec<&str> = Vec::new();
    for id in PAPER_ENGINES {
        match s.engines.get(id) {
            Some(a) if a.enabled && promised_rupees(s.paper_total, a) > 0.0 => {
                promised_frac += promised_rupees(s.paper_total, a) / s.paper_total;
            }
            _ => need.push(id),
        }
    }
    // Split whatever is LEFT of the pool equally across the tabs that still need
    // an allotment. Dividing the FULL pool by the tab count instead would
    // over-commit whenever an operator already set a manual rupee amount on one
    // tab - `fit_family_to_pool` would then shrink that manual amount, silently
    // changing a value the operator chose. Seeding only the remainder keeps every
    // operator-set allotment exactly as entered.
    if !need.is_empty() {
        let remaining_frac = (1.0 - promised_frac).max(0.0);
        let share = 100.0 * remaining_frac / need.len() as f64;
        for id in need {
            s.engines.insert(
                id.to_string(),
                Allot { enabled: true, mode: "pct".into(), pct: share, amount: 0.0 },
            );
        }
    }
    fit_family_to_pool(s, "paper");
}

/// Engine id -> the durable state file that used to hold its manual margin.
/// Only read ONCE (guarded by `legacy_migrated`) so a later edit is never
/// overwritten.
const LEGACY_FILES: [(&str, &str); 12] = [
    ("realtime", "algodhan_realtime_state.json"),
    ("realtime2", "algodhan_realtime2_state.json"),
    ("realtime3", "algodhan_realtime3_state.json"),
    ("paper", "algodhan_paper_state.json"),
    ("paper2", "algodhan_paper2_state.json"),
    ("paper3", "algodhan_paper3_state.json"),
    ("paper4", "algodhan_paper4_state.json"),
    ("autoexp", "algodhan_autoexp_state.json"),
    ("aitrader", "algodhan_aitrader_state.json"),
    ("aitraderlive", "algodhan_aitraderlive_state.json"),
    ("equitylive", "equity_live_engine.json"),
    ("equitypaper", "equity_paper_engine.json"),
];

/// Convert the legacy per-tab margin settings into shared-margin allotments so
/// an existing book keeps the size the operator actually chose:
///
///   * `marginAmount > 0` -> fixed rupee allotment of that amount
///   * else `marginPct > 0` -> percentage allotment of the same share (so the
///     old `100% of available balance` keeps meaning `100% of the pool`)
///   * equity engines -> fixed rupee allotment of their `capital`
///   * nothing usable -> allotment stays OFF (tab blocked until it is set)
fn migrate_legacy_allotments(s: &mut SharedMargin, dir: &std::path::Path) {
    for (engine, file) in LEGACY_FILES {
        if s.engines.get(engine).map(|a| a.enabled).unwrap_or(false) {
            continue;
        }
        let Some(v) = std::fs::read_to_string(dir.join(file))
            .ok()
            .and_then(|t| serde_json::from_str::<Value>(&t).ok())
        else {
            continue;
        };
        let st = v.get("settings").cloned().unwrap_or(Value::Null);
        let amount = st.get("marginAmount").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let pct = st.get("marginPct").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let capital = st.get("capital").and_then(|x| x.as_f64()).unwrap_or(0.0);
        // Paper tabs historically sized trades from `paperCapital` (their
        // bankroll) and had no margin fields at all. Fall back to it so a tab
        // that traded before keeps trading instead of silently landing OFF.
        let paper_capital = st.get("paperCapital").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let fallback = if group_of(engine) == "paper" { paper_capital } else { 0.0 };
        let allot = if amount > 0.0 {
            Allot { enabled: true, mode: "rs".into(), pct: 0.0, amount }
        } else if pct > 0.0 {
            Allot { enabled: true, mode: "pct".into(), pct: pct.clamp(0.0, 100.0), amount: 0.0 }
        } else if capital > 0.0 {
            Allot { enabled: true, mode: "rs".into(), pct: 0.0, amount: capital }
        } else if fallback > 0.0 {
            Allot { enabled: true, mode: "rs".into(), pct: 0.0, amount: fallback }
        } else {
            continue;
        };
        s.engines.insert(engine.to_string(), allot);
    }
    // The legacy book may promise far more than the NEW pool holds (e.g. every
    // tab was allowed 100% of its own balance). Fit the migrated numbers into
    // the pool once, so the operator starts from a state that is actually
    // savable. Later shortfalls (a shrunken real Dhan balance) are handled at
    // READ time by [`scaled_cap`] and recover automatically.
    for group in ["paper", "realtime"] {
        fit_family_to_pool(s, group);
    }
}

/// Permanently scale a family's saved allotments down so they sum to at most
/// the pool total. Percentages are left alone (they can never exceed the pool);
/// only fixed-rupee allotments are rescaled, proportionally to their raw size.
fn fit_family_to_pool(s: &mut SharedMargin, group: &str) {
    let total = s.total_of(group);
    if total <= 0.0 {
        return;
    }
    let mut pct_promise = 0.0;
    let mut rs_promise = 0.0;
    for id in ENGINE_IDS {
        if group_of(id) != group {
            continue;
        }
        if let Some(a) = s.engines.get(id) {
            if a.enabled {
                if a.mode.eq_ignore_ascii_case("rs") {
                    rs_promise += a.amount.max(0.0);
                } else {
                    pct_promise += promised_rupees(total, a);
                }
            }
        }
    }
    // Percentages can never exceed the pool themselves, so they always keep
    // their meaning; the fixed-rupee allotments absorb whatever they leave.
    let available = (total - pct_promise).max(0.0);
    if rs_promise <= available + 0.5 {
        return;
    }
    let scale = if rs_promise > 0.0 { available / rs_promise } else { 0.0 };
    for id in ENGINE_IDS {
        if group_of(id) != group {
            continue;
        }
        if let Some(a) = s.engines.get_mut(id) {
            if a.enabled && a.mode.eq_ignore_ascii_case("rs") {
                a.amount = round2(a.amount * scale);
            }
        }
    }
}

/// Raw rupee promise of one allotment against a pool total, BEFORE clamping.
/// Used by the over-commitment guards so an allotment larger than the pool can
/// never hide itself behind the clamp.
fn promised_rupees(total: f64, allot: &Allot) -> f64 {
    if !allot.enabled {
        return 0.0;
    }
    if allot.mode.eq_ignore_ascii_case("rs") {
        allot.amount.max(0.0)
    } else {
        total * allot.pct.clamp(0.0, 100.0) / 100.0
    }
}

/// Sum of every active allotment in a family, optionally including a candidate
/// that is not stored yet and excluding one engine id.
fn family_promise(
    s: &SharedMargin,
    group: &str,
    total: f64,
    extra: Option<(&str, &Allot)>,
    skip: &str,
) -> f64 {
    let mut sum = 0.0;
    for id in ENGINE_IDS {
        if id == skip || group_of(id) != group {
            continue;
        }
        if let Some(a) = s.engines.get(id) {
            sum += promised_rupees(total, a);
        }
    }
    if let Some((id, a)) = extra {
        if group_of(id) == group {
            sum += promised_rupees(total, a);
        }
    }
    sum
}

/// Hard margin cap for one engine tab from ITS group pool. The shared allotment
/// is the ONLY margin source: an inactive allotment (checkbox OFF) resolves to
/// Rs 0, which blocks every entry for that tab, and an active one resolves to
/// its slice of the family pool (Rs 0 when the pool itself is empty, e.g. an
/// empty real Dhan account). Lock-only, no I/O.
///
/// When a family's allotments promise more than the pool holds (fixed-rupee
/// allotments after the real Dhan balance shrinks), every cap is scaled down
/// proportionally at read time so the family as a whole can never lock more than
/// the pool. Saved allotments are untouched, so the original values come back
/// automatically once the balance recovers.
pub fn cap_for(engine: &str) -> Option<f64> {
    let s = state();
    let group = group_of(engine);
    let total = s.total_of(group);
    Some(scaled_cap(&s, group, total, engine))
}

/// One engine's resolved cap, already scaled to fit the whole family inside the
/// pool total. Always returns a number: 0 means "this tab cannot trade".
fn scaled_cap(s: &SharedMargin, group: &str, total: f64, engine: &str) -> f64 {
    let Some(a) = s.engines.get(engine) else {
        return 0.0;
    };
    if !a.enabled || total <= 0.0 {
        return 0.0;
    }
    let raw = promised_rupees(total, a);
    if raw <= 0.0 {
        return 0.0;
    }
    let promise = family_promise(s, group, total, None, "");
    let cap = if promise > total + 0.5 {
        raw * total / promise
    } else {
        raw
    };
    cap.min(total).max(0.0)
}

/// Resolved cap for a pool total + allotment, without family scaling (used by
/// the pure unit tests and single-allotment maths). Always resolves to a number:
///
///   * inactive allotment (checkbox OFF) -> Rs 0 (tab cannot trade)
///   * active allotment on a funded pool -> the allotted slice, capped at the pool
///   * active allotment on an empty pool -> Rs 0
///
/// The legacy per-tab "Margin Rs / Margin to use % / Dynamic margin" settings
/// are no longer consulted anywhere.
pub fn effective_cap(total: f64, allot: Option<&Allot>) -> Option<f64> {
    match allot {
        Some(a) if a.enabled => Some(allotment_cap(total, Some(a)).unwrap_or(0.0)),
        _ => Some(0.0),
    }
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn engine_caps(s: &SharedMargin) -> Value {
    let mut out = serde_json::Map::new();
    for id in ENGINE_IDS {
        let allot = s.engines.get(id).cloned().unwrap_or_default();
        let group = group_of(id);
        let total = s.total_of(group);
        let cap = Some(scaled_cap(s, group, total, id));
        out.insert(
            id.to_string(),
            json!({
                "group": group,
                "enabled": allot.enabled,
                "mode": allot.mode,
                "pct": allot.pct,
                "amount": allot.amount,
                "cap": cap.map(round2),
            }),
        );
    }
    Value::Object(out)
}

fn group_view(s: &SharedMargin, group: &str, engines: &[&str]) -> Value {
    let total = s.total_of(group);
    let mut used = 0.0;
    for id in engines {
        used += scaled_cap(s, group, total, id);
    }
    let mut out = json!({
        "total": round2(total),
        "allotted": round2(used),
        "allottedPct": round2(if total > 0.0 { used / total * 100.0 } else { 0.0 }),
    });
    if group.eq_ignore_ascii_case("realtime") {
        out["source"] = json!(if s.real_source.eq_ignore_ascii_case("manual") { "manual" } else { "dhan" });
        out["live"] = json!(round2(s.real_live));
        out["manual"] = json!(round2(s.real_total));
        out["fetchedAt"] = json!(s.real_fetched_at);
    }
    out
}

fn view(s: &SharedMargin) -> Value {
    json!({
        "ok": true,
        "paperTotal": round2(s.paper_total),
        "realtimeTotal": round2(s.total_of("realtime")),
        "realtimeSource": if s.real_source.eq_ignore_ascii_case("manual") { "manual" } else { "dhan" },
        "realtimeLive": round2(s.real_live),
        "realtimeFetchedAt": s.real_fetched_at,
        "paper": group_view(s, "paper", &PAPER_ENGINES),
        "realtime": group_view(s, "realtime", &REALTIME_ENGINES),
        "engines": engine_caps(s),
    })
}

pub async fn get() -> Json<Value> {
    Json(view(&state()))
}

/// Plain view of the current shared-margin state for the diagnostics snapshot.
/// Unlike [`view`] this is synchronous and carries no `ok` wrapper, so it can be
/// embedded inside a larger report.
pub fn snapshot_json() -> Value {
    let s = state();
    view(&s)
}

/// Set one pool total. Body: `{ "group": "paper" | "realtime", "total": n }`
/// plus, for the realtime pool, an optional `"source"`: `dhan` (default, live
/// account money) or `manual` (fixed fallback). A body with only `total` (no
/// group) sets both pools, so older callers keep working.
pub async fn set_total(Json(v): Json<Value>) -> Json<Value> {
    let group = v.get("group").and_then(|x| x.as_str()).unwrap_or("");
    let source = v.get("source").and_then(|x| x.as_str()).unwrap_or("");
    let total = v.get("total").and_then(|x| x.as_f64());
    if total.is_none() && source.is_empty() {
        return Json(json!({"error": "total is required"}));
    }
    let mut s = state();
    if let Some(total) = total {
        let clamped = total.clamp(0.0, 1_000_000_000.0);
        if group.eq_ignore_ascii_case("paper") {
            s.paper_total = clamped;
        } else if group.eq_ignore_ascii_case("realtime") {
            s.real_total = clamped;
        } else {
            s.paper_total = clamped;
            s.real_total = clamped;
        }
    }
    if !source.is_empty() && (group.is_empty() || group.eq_ignore_ascii_case("realtime")) {
        s.real_source = if source.eq_ignore_ascii_case("manual") {
            "manual".into()
        } else {
            "dhan".into()
        };
    }
    persist(&s);
    Json(view(&s))
}

pub async fn set_allot(Json(v): Json<Value>) -> Json<Value> {
    let engine = v
        .get("engine")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    if !ENGINE_IDS.contains(&engine.as_str()) {
        return Json(json!({"error": "unknown engine"}));
    }
    let mut s = state();
    let mut allot = s.engines.get(&engine).cloned().unwrap_or_default();
    let previous = allot.clone();
    if let Some(x) = v.get("enabled").and_then(|x| x.as_bool()) {
        allot.enabled = x;
    }
    if let Some(x) = v.get("mode").and_then(|x| x.as_str()) {
        allot.mode = if x.eq_ignore_ascii_case("rs") { "rs".into() } else { "pct".into() };
    }
    if let Some(x) = v.get("pct").and_then(|x| x.as_f64()) {
        allot.pct = x.clamp(0.0, 100.0);
    }
    if let Some(x) = v.get("amount").and_then(|x| x.as_f64()) {
        allot.amount = x.max(0.0);
    }
    // A shared pool must never be over-committed: the live allotments of a
    // family cannot promise more rupees than the pool holds, otherwise several
    // tabs could each spend the same real money. Reject the save with the exact
    // numbers so the operator can fix it.
    let group = group_of(&engine);
    let total = s.total_of(group);
    if total > 0.0 && allot.enabled {
        let after = family_promise(&s, group, total, Some((&engine, &allot)), &engine);
        let before = family_promise(&s, group, total, None, &engine)
            + promised_rupees(total, &previous);
        // Reject only a save that makes over-commitment worse. If the pool
        // already shrank below the promised allotments (e.g. Dhan balance
        // dropped), every decrease/reduction must still be savable.
        if after > total + 0.5 && after > before + 0.5 {
            return Json(json!({
                "error": format!(
                    "over-committed: this family's allotments would total Rs {} but the pool is only Rs {}",
                    round2(after),
                    round2(total)
                )
            }));
        }
    }
    s.engines.insert(engine, allot);
    persist(&s);
    Json(view(&s))
}

/// Test-only: install a specific allotment so an entry-path test can exercise
/// the gate deterministically. Serialised by the caller with `TEST_ALLOT_LOCK`.
#[cfg(test)]
pub fn set_allot_for_test(engine: &str, enabled: bool, mode: &str, pct: f64, amount: f64) {
    let mut s = state();
    s.engines.insert(
        engine.to_string(),
        Allot { enabled, mode: mode.into(), pct, amount },
    );
}

/// Test-only: remove an allotment installed by [`set_allot_for_test`].
#[cfg(test)]
pub fn clear_allot_for_test(engine: &str) {
    state().engines.remove(engine);
}

/// Test-only: set a family pool total (in-memory, not persisted).
#[cfg(test)]
pub fn set_total_for_test(group: &str, total: f64) {
    let mut s = state();
    if group.eq_ignore_ascii_case("paper") {
        s.paper_total = total;
    } else {
        s.real_total = total;
        s.real_source = "manual".into();
    }
}

#[cfg(test)]
pub static TEST_ALLOT_LOCK: Mutex<()> = Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;

    fn allot(enabled: bool, mode: &str, pct: f64, amount: f64) -> Allot {
        Allot {
            enabled,
            mode: mode.into(),
            pct,
            amount,
        }
    }

    #[test]
    fn engines_map_to_their_family_pool() {
        assert_eq!(group_of("paper"), "paper");
        assert_eq!(group_of("paper2"), "paper");
        assert_eq!(group_of("paper3"), "paper");
        assert_eq!(group_of("paper4"), "paper");
        assert_eq!(group_of("autoexp"), "paper");
        assert_eq!(group_of("aitrader"), "paper");
        assert_eq!(group_of("equitypaper"), "paper");
        assert_eq!(group_of("realtime"), "realtime");
        assert_eq!(group_of("realtime2"), "realtime");
        assert_eq!(group_of("realtime3"), "realtime");
        assert_eq!(group_of("aitraderlive"), "realtime");
        assert_eq!(group_of("equitylive"), "realtime");
    }

    #[test]
    fn allotment_cap_only_applies_to_active_allotments() {
        assert_eq!(allotment_cap(100_000.0, None), None);
        assert_eq!(allotment_cap(100_000.0, Some(&allot(false, "pct", 20.0, 0.0))), None);
    }

    #[test]
    fn legacy_manual_margin_converts_to_allotments_once() {
        let dir = std::env::temp_dir().join(format!("sm_mig_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let write = |name: &str, body: &str| {
            std::fs::write(dir.join(name), body).unwrap();
        };
        write(
            "algodhan_paper_state.json",
            r#"{"settings":{"paperCapital":200000,"marginAmount":300000,"marginPct":100}}"#,
        );
        write(
            "algodhan_realtime2_state.json",
            r#"{"settings":{"marginAmount":0,"marginPct":60}}"#,
        );
        write(
            "equity_live_engine.json",
            r#"{"settings":{"capital":750000}}"#,
        );
        let mut s = SharedMargin::default();
        migrate_legacy_allotments(&mut s, &dir);
        // A manual rupee amount becomes a fixed-rupee allotment.
        let paper = s.engines.get("paper").expect("paper migrated");
        assert!(paper.enabled);
        assert_eq!(paper.mode, "rs");
        assert_eq!(paper.amount, 300_000.0);
        // Percentage-only margin becomes a percentage allotment.
        let rt2 = s.engines.get("realtime2").expect("realtime2 migrated");
        assert!(rt2.enabled);
        assert_eq!(rt2.mode, "pct");
        assert_eq!(rt2.pct, 60.0);
        // An equity engine falls back to its capital as a rupee allotment.
        let live = s.engines.get("equitylive").expect("equitylive migrated");
        assert!(live.enabled);
        assert_eq!(live.mode, "rs");
        assert_eq!(live.amount, 750_000.0);
        // Engines without any usable legacy value stay OFF (blocked).
        assert!(s.engines.get("paper2").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn family_promise_uses_raw_amounts_not_clamped_caps() {
        let mut s = SharedMargin { paper_total: 500_000.0, ..Default::default() };
        s.engines.insert("paper".into(), allot(true, "rs", 0.0, 900_000.0));
        s.engines.insert("paper2".into(), allot(true, "rs", 0.0, 100_000.0));
        // Both caps clamp to the 500k pool, but the promise is 1,000,000.
        assert_eq!(family_promise(&s, "paper", 500_000.0, None, ""), 1_000_000.0);
        // Excluding a tab drops its share from the sum.
        assert_eq!(family_promise(&s, "paper", 500_000.0, None, "paper2"), 900_000.0);
    }

    #[test]
    fn migration_fits_over_committed_allotments_into_the_pool() {
        let dir = std::env::temp_dir().join(format!("sm_fit_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(
            dir.join("algodhan_paper_state.json"),
            r#"{"settings":{"marginAmount":400000}}"#,
        )
        .unwrap();
        std::fs::write(
            dir.join("algodhan_paper2_state.json"),
            r#"{"settings":{"marginAmount":600000}}"#,
        )
        .unwrap();
        let mut s = SharedMargin { paper_total: 500_000.0, ..Default::default() };
        migrate_legacy_allotments(&mut s, &dir);
        // 1,000,000 promised against a 5L pool -> fitted proportionally, so the
        // fresh state is already within budget and every tab stays editable.
        assert_eq!(s.engines.get("paper").unwrap().amount, 200_000.0);
        assert_eq!(s.engines.get("paper2").unwrap().amount, 300_000.0);
        assert_eq!(family_promise(&s, "paper", 500_000.0, None, ""), 500_000.0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shrunken_pool_scales_caps_at_read_time_without_rewriting_allotments() {
        let mut s = SharedMargin {
            real_total: 300_000.0,
            real_source: "manual".into(),
            ..Default::default()
        };
        s.engines.insert("realtime2".into(), allot(true, "rs", 0.0, 300_000.0));
        s.engines.insert("realtime3".into(), allot(true, "rs", 0.0, 300_000.0));
        // Saved amounts are untouched...
        assert_eq!(s.engines.get("realtime2").unwrap().amount, 300_000.0);
        // ...but the caps are scaled so the family fits the shrunken 3L pool.
        let c2 = scaled_cap(&s, "realtime", 300_000.0, "realtime2");
        let c3 = scaled_cap(&s, "realtime", 300_000.0, "realtime3");
        assert!((c2 - 150_000.0).abs() < 1.0, "got {c2}");
        assert!((c3 - 150_000.0).abs() < 1.0, "got {c3}");
        assert!(c2 + c3 <= 300_000.0 + 0.5);
    }

    #[test]
    fn inactive_allotment_always_blocks_trading() {
        // The shared allotment is the ONLY margin source now, so an inactive
        // checkbox (the default) must resolve to Rs 0 - blocking - for a funded
        // and an empty pool alike, never a legacy fallback.
        assert_eq!(effective_cap(0.0, None), Some(0.0));
        assert_eq!(effective_cap(0.0, Some(&allot(false, "pct", 20.0, 0.0))), Some(0.0));
        assert_eq!(effective_cap(500_000.0, None), Some(0.0));
        assert_eq!(effective_cap(500_000.0, Some(&allot(false, "pct", 20.0, 0.0))), Some(0.0));
    }

    #[test]
    fn empty_pool_with_active_allotment_caps_at_zero() {
        // Empty real Dhan account: an active allotment must block trading, not
        // fall back to the tab's legacy budget.
        assert_eq!(effective_cap(0.0, Some(&allot(true, "pct", 20.0, 0.0))), Some(0.0));
        assert_eq!(effective_cap(0.0, Some(&allot(true, "rs", 0.0, 50_000.0))), Some(0.0));
        assert_eq!(effective_cap(0.0, Some(&allot(true, "pct", 100.0, 0.0))), Some(0.0));
    }

    #[test]
    fn percent_allotment_is_share_of_total() {
        let a = allot(true, "pct", 20.0, 0.0);
        assert_eq!(allotment_cap(500_000.0, Some(&a)), Some(100_000.0));
        let a = allot(true, "pct", 0.0, 0.0);
        assert_eq!(allotment_cap(500_000.0, Some(&a)), Some(0.0));
        let a = allot(true, "pct", 150.0, 0.0);
        assert_eq!(allotment_cap(500_000.0, Some(&a)), Some(500_000.0));
    }

    #[test]
    fn rupee_allotment_is_capped_by_total() {
        let a = allot(true, "rs", 0.0, 75_000.0);
        assert_eq!(allotment_cap(500_000.0, Some(&a)), Some(75_000.0));
        let a = allot(true, "rs", 0.0, 800_000.0);
        assert_eq!(allotment_cap(500_000.0, Some(&a)), Some(500_000.0));
        let a = allot(true, "RS", 0.0, -5.0);
        assert_eq!(allotment_cap(500_000.0, Some(&a)), Some(0.0));
    }

    #[test]
    fn group_totals_are_independent() {
        let s = SharedMargin {
            paper_total: 100_000.0,
            real_total: 900_000.0,
            real_source: "manual".into(),
            real_live: 0.0,
            real_fetched_at: 0,
            engines: BTreeMap::new(),
            legacy_migrated: true,
            paper_seeded: true,
        };
        assert_eq!(s.total_of("paper"), 100_000.0);
        assert_eq!(s.total_of("realtime"), 900_000.0);
        assert_eq!(s.total_of("anything-else"), 900_000.0);
    }

    #[test]
    fn realtime_pool_prefers_live_dhan_money() {
        let mut s = SharedMargin {
            paper_total: 100_000.0,
            real_total: 900_000.0,
            real_source: "dhan".into(),
            real_live: 125_000.0,
            real_fetched_at: 1,
            engines: BTreeMap::new(),
            legacy_migrated: true,
            paper_seeded: true,
        };
        assert_eq!(s.total_of("realtime"), 125_000.0);
        // Fetched-but-zero real balance is real (empty account): it must win
        // over the manual fallback and never silently enable trading.
        s.real_live = 0.0;
        assert_eq!(s.total_of("realtime"), 0.0);
        // Never fetched -> use the manual fallback.
        s.real_fetched_at = 0;
        assert_eq!(s.total_of("realtime"), 900_000.0);
        // Manual source always wins over the live balance.
        s.real_live = 125_000.0;
        s.real_fetched_at = 1;
        s.real_source = "manual".into();
        assert_eq!(s.total_of("realtime"), 900_000.0);
        // The paper pool is never affected by real money.
        assert_eq!(s.total_of("paper"), 100_000.0);
    }

    #[test]
    fn realtime_allotment_uses_live_balance() {
        let a = allot(true, "pct", 40.0, 0.0);
        let live = 250_000.0;
        assert_eq!(allotment_cap(live, Some(&a)), Some(100_000.0));
    }

    #[test]
    fn fresh_install_seeds_a_usable_paper_pool() {
        // A brand new install has nothing saved: no pool total, no allotments.
        // Every paper tab must still resolve to a positive cap, otherwise the
        // very first paper entry is hard-blocked and auto trading looks broken.
        let mut s = SharedMargin::default();
        seed_paper_defaults(&mut s);
        assert_eq!(s.paper_total, DEFAULT_PAPER_TOTAL);
        for id in PAPER_ENGINES {
            let cap = scaled_cap(&s, "paper", s.paper_total, id);
            assert!(cap > 0.0, "{id} must be able to trade on a fresh install");
        }
        // The paper family never promises more than its pool.
        let promise = family_promise(&s, "paper", s.paper_total, None, "");
        assert!(promise <= s.paper_total + 0.5, "promise {promise}");
        // The realtime family is untouched: real money is never guessed.
        assert_eq!(s.real_total, 0.0);
        for id in REALTIME_ENGINES {
            assert_eq!(scaled_cap(&s, "realtime", 0.0, id), 0.0);
        }
    }

    #[test]
    fn seeding_never_overwrites_an_operator_configured_paper_pool() {
        let mut s = SharedMargin { paper_total: 250_000.0, ..Default::default() };
        s.engines.insert("paper".into(), allot(true, "rs", 0.0, 50_000.0));
        seed_paper_defaults(&mut s);
        assert_eq!(s.paper_total, 250_000.0);
        assert_eq!(s.engines.get("paper").unwrap().amount, 50_000.0);
        // Untouched tabs that had nothing are still seeded so they can trade.
        assert!(scaled_cap(&s, "paper", s.paper_total, "paper2") > 0.0);
        assert!(family_promise(&s, "paper", s.paper_total, None, "") <= 250_000.0 + 0.5);
    }

    #[test]
    fn allotment_defaults_are_off_and_percent_mode() {
        let a = Allot::default();
        assert!(!a.enabled);
        assert_eq!(a.mode, "pct");
        assert_eq!(a.pct, 0.0);
        assert_eq!(a.amount, 0.0);
    }

    #[test]
    fn legacy_single_total_upgrades_to_both_pools() {
        let raw: RawSharedMargin =
            serde_json::from_str(r#"{"total":250000,"engines":{}}"#).unwrap();
        let mut s = SharedMargin {
            paper_total: raw.paper_total,
            real_total: raw.real_total,
            real_source: "dhan".into(),
            real_live: 0.0,
            real_fetched_at: 0,
            engines: raw.engines,
            legacy_migrated: true,
            paper_seeded: true,
        };
        if s.paper_total <= 0.0 && s.real_total <= 0.0 && raw.total > 0.0 {
            s.paper_total = raw.total;
            s.real_total = raw.total;
        }
        assert_eq!(s.paper_total, 250_000.0);
        assert_eq!(s.real_total, 250_000.0);
    }
}
