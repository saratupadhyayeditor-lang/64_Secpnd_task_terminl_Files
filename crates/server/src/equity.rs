//! Free cash-equity research module.
//!
//! Screens the NSE cash universe (series EQ) with only free public sources:
//!
//!   * NSE `EQUITY_L.csv`          - symbol / company / ISIN master
//!   * NSE `sec_bhavdata_full`     - price, turnover and delivery percentage
//!   * Yahoo chart API             - 2y daily candles for technicals/backtest
//!   * Yahoo quoteSummary          - PE/PB/ROE/debt/growth/cash-flow
//!   * NSE share-holdings master   - promoter holding (with change history)
//!   * Dhan option chain           - ATM IV / PCR / OI, F&O names only
//!
//! The funnel is deliberately cheap: liquidity prefilter -> technical score on
//! the survivors -> fundamentals only for the leaders -> option metrics only for
//! the F&O leaders. Every network source is optional; a missing source lowers
//! the confidence instead of dropping the stock.

use axum::extract::{Query, State};
use axum::response::Json;
use algo_core::model::Candle;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use crate::broker::DhanState;
use crate::market;
use crate::optionchain;
use crate::scrip;

const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                  (KHTML, like Gecko) Chrome/126.0 Safari/537.36";
const SCAN_TTL_SECS: i64 = 6 * 3600;
const CHART_CHUNK: usize = 8;
const MAX_UNIVERSE: usize = 900;
const MAX_FUNDAMENTALS: usize = 220;
const MAX_OPTIONS: usize = 20;
const MAX_ROWS: usize = 300;

// ---------------------------------------------------------------------------
// Cache / filter types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Filters {
    pub max_price: f64,
    pub min_turnover_lacs: f64,
    pub min_delivery: f64,
    pub series: String,
    /// "Scan all listed stocks": when ON the liquidity screen is skipped and
    /// every discovered NSE + BSE equity gets a price history, so the research
    /// covers the whole listed market instead of the cap-balanced 900-stock
    /// sample. Expensive (thousands of chart calls), so it stays opt-in and the
    /// UI shows a long-run warning.
    pub scan_all: bool,
    /// "Auto scan" switch. When ON the research refresher re-scans on the
    /// `auto_scan_minutes` cadence instead of the 6-hour TTL, so newly listed /
    /// newly liquid names are picked up without a manual refresh. 0 minutes means
    /// the auto cadence is not active.
    pub auto_scan: bool,
    /// Minutes between automatic scans while `auto_scan` is ON (clamped to a safe
    /// 5-minute floor so a typo cannot hammer the free data sources).
    pub auto_scan_minutes: f64,
    /// "Auto send scanned stocks to trading engines". When ON, every scan that
    /// finishes pushes the stocks it has NOT already sent into both equity
    /// engines' selected-symbol list, and the armed engine buys each new name
    /// while a free slot and shared margin remain.
    pub auto_push: bool,
    /// Symbols already handed to the equity engines, so a re-scan only sends
    /// genuinely NEW names instead of re-pushing the whole screen. Persisted
    /// with the cache so a restart does not repeat the same push.
    pub pushed_symbols: Vec<String>,
}

impl Default for Filters {
    fn default() -> Self {
        Self {
            max_price: 0.0,
            min_turnover_lacs: 3.0,
            min_delivery: 0.0,
            series: "EQ".to_string(),
            scan_all: false,
            auto_scan: false,
            auto_scan_minutes: 60.0,
            auto_push: false,
            pushed_symbols: Vec::new(),
        }
    }
}

/// Clamp the operator's auto-scan interval into a safe range. 0 = auto scan
/// effectively off (the refresher falls back to the 6-hour TTL).
pub fn auto_scan_secs(filters: &Filters) -> i64 {
    if !filters.auto_scan {
        return SCAN_TTL_SECS;
    }
    let mins = filters.auto_scan_minutes.max(5.0).min(24.0 * 60.0);
    (mins * 60.0).round() as i64
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Cache {
    pub as_of: String,
    pub at: i64,
    pub busy: bool,
    pub stage: String,
    pub done: usize,
    pub total: usize,
    pub started_at: i64,
    pub finished_at: i64,
    pub errors: Vec<String>,
    pub note: String,
    pub filters: Filters,
    pub universe: usize,
    pub listed: usize,
    pub nse: usize,
    pub bse: usize,
    pub rows: Vec<Value>,
    pub backtest: Value,
    pub accuracy: Value,
}

static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
static BUSY: AtomicBool = AtomicBool::new(false);
static DONE_AT: AtomicI64 = AtomicI64::new(0);

fn cache() -> &'static Mutex<Cache> {
    CACHE.get_or_init(|| {
        let mut loaded = read_json(&data_path("equity_cache.json"))
            .and_then(|v| serde_json::from_value::<Cache>(v).ok())
            .unwrap_or_default();
        // Older cache files carried the narrow large-cap defaults.
        if loaded.filters.max_price == 5000.0 {
            loaded.filters.max_price = 0.0;
        }
        if loaded.filters.min_turnover_lacs >= 100.0 {
            loaded.filters.min_turnover_lacs = 3.0;
        }
        DONE_AT.store(loaded.finished_at, Ordering::Relaxed);
        Mutex::new(loaded)
    })
}

fn data_path(name: &str) -> PathBuf {
    let dir = PathBuf::from("data");
    let _ = std::fs::create_dir_all(&dir);
    dir.join(name)
}

fn read_json(path: &PathBuf) -> Option<Value> {
    let s = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&s).ok()
}

fn write_json(path: &PathBuf, value: &Value) -> Option<()> {
    let s = serde_json::to_string(value).ok()?;
    std::fs::write(path, s).ok()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn now_secs() -> i64 {
    now_ms() / 1000
}

fn r2(v: f64) -> f64 {
    if v.is_finite() {
        (v * 100.0).round() / 100.0
    } else {
        0.0
    }
}

fn nv(v: Option<f64>) -> Value {
    match v {
        Some(x) if x.is_finite() => json!(r2(x)),
        _ => Value::Null,
    }
}

fn set_stage(c: &mut Cache, stage: &str, done: usize, total: usize) {
    c.stage = stage.to_string();
    c.done = done;
    c.total = total;
}

fn push_error(c: &mut Cache, msg: impl Into<String>) {
    if c.errors.len() < 25 {
        c.errors.push(msg.into());
    }
}

// ---------------------------------------------------------------------------
// Dates
// ---------------------------------------------------------------------------

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64;
    let doy = (153 * mp + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}

fn weekday(days: i64) -> u32 {
    ((days + 4) % 7) as u32
}

fn ddmmyyyy(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{d:02}{m:02}{y}")
}

fn ymd(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

fn parse_ymd(s: &str) -> Option<i64> {
    let mut it = s.split('-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: u32 = it.next()?.parse().ok()?;
    let d: u32 = it.next()?.parse().ok()?;
    Some(days_from_civil(y, m, d))
}

fn today_ist() -> i64 {
    (now_secs() + 19_800) / 86_400
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

fn http() -> Option<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(25))
        .user_agent(UA)
        .build()
        .ok()
}

async fn get_text(client: &reqwest::Client, url: &str) -> Option<String> {
    let resp = client.get(url).header("User-Agent", UA).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.text().await.ok()
}

async fn get_json(client: &reqwest::Client, url: &str) -> Option<Value> {
    let text = get_text(client, url).await?;
    serde_json::from_str(&text).ok()
}

fn parse_num(s: &str) -> Option<f64> {
    let t = s.trim().replace(',', "");
    if t.is_empty() || t == "-" || t.eq_ignore_ascii_case("na") {
        None
    } else {
        t.parse().ok()
    }
}

// ---------------------------------------------------------------------------
// NSE universe + bhavcopy
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
struct Uni {
    /// Trading symbol without the Yahoo suffix (e.g. RELIANCE).
    symbol: String,
    /// Yahoo symbol including the exchange suffix (e.g. RELIANCE.NS / .BO).
    ysym: String,
    name: String,
    exch: String,
    isin: String,
    cap: f64,
    price: f64,
    chg: f64,
    avg3: f64,
    avg10: f64,
    high52: f64,
    low52: f64,
    dma50: f64,
    dma200: f64,
    pe: f64,
    pb: f64,
    div: f64,
    disc: f64,
}

#[derive(Clone, Debug, Default)]
struct Bhav {
    prev_close: f64,
    close: f64,
    volume: f64,
    turnover_lacs: f64,
    deliv_pct: f64,
}

fn skip_name(name: &str) -> bool {
    let n = name.to_uppercase();
    n.contains("ETF")
        || n.contains("BEES")
        || n.contains("MUTUAL FUND")
        || n.contains("GILT")
        || n.contains("LIQUID FUND")
}

/// Yahoo's public screener is the only free source in this environment that
/// enumerates both exchanges with market cap in one shot. BSE's own API and
/// bhavcopy hosts answer 403 from this network, so BSE is discovered here and
/// enriched through the same Yahoo chart/fundamentals calls.
async fn fetch_screener_universe(
    client: &reqwest::Client,
    auth: &(String, String),
    exchange: &str,
    suffix: &str,
    max_pages: usize,
    allow_thin: bool,
) -> Vec<Uni> {
    let mut out = Vec::new();
    let page = 250usize;
    let mut offset = 0usize;
    for _ in 0..max_pages {
        let body = json!({
            "size": page,
            "offset": offset,
            "sortField": "intradaymarketcap",
            "sortType": "DESC",
            "quoteType": "EQUITY",
            "query": {"operator": "AND", "operands": [
                {"operator": "eq", "operands": ["region", "in"]},
                {"operator": "eq", "operands": ["exchange", exchange]}
            ]},
            "userId": "",
            "userIdType": "guid"
        })
        .to_string();
        let url = format!(
            "https://query1.finance.yahoo.com/v1/finance/screener?crumb={}",
            urlencode(&auth.1)
        );
        let resp = client
            .post(&url)
            .header("User-Agent", UA)
            .header("Cookie", &auth.0)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .body(body)
            .send()
            .await;
        let Ok(resp) = resp else { break };
        let Ok(text) = resp.text().await else { break };
        let Ok(v) = serde_json::from_str::<Value>(&text) else {
            break;
        };
        let Some(result) = v.pointer("/finance/result/0") else {
            break;
        };
        let total = result.get("total").and_then(|x| x.as_u64()).unwrap_or(0) as usize;
        let quotes = result
            .get("quotes")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        if quotes.is_empty() {
            break;
        }
        for q in &quotes {
            if q.get("quoteType").and_then(|x| x.as_str()) != Some("EQUITY") {
                continue;
            }
            let Some(ysym) = q.get("symbol").and_then(|x| x.as_str()) else {
                continue;
            };
            if !ysym.to_uppercase().ends_with(suffix) {
                continue;
            }
            let base = ysym[..ysym.len() - suffix.len()].to_uppercase();
            if base.is_empty() || base.contains('-') || base.contains('^') {
                continue;
            }
            let name = q
                .get("longName")
                .or_else(|| q.get("shortName"))
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            if skip_name(&name) {
                continue;
            }
            let num = |key: &str| q.get(key).and_then(|x| x.as_f64()).unwrap_or(0.0);
            let cap = num("marketCap");
            let price = num("regularMarketPrice").max(num("fulldayPrice"));
            let avg3 = num("averageDailyVolume3Month");
            // `scan_all` keeps names the screener reports with a missing cap or
            // volume figure; only a usable price is mandatory.
            if allow_thin {
                if price <= 0.0 {
                    continue;
                }
            } else if cap <= 0.0 || price <= 0.0 || avg3 <= 0.0 {
                continue;
            }
            out.push(Uni {
                symbol: base,
                ysym: ysym.to_uppercase(),
                name,
                exch: if exchange == "NSI" { "NSE" } else { exchange }.to_string(),
                isin: q
                    .get("isin")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string(),
                cap,
                price,
                chg: num("regularMarketChangePercent"),
                avg3,
                avg10: num("averageDailyVolume10Day"),
                high52: num("fiftyTwoWeekHigh"),
                low52: num("fiftyTwoWeekLow"),
                dma50: num("fiftyDayAverage"),
                dma200: num("twoHundredDayAverage"),
                pe: num("trailingPE"),
                pb: num("priceToBook"),
                div: num("dividendYield"),
                disc: 0.0,
            });
        }
        offset += page;
        if offset >= total || offset >= 10_000 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    out
}

async fn fetch_equity_list(client: &reqwest::Client) -> Vec<Uni> {
    let url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv";
    let Some(text) = get_text(client, url).await else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for line in text.lines().skip(1) {
        let f: Vec<&str> = line.split(',').collect();
        if f.len() < 7 {
            continue;
        }
        let symbol = f[0].trim().to_uppercase();
        if symbol.is_empty() {
            continue;
        }
        let name = f[1].trim().to_string();
        if skip_name(&name) {
            continue;
        }
        out.push(Uni {
            symbol: symbol.clone(),
            ysym: format!("{symbol}.NS"),
            name,
            exch: "NSE".to_string(),
            isin: f[6].trim().to_string(),
            disc: 0.0,
            ..Uni::default()
        });
    }
    out
}

fn discovery_score(u: &Uni) -> f64 {
    let mut s = 0.0;
    if u.high52 > 0.0 && u.price > 0.0 {
        let from_high = (u.price / u.high52 - 1.0) * 100.0;
        s += scale(from_high, -45.0, 0.0, 26.0);
    } else {
        s += 8.0;
    }
    if u.dma200 > 0.0 && u.price > u.dma200 {
        s += 14.0;
    }
    if u.dma50 > 0.0 && u.dma200 > 0.0 && u.dma50 > u.dma200 {
        s += 14.0;
    }
    if u.avg3 > 0.0 && u.avg10 > 0.0 {
        s += scale(u.avg10 / u.avg3, 0.8, 1.8, 16.0);
    }
    let cap_cr = u.cap / 1e7;
    s += if (100.0..=5000.0).contains(&cap_cr) {
        20.0
    } else if (5000.0..=50_000.0).contains(&cap_cr) {
        14.0
    } else if (50.0..100.0).contains(&cap_cr) {
        8.0
    } else {
        3.0
    };
    if u.pe > 0.0 && u.pe < 60.0 {
        s += 5.0;
    }
    if u.low52 > 0.0 && u.price > 0.0 {
        s += scale((u.price / u.low52 - 1.0) * 100.0, 20.0, 150.0, 5.0);
    }
    s.clamp(0.0, 100.0)
}

fn pick_universe(mut list: Vec<Uni>) -> Vec<Uni> {
    for u in &mut list {
        u.disc = discovery_score(u);
    }
    let mut buckets: [Vec<Uni>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for u in list {
        let cap_cr = u.cap / 1e7;
        let idx = if cap_cr < 500.0 {
            0
        } else if cap_cr < 5000.0 {
            1
        } else if cap_cr < 50_000.0 {
            2
        } else {
            3
        };
        buckets[idx].push(u);
    }
    for b in &mut buckets {
        b.sort_by(|a, b| b.disc.partial_cmp(&a.disc).unwrap_or(std::cmp::Ordering::Equal));
    }
    let quotas = [220usize, 340usize, 220usize, 120usize];
    let mut out: Vec<Uni> = Vec::new();
    for (i, b) in buckets.iter_mut().enumerate() {
        let n = quotas[i].min(b.len());
        out.extend(b.drain(0..n));
    }
    if out.len() < MAX_UNIVERSE {
        let mut rest: Vec<Uni> = buckets.into_iter().flatten().collect();
        rest.sort_by(|a, b| b.disc.partial_cmp(&a.disc).unwrap_or(std::cmp::Ordering::Equal));
        for u in rest.into_iter().take(MAX_UNIVERSE - out.len()) {
            out.push(u);
        }
    }
    out
}

async fn fetch_bhavcopy(client: &reqwest::Client) -> Option<(HashMap<String, Bhav>, String)> {
    let today = today_ist();
    for back in 0i64..7 {
        let day = today - back;
        let wd = weekday(day);
        if wd == 0 || wd == 6 {
            continue;
        }
        let ds = ddmmyyyy(day);
        let url = format!(
            "https://archives.nseindia.com/products/content/sec_bhavdata_full_{ds}.csv"
        );
        let Some(text) = get_text(client, &url).await else {
            continue;
        };
        let mut map = HashMap::new();
        let mut header: Option<(usize, usize, usize, usize, usize, usize, usize)> = None;
        for line in text.lines() {
            if header.is_none() {
                let cols: Vec<&str> = line.split(',').collect();
                let idx = |name: &str| cols.iter().position(|c| c.trim() == name);
                let sym = idx("SYMBOL");
                let close = idx("CLOSE_PRICE");
                let prev = idx("PREV_CLOSE");
                let vol = idx("TTL_TRD_QNTY");
                let turn = idx("TURNOVER_LACS");
                let deliv = idx("DELIV_PER");
                if let (Some(a), Some(b), Some(c), Some(d), Some(e), Some(f)) =
                    (sym, close, prev, vol, turn, deliv)
                {
                    header = Some((a, b, c, d, e, f, usize::MAX));
                }
                continue;
            }
            let Some((si, ci, pi, vi, ti, di, _)) = header else {
                continue;
            };
            let f: Vec<&str> = line.split(',').collect();
            if f.len() <= si.max(ci).max(pi).max(vi).max(ti).max(di) {
                continue;
            }
            let symbol = f[si].trim().to_uppercase();
            if symbol.is_empty() || symbol == "SYMBOL" {
                continue;
            }
            map.insert(
                symbol,
                Bhav {
                    prev_close: parse_num(f[pi]).unwrap_or(0.0),
                    close: parse_num(f[ci]).unwrap_or(0.0),
                    volume: parse_num(f[vi]).unwrap_or(0.0),
                    turnover_lacs: parse_num(f[ti]).unwrap_or(0.0),
                    deliv_pct: parse_num(f[di]).unwrap_or(0.0),
                },
            );
        }
        if !map.is_empty() {
            return Some((map, ymd(day)));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Yahoo chart + fundamentals
// ---------------------------------------------------------------------------

async fn yahoo_chart(client: &reqwest::Client, symbol: &str, range: &str) -> Vec<Candle> {
    let url = format!(
        "https://query1.finance.yahoo.com/v8/finance/chart/{}?range={}&interval=1d",
        urlencode(symbol),
        range
    );
    let Some(v) = get_json(client, &url).await else {
        return Vec::new();
    };
    let Some(result) = v.pointer("/chart/result/0") else {
        return Vec::new();
    };
    let Some(ts) = result.get("timestamp").and_then(|x| x.as_array()) else {
        return Vec::new();
    };
    let Some(q) = result.pointer("/indicators/quote/0") else {
        return Vec::new();
    };
    let get = |key: &str| q.get(key).and_then(|x| x.as_array());
    let (o, h, l, c, vol) = (
        get("open"),
        get("high"),
        get("low"),
        get("close"),
        get("volume"),
    );
    let (Some(o), Some(h), Some(l), Some(c)) = (o, h, l, c) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(ts.len());
    for i in 0..ts.len() {
        let (Some(close), Some(open), Some(high), Some(low)) = (
            c.get(i).and_then(|x| x.as_f64()),
            o.get(i).and_then(|x| x.as_f64()),
            h.get(i).and_then(|x| x.as_f64()),
            l.get(i).and_then(|x| x.as_f64()),
        ) else {
            continue;
        };
        if close <= 0.0 {
            continue;
        }
        out.push(Candle {
            time: ts[i].as_i64().unwrap_or(0),
            open,
            high,
            low,
            close,
            volume: vol
                .and_then(|v| v.get(i))
                .and_then(|x| x.as_i64())
                .unwrap_or(0) as f64,
        });
    }
    out
}

/// Free real-time fallback for the equity engines' mark-to-market. Yahoo's
/// batch quote endpoint needs the same cookie + crumb the research scan already
/// uses, but returns every symbol in one request (unlike per-symbol charts).
/// Returns `symbol -> regularMarketPrice` for the symbols Yahoo recognised.
pub async fn yahoo_live_prices(items: &[(String, String)]) -> HashMap<String, f64> {
    if items.is_empty() {
        return HashMap::new();
    }
    let mut ysym_for: HashMap<String, String> = HashMap::new();
    let mut ysymbols: Vec<String> = Vec::new();
    for (symbol, exchange) in items {
        let base = symbol.trim().to_uppercase();
        if base.is_empty() || !base.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '&') {
            continue;
        }
        let ys = if exchange.eq_ignore_ascii_case("BSE") {
            format!("{base}.BO")
        } else {
            format!("{base}.NS")
        };
        if !ysym_for.contains_key(&ys) {
            ysym_for.insert(ys.clone(), symbol.clone());
            ysymbols.push(ys);
        }
    }
    if ysymbols.is_empty() {
        return HashMap::new();
    }
    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent(UA)
        .build()
    else {
        return HashMap::new();
    };
    let mut out = HashMap::new();

    // Primary: the batched v7 quote endpoint. Yahoo has been returning 401 for
    // this route without a valid crumb, so the result is treated as best-effort
    // and the per-symbol v8 chart endpoint fills whatever is left.
    if let Some((cookie, crumb)) = yahoo_auth().await {
        for chunk in ysymbols.chunks(50) {
            let url = format!(
                "https://query1.finance.yahoo.com/v7/finance/quote?symbols={}&crumb={}",
                urlencode(&chunk.join(",")),
                urlencode(&crumb)
            );
            let resp = client
                .get(&url)
                .header("User-Agent", UA)
                .header("Cookie", &cookie)
                .send()
                .await;
            let Ok(resp) = resp else { continue };
            if !resp.status().is_success() {
                continue;
            }
            let Ok(v) = resp.json::<Value>().await else { continue };
            let Some(rows) = v.pointer("/quoteResponse/result").and_then(|x| x.as_array()) else {
                continue;
            };
            for q in rows {
                let Some(ys) = q.get("symbol").and_then(|x| x.as_str()) else {
                    continue;
                };
                let px = q
                    .get("regularMarketPrice")
                    .and_then(|x| x.as_f64())
                    .filter(|p| *p > 0.0);
                if let (Some(px), Some(sym)) = (px, ysym_for.get(ys)) {
                    out.insert(sym.clone(), px);
                }
            }
        }
    }

    // Fallback: v8 chart, one symbol per call. Bounded concurrency keeps the
    // open-position refresh cheap while still giving a live LTP for the books.
    let missing: Vec<String> = ysymbols
        .iter()
        .filter(|ys| ysym_for.get(*ys).map(|s| !out.contains_key(s)).unwrap_or(false))
        .cloned()
        .collect();
    if !missing.is_empty() {
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(6));
        let mut tasks = Vec::new();
        for ys in missing {
            let client = client.clone();
            let sem = sem.clone();
            let label = ysym_for.get(&ys).cloned().unwrap_or_default();
            tasks.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.ok()?;
                yahoo_chart_price(&client, &ys).await.map(|px| (label, px))
            }));
        }
        for t in tasks {
            if let Ok(Some((sym, px))) = t.await {
                if !sym.is_empty() && px > 0.0 {
                    out.insert(sym, px);
                }
            }
        }
    }
    out
}

/// Latest traded price from the v8 chart endpoint (the v7 quote route needs a
/// crumb and frequently answers 401). Prefers `regularMarketPrice`, then the
/// last non-null close in the 1-day series.
async fn yahoo_chart_price(client: &reqwest::Client, ysym: &str) -> Option<f64> {
    let url = format!(
        "https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=1d&range=1d",
        urlencode(ysym)
    );
    let resp = client.get(&url).header("User-Agent", UA).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let v: Value = resp.json().await.ok()?;
    let result = v.pointer("/chart/result")?.get(0)?;
    let direct = result
        .pointer("/meta/regularMarketPrice")
        .and_then(|x| x.as_f64())
        .filter(|p| *p > 0.0);
    if direct.is_some() {
        return direct;
    }
    result
        .pointer("/indicators/quote/0/close")
        .and_then(|x| x.as_array())
        .and_then(|arr| {
            arr.iter()
                .rev()
                .find_map(|c| c.as_f64())
                .filter(|p| *p > 0.0)
        })
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn cookie_header(jar: &HashMap<String, String>) -> String {
    jar.iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("; ")
}

async fn collect_cookies(client: &reqwest::Client, start: &str) -> HashMap<String, String> {
    let mut jar = HashMap::new();
    let mut url = start.to_string();
    for _ in 0..5 {
        let Ok(resp) = client.get(&url).header("User-Agent", UA).send().await else {
            break;
        };
        for h in resp.headers().get_all("set-cookie") {
            if let Ok(s) = h.to_str() {
                if let Some(pair) = s.split(';').next() {
                    if let Some((k, v)) = pair.split_once('=') {
                        jar.insert(k.trim().to_string(), v.trim().to_string());
                    }
                }
            }
        }
        let loc = resp
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        match loc {
            Some(l) if l.starts_with("http") => url = l,
            Some(l) => {
                url = if l.starts_with('/') {
                    format!("https://finance.yahoo.com{l}")
                } else {
                    format!("https://finance.yahoo.com/{l}")
                }
            }
            None => break,
        }
    }
    jar
}

async fn yahoo_auth() -> Option<(String, String)> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent(UA)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .ok()?;
    let mut jar = collect_cookies(&client, "https://fc.yahoo.com").await;
    jar.extend(collect_cookies(&client, "https://finance.yahoo.com/quote/RELIANCE.NS").await);
    let cookie = cookie_header(&jar);
    if cookie.is_empty() {
        return None;
    }
    let resp = client
        .get("https://query1.finance.yahoo.com/v1/test/getcrumb")
        .header("User-Agent", UA)
        .header("Cookie", &cookie)
        .send()
        .await
        .ok()?;
    let crumb = resp.text().await.ok()?.trim().to_string();
    if crumb.is_empty() || crumb.len() > 40 || crumb.contains('<') || crumb.contains('{') {
        return None;
    }
    Some((cookie, crumb))
}

fn fundamental_file() -> &'static Mutex<HashMap<String, Value>> {
    static FUNDS: OnceLock<Mutex<HashMap<String, Value>>> = OnceLock::new();
    FUNDS.get_or_init(|| {
        let map = read_json(&data_path("equity_fundamentals.json"))
            .and_then(|v| serde_json::from_value::<HashMap<String, Value>>(v).ok())
            .unwrap_or_default();
        Mutex::new(map)
    })
}

async fn yahoo_fundamentals(
    client: &reqwest::Client,
    auth: &(String, String),
    ysym: &str,
) -> Option<Value> {
    let modules = "financialData,defaultKeyStatistics,summaryDetail,price,assetProfile,\
                   incomeStatementHistory,cashflowStatementHistory,balanceSheetHistory";
    let url = format!(
        "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{}?modules={}&crumb={}",
        urlencode(ysym),
        modules,
        urlencode(&auth.1)
    );
    let resp = client
        .get(&url)
        .header("User-Agent", UA)
        .header("Cookie", &auth.0)
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let text = resp.text().await.ok()?;
    if text.contains("Invalid Cookie") || text.contains("Unauthorized") {
        return None;
    }
    let v: Value = serde_json::from_str(&text).ok()?;
    parse_fundamentals(&v)
}

fn raw(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.get("raw").and_then(|x| x.as_f64()))
        .filter(|x| x.is_finite())
}

fn pget(v: &Value, key: &str) -> Option<f64> {
    v.get(key).and_then(raw)
}

fn cagr(now: f64, then: f64, years: f64) -> Option<f64> {
    if now > 0.0 && then > 0.0 && years > 0.2 {
        Some(((now / then).powf(1.0 / years) - 1.0) * 100.0)
    } else {
        None
    }
}

fn parse_fundamentals(v: &Value) -> Option<Value> {
    let result = v.pointer("/quoteSummary/result/0")?;
    let fin = result.get("financialData").filter(|x| x.is_object());
    let stats = result.get("defaultKeyStatistics").filter(|x| x.is_object());
    let sum = result.get("summaryDetail").filter(|x| x.is_object());
    let price = result.get("price").filter(|x| x.is_object());
    let profile = result.get("assetProfile").filter(|x| x.is_object());
    let pick = |key: &str| -> Option<f64> {
        fin.and_then(|x| pget(x, key))
            .or_else(|| stats.and_then(|x| pget(x, key)))
            .or_else(|| sum.and_then(|x| pget(x, key)))
            .or_else(|| price.and_then(|x| pget(x, key)))
    };

    let income = result
        .pointer("/incomeStatementHistory/incomeStatementHistory")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let mut revenues: Vec<(i64, f64)> = Vec::new();
    let mut profits: Vec<(i64, f64)> = Vec::new();
    for row in &income {
        let date = row
            .pointer("/endDate/fmt")
            .and_then(|x| x.as_str())
            .and_then(parse_ymd);
        let rev = pget(row, "totalRevenue");
        let net = pget(row, "netIncome");
        if let Some(d) = date {
            if let Some(r) = rev {
                revenues.push((d, r));
            }
            if let Some(n) = net {
                profits.push((d, n));
            }
        }
    }
    revenues.sort_by_key(|x| x.0);
    profits.sort_by_key(|x| x.0);
    let span = |v: &[(i64, f64)]| -> Option<f64> {
        if v.len() < 2 {
            return None;
        }
        let years = (v[v.len() - 1].0 - v[0].0) as f64 / 365.25;
        cagr(v[v.len() - 1].1, v[0].1, years)
    };
    let rev_cagr = span(&revenues);
    let profit_cagr = span(&profits);

    let cfo = result
        .pointer("/cashflowStatementHistory/cashflowStatementHistory/0")
        .and_then(|x| pget(x, "totalCashFromOperatingActivities"))
        .or_else(|| {
            result
                .pointer("/cashflowStatementHistory/cashflowStatementHistory/0")
                .and_then(|x| pget(x, "operatingCashflow"))
        });
    let equity = result
        .pointer("/balanceSheetHistory/balanceSheetHistory/0")
        .and_then(|x| pget(x, "totalStockholderEquity"));
    let debt = pick("totalDebt");
    let net_income = profits.last().map(|x| x.1);
    let cfo_pat = match (cfo, net_income) {
        (Some(c), Some(n)) if n > 0.0 => Some(c / n),
        _ => None,
    };

    let roe = pick("returnOnEquity").map(|x| x * 100.0).or_else(|| {
        match (net_income, equity) {
            (Some(n), Some(e)) if e > 0.0 && n > 0.0 => Some(n / e * 100.0),
            _ => None,
        }
    });
    let debt_eq = pick("debtToEquity").map(|x| if x > 10.0 { x / 100.0 } else { x });
    let pe = pick("trailingPE").or_else(|| pick("forwardPE"));
    let pb = pick("priceToBook");
    let market_cap = pick("marketCap");
    let div_yield = pick("dividendYield").map(|x| if x < 1.0 { x * 100.0 } else { x });
    let op_margin = pick("operatingMargins").map(|x| x * 100.0);
    let rev_growth = pick("revenueGrowth").map(|x| x * 100.0);
    let earn_growth = pick("earningsGrowth").map(|x| x * 100.0);
    let fcf = pick("freeCashflow");
    let current_ratio = pick("currentRatio");
    let peg = match (pe, earn_growth) {
        (Some(p), Some(g)) if g > 0.0 => Some(p / g),
        _ => None,
    };

    let sector = profile
        .and_then(|x| x.get("sector"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let industry = profile
        .and_then(|x| x.get("industry"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();

    Some(json!({
        "sector": sector,
        "industry": industry,
        "pe": nv(pe),
        "pb": nv(pb),
        "peg": nv(peg),
        "roe": nv(roe),
        "debtEquity": nv(debt_eq),
        "revenueCagr3": nv(rev_cagr),
        "profitCagr3": nv(profit_cagr),
        "cfoPat": nv(cfo_pat),
        "opMargin": nv(op_margin),
        "revGrowth": nv(rev_growth),
        "earnGrowth": nv(earn_growth),
        "fcf": if fcf.is_some() { json!(1) } else { json!(0) },
        "marketCap": nv(market_cap),
        "divYield": nv(div_yield),
        "currentRatio": nv(current_ratio),
        "debt": nv(debt.map(|d| d / 1e7)),
    }))
}

// ---------------------------------------------------------------------------
// NSE shareholding
// ---------------------------------------------------------------------------

async fn shareholding_master(client: &reqwest::Client) -> HashMap<String, (f64, f64, String)> {
    let url =
        "https://www.nseindia.com/api/corporate-share-holdings-master?index=equities";
    let resp = client
        .get(url)
        .header("User-Agent", UA)
        .header("Accept", "application/json")
        .header("Referer", "https://www.nseindia.com/")
        .send()
        .await;
    let Ok(resp) = resp else {
        return HashMap::new();
    };
    let Ok(text) = resp.text().await else {
        return HashMap::new();
    };
    let Ok(v) = serde_json::from_str::<Value>(&text) else {
        return HashMap::new();
    };
    let arr = if v.is_array() {
        v.as_array()
    } else {
        v.get("data").and_then(|x| x.as_array())
    };
    let Some(arr) = arr else {
        return HashMap::new();
    };
    let mut out = HashMap::new();
    for row in arr {
        let Some(sym) = row.get("symbol").and_then(|x| x.as_str()) else {
            continue;
        };
        let promoter = row
            .get("pr_and_prgrp")
            .and_then(|x| x.as_str())
            .and_then(parse_num)
            .or_else(|| row.get("pr_and_prgrp").and_then(|x| x.as_f64()));
        let public = row
            .get("public_val")
            .and_then(|x| x.as_str())
            .and_then(parse_num)
            .or_else(|| row.get("public_val").and_then(|x| x.as_f64()));
        let date = row
            .get("date")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        if let Some(p) = promoter {
            out.entry(sym.trim().to_uppercase())
                .or_insert((p, public.unwrap_or(0.0), date));
        }
    }
    out
}

fn holding_history() -> &'static Mutex<HashMap<String, (String, f64)>> {
    static H: OnceLock<Mutex<HashMap<String, (String, f64)>>> = OnceLock::new();
    H.get_or_init(|| {
        let map = read_json(&data_path("equity_holdings.json"))
            .and_then(|v| serde_json::from_value::<HashMap<String, (String, f64)>>(v).ok())
            .unwrap_or_default();
        Mutex::new(map)
    })
}

fn promoter_change(symbol: &str, date: &str, current: f64) -> Option<f64> {
    let mut h = holding_history().lock().ok()?;
    let prev = h.get(symbol).cloned();
    if prev.as_ref().map(|p| p.0.as_str()) != Some(date) {
        h.insert(symbol.to_string(), (date.to_string(), current));
    }
    drop(h);
    match prev {
        Some((d, p)) if d != date => Some(current - p),
        _ => None,
    }
}

fn save_holdings() {
    if let Ok(h) = holding_history().lock() {
        let value = serde_json::to_value(&*h).unwrap_or(Value::Null);
        let _ = write_json(&data_path("equity_holdings.json"), &value);
    }
}

// ---------------------------------------------------------------------------
// Option snapshot (Dhan)
// ---------------------------------------------------------------------------

fn iv_history() -> &'static Mutex<HashMap<String, Vec<(i64, f64)>>> {
    static IV: OnceLock<Mutex<HashMap<String, Vec<(i64, f64)>>>> = OnceLock::new();
    IV.get_or_init(|| {
        let map = read_json(&data_path("equity_iv.json"))
            .and_then(|v| serde_json::from_value::<HashMap<String, Vec<(i64, f64)>>>(v).ok())
            .unwrap_or_default();
        Mutex::new(map)
    })
}

fn iv_percentile(symbol: &str, iv: f64) -> Option<f64> {
    let day = today_ist();
    let mut all = iv_history().lock().ok()?;
    let series = all.entry(symbol.to_string()).or_default();
    match series.last_mut() {
        Some(last) if last.0 == day => last.1 = iv,
        _ => series.push((day, iv)),
    }
    while series.len() > 120 {
        series.remove(0);
    }
    drop(all);
    let value = serde_json::to_value(&*iv_history().lock().ok()?).ok()?;
    let _ = write_json(&data_path("equity_iv.json"), &value);

    let hist = iv_history().lock().ok()?;
    let s = hist.get(symbol)?;
    if s.len() < 10 {
        return None;
    }
    let below = s.iter().filter(|x| x.1 < iv).count() as f64;
    Some(below / (s.len() - 1) as f64 * 100.0)
}

async fn option_snapshot(
    dhan: &DhanState,
    symbol: &str,
    spot: f64,
) -> Value {
    let unavailable = json!({"available": false, "fno": false});
    let Some(sc) = scrip::get() else {
        return unavailable;
    };
    let Some(sid) = sc.equity_sid(symbol) else {
        return unavailable;
    };
    let prefix = scrip::fno_underlying(symbol);
    if sc.has_options(&prefix, "NSE") != Some(true) {
        return unavailable;
    }
    let (fno_sid, fno_seg) = sc.resolve_underlying(symbol, sid, "NSE_EQ", "");
    let mut expiries = sc.expiries_for(&prefix, "NSE").unwrap_or_default();
    if expiries.is_empty() {
        if let Ok(list) = dhan.fetch_option_expiries(fno_sid, &fno_seg).await {
            expiries = list;
        }
    }
    let Some(expiry) = expiries.into_iter().find(|e| !e.trim().is_empty()) else {
        return json!({"available": false, "fno": true});
    };
    let Ok(data) = dhan.fetch_option_chain(fno_sid, &fno_seg, &expiry).await else {
        return json!({"available": false, "fno": true, "expiry": expiry});
    };
    let rows = optionchain::dhan_rows(&data, market::now_secs());
    if rows.is_empty() {
        return json!({"available": false, "fno": true, "expiry": expiry});
    }
    let ce_oi: f64 = rows.iter().map(|r| r.ce.oi).sum();
    let pe_oi: f64 = rows.iter().map(|r| r.pe.oi).sum();
    let pcr = if ce_oi > 0.0 { pe_oi / ce_oi } else { 0.0 };
    let atm = rows
        .iter()
        .min_by(|a, b| {
            (a.strike - spot)
                .abs()
                .partial_cmp(&(b.strike - spot).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|r| {
            let ivs: Vec<f64> = [r.ce.iv, r.pe.iv]
                .into_iter()
                .filter(|x| *x > 0.0 && x.is_finite())
                .collect();
            if ivs.is_empty() {
                0.0
            } else {
                ivs.iter().sum::<f64>() / ivs.len() as f64
            }
        })
        .unwrap_or(0.0);
    let pct = if atm > 0.0 { iv_percentile(symbol, atm) } else { None };
    json!({
        "available": true,
        "fno": true,
        "expiry": expiry,
        "atmIv": nv(Some(atm)),
        "ivPct": nv(pct),
        "pcr": nv(Some(pcr)),
        "ceOi": nv(Some(ce_oi)),
        "peOi": nv(Some(pe_oi)),
    })
}

// ---------------------------------------------------------------------------
// Technicals
// ---------------------------------------------------------------------------

fn sma_last(vals: &[f64], n: usize) -> f64 {
    if vals.len() < n || n == 0 {
        return 0.0;
    }
    vals[vals.len() - n..].iter().sum::<f64>() / n as f64
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
        if tr.is_finite() {
            sum += tr;
            count += 1;
        }
        prev_close = c.close;
    }
    if count == 0 {
        0.0
    } else {
        sum / count as f64
    }
}

fn rsi(closes: &[f64], n: usize) -> f64 {
    if closes.len() <= n || n == 0 {
        return 50.0;
    }
    let start = closes.len().saturating_sub(n + 1);
    let mut gain = 0.0;
    let mut loss = 0.0;
    for i in start + 1..closes.len() {
        let d = closes[i] - closes[i - 1];
        if d >= 0.0 {
            gain += d;
        } else {
            loss -= d;
        }
    }
    if loss == 0.0 {
        return 100.0;
    }
    let rs = (gain / n as f64) / (loss / n as f64);
    100.0 - 100.0 / (1.0 + rs)
}

fn scale(v: f64, lo: f64, hi: f64, max: f64) -> f64 {
    if !v.is_finite() || hi <= lo {
        return 0.0;
    }
    ((v - lo) / (hi - lo)).clamp(0.0, 1.0) * max
}

/// Score where a smaller value is better (debt, PE, base depth).
fn inverse(v: f64, good: f64, bad: f64, max: f64) -> f64 {
    if !v.is_finite() || bad <= good {
        return 0.0;
    }
    if v <= good {
        max
    } else if v >= bad {
        0.0
    } else {
        max * (1.0 - (v - good) / (bad - good))
    }
}

fn pattern_names(cs: &[Candle]) -> Vec<String> {
    let mut out = Vec::new();
    if cs.len() < 6 {
        return out;
    }
    let n = cs.len();
    let c = &cs[n - 1];
    let p = &cs[n - 2];
    let body = (c.close - c.open).abs();
    let range = (c.high - c.low).max(1e-9);
    let lower = c.open.min(c.close) - c.low;
    let upper = c.high - c.open.max(c.close);

    if c.close > c.open
        && p.close < p.open
        && c.open <= p.close
        && c.close >= p.open
        && body > 0.0
    {
        out.push("Bullish engulfing".to_string());
    }
    if lower > 2.0 * body && upper < body && body > 0.0 {
        out.push("Hammer".to_string());
    }
    if body / range < 0.25 {
        out.push("Doji / indecision".to_string());
    }
    let prior_high = cs[n - 6..n - 1]
        .iter()
        .map(|x| x.high)
        .fold(0.0f64, f64::max);
    if prior_high > 0.0 && c.close > prior_high {
        out.push("5-bar breakout".to_string());
    }
    out
}

/// Multi-month base structure. Multibaggers usually leave a long, tight base
/// with rising volume rather than a one-week spike, so these metrics carry as
/// much weight as raw momentum in the composite score.
fn base_metrics(cs: &[Candle]) -> Value {
    if cs.len() < 60 {
        return json!({"available": false});
    }
    let n = cs.len();
    let last = cs[n - 1].close;
    let win = &cs[n.saturating_sub(120)..];
    let hi = win.iter().map(|c| c.high).fold(0.0f64, f64::max);
    let lo = win
        .iter()
        .map(|c| c.low)
        .filter(|x| *x > 0.0)
        .fold(f64::MAX, f64::min);
    let depth = if lo.is_finite() && lo > 0.0 {
        (hi / lo - 1.0) * 100.0
    } else {
        0.0
    };
    let tail20 = &cs[n.saturating_sub(20)..];
    let h20 = tail20.iter().map(|c| c.high).fold(0.0f64, f64::max);
    let l20 = tail20
        .iter()
        .map(|c| c.low)
        .filter(|x| *x > 0.0)
        .fold(f64::MAX, f64::min);
    let tight20 = if l20.is_finite() && l20 > 0.0 {
        (h20 / l20 - 1.0) * 100.0
    } else {
        0.0
    };
    let breakout26 = n > 121
        && last
            > cs[n - 121..n - 1]
                .iter()
                .map(|c| c.high)
                .fold(0.0f64, f64::max);
    let closes: Vec<f64> = cs.iter().map(|c| c.close).collect();
    let ma150 = sma_last(&closes, 150);
    let ma150_prev = if closes.len() > 170 {
        sma_last(&closes[..closes.len() - 20], 150)
    } else {
        ma150
    };
    let stage2 = ma150 > 0.0 && last > ma150 && ma150 > ma150_prev;

    let wk = &cs[n.saturating_sub(130)..];
    let wk_hi = wk.iter().map(|c| c.close).fold(0.0f64, f64::max);
    let mut near = 0usize;
    let mut samples = 0usize;
    for c in wk.iter().step_by(5) {
        samples += 1;
        if wk_hi > 0.0 && c.close >= wk_hi * 0.88 {
            near += 1;
        }
    }
    let near_frac = if samples > 0 {
        near as f64 / samples as f64
    } else {
        0.0
    };

    let recent = &cs[n.saturating_sub(50)..];
    let mut up_vol = 0.0;
    let mut down_vol = 0.0;
    for i in 1..recent.len() {
        if recent[i].close >= recent[i - 1].close {
            up_vol += recent[i].volume;
        } else {
            down_vol += recent[i].volume;
        }
    }
    let updown = if down_vol > 0.0 {
        (up_vol / down_vol).clamp(0.0, 10.0)
    } else if up_vol > 0.0 {
        10.0
    } else {
        1.0
    };
    let from_low = if lo.is_finite() && lo > 0.0 {
        (last / lo - 1.0) * 100.0
    } else {
        0.0
    };

    json!({
        "available": true,
        "depth120": nv(Some(depth)),
        "tight20": nv(Some(tight20)),
        "breakout26w": breakout26,
        "stage2": stage2,
        "nearHighFrac": nv(Some(near_frac)),
        "updownVol": nv(Some(updown)),
        "fromBaseLow": nv(Some(from_low)),
    })
}

fn base_score(base: &Value) -> f64 {
    if base.get("available").and_then(|x| x.as_bool()) != Some(true) {
        return 40.0;
    }
    let depth = base.get("depth120").and_then(|x| x.as_f64()).unwrap_or(60.0);
    let tight = base.get("tight20").and_then(|x| x.as_f64()).unwrap_or(18.0);
    let mut s = 0.0;
    s += inverse(depth, 30.0, 90.0, 30.0);
    s += inverse(tight, 8.0, 30.0, 25.0);
    s += if base.get("breakout26w").and_then(|x| x.as_bool()) == Some(true) {
        20.0
    } else {
        0.0
    };
    s += if base.get("stage2").and_then(|x| x.as_bool()) == Some(true) {
        15.0
    } else {
        0.0
    };
    s += scale(
        base.get("nearHighFrac").and_then(|x| x.as_f64()).unwrap_or(0.0),
        0.35,
        1.0,
        10.0,
    );
    s.clamp(0.0, 100.0)
}

/// Technical snapshot + 0..100 score. `bench6` is the benchmark 6-month
/// return in percent (0 when unavailable, which neutralises relative strength).
fn tech(cs: &[Candle], bench6: f64, deliv: f64) -> (f64, Value, Vec<String>) {
    let closes: Vec<f64> = cs.iter().map(|c| c.close).collect();
    let last = closes.last().copied().unwrap_or(0.0);
    let e20 = ema_last(&closes, 20);
    let e50 = ema_last(&closes, 50);
    let e200 = ema_last(&closes, 200);
    let e200_prev = if closes.len() > 20 {
        ema_last(&closes[..closes.len() - 20], 200)
    } else {
        e200
    };
    let ret = |bars: usize| -> f64 {
        if closes.len() > bars && closes[closes.len() - 1 - bars] > 0.0 {
            (last / closes[closes.len() - 1 - bars] - 1.0) * 100.0
        } else {
            0.0
        }
    };
    let r1m = ret(21);
    let r3m = ret(63);
    let r6m = ret(126);
    let r1y = ret(252);
    let rs6 = if bench6.is_finite() { r6m - bench6 } else { r6m };
    let atr14 = atr(cs, 14);
    let atr50 = atr(cs, 50);
    let atr_pct = if last > 0.0 { atr14 / last * 100.0 } else { 0.0 };
    let avg_vol = sma_last(
        &cs.iter().map(|c| c.volume).collect::<Vec<_>>(),
        20,
    );
    let vol_ratio = if avg_vol > 0.0 {
        cs.last().map(|c| c.volume).unwrap_or(0.0) / avg_vol
    } else {
        0.0
    };
    let high52 = cs
        .iter()
        .rev()
        .take(252)
        .map(|c| c.high)
        .fold(0.0f64, f64::max);
    let low52 = cs
        .iter()
        .rev()
        .take(252)
        .map(|c| c.low)
        .filter(|x| *x > 0.0)
        .fold(f64::MAX, f64::min);
    let from_high = if high52 > 0.0 {
        (last / high52 - 1.0) * 100.0
    } else {
        0.0
    };
    let prior20 = cs
        .iter()
        .rev()
        .skip(1)
        .take(20)
        .map(|c| c.high)
        .fold(0.0f64, f64::max);
    let breakout = prior20 > 0.0 && last > prior20;
    let patterns = pattern_names(cs);
    let base = base_metrics(cs);

    let mut score = 0.0;
    score += if e20 > 0.0 && last > e20 { 5.0 } else { 0.0 };
    score += if e20 > e50 { 5.0 } else { 0.0 };
    score += if e200 > 0.0 && e50 > e200 { 5.0 } else { 0.0 };
    score += if e200 > 0.0 && e200 > e200_prev { 5.0 } else { 0.0 };
    score += if e200 > 0.0 && last > e200 { 5.0 } else { 0.0 };
    score += scale(r3m, -10.0, 25.0, 7.0);
    score += scale(r6m, -15.0, 40.0, 7.0);
    score += scale(r1y, -20.0, 60.0, 6.0);
    score += scale(rs6, -15.0, 25.0, 20.0);
    score += scale(vol_ratio, 0.7, 2.0, 8.0);
    score += scale(deliv, 20.0, 60.0, 7.0);
    score += if breakout { 6.0 } else { 0.0 };
    score += scale(from_high, -25.0, 0.0, 5.0);
    score += if atr50 > 0.0 && atr14 / atr50 < 0.9 { 4.0 } else { 0.0 };
    score += (patterns.len().min(2) as f64) * 2.5;
    score = score.clamp(0.0, 100.0);

    let value = json!({
        "ema20": nv(Some(e20)),
        "ema50": nv(Some(e50)),
        "ema200": nv(Some(e200)),
        "rsi": nv(Some(rsi(&closes, 14))),
        "atr": nv(Some(atr14)),
        "atrPct": nv(Some(atr_pct)),
        "ret1m": nv(Some(r1m)),
        "ret3m": nv(Some(r3m)),
        "ret6m": nv(Some(r6m)),
        "ret1y": nv(Some(r1y)),
        "rs6m": nv(Some(rs6)),
        "volRatio": nv(Some(vol_ratio)),
        "high52": nv(Some(high52)),
        "low52": if low52.is_finite() { nv(Some(low52)) } else { Value::Null },
        "fromHighPct": nv(Some(from_high)),
        "breakout20": breakout,
        "deliveryPct": nv(Some(deliv)),
        "base": base,
    });
    (score, value, patterns)
}

// ---------------------------------------------------------------------------
// Scoring
// ---------------------------------------------------------------------------

fn fval(v: &Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| x.as_f64())
}

fn fundamental_score(f: &Value) -> (f64, usize) {
    let rev = fval(f, "revenueCagr3");
    let prof = fval(f, "profitCagr3");
    let roe = fval(f, "roe");
    let de = fval(f, "debtEquity");
    let cfo = fval(f, "cfoPat");
    let pe = fval(f, "pe");
    let peg = fval(f, "peg");
    let opm = fval(f, "opMargin");
    let fcf = fval(f, "fcf");
    let cr = fval(f, "currentRatio");

    let mut keys = 0usize;
    for x in [rev, prof, roe, de, cfo, pe] {
        if x.is_some() {
            keys += 1;
        }
    }

    let mut s = 0.0;
    s += match rev {
        Some(x) => scale(x, 0.0, 25.0, 15.0),
        None => 7.5,
    };
    s += match prof {
        Some(x) => scale(x, 0.0, 25.0, 15.0),
        None => 7.5,
    };
    s += match roe {
        Some(x) => scale(x, 5.0, 25.0, 15.0),
        None => 7.5,
    };
    s += match opm {
        Some(x) => scale(x, 5.0, 22.0, 10.0),
        None => 5.0,
    };
    s += match de {
        Some(x) => scale(-x, -2.0, -0.2, 10.0),
        None => 5.0,
    };
    s += match cr {
        Some(x) => scale(x, 0.8, 2.0, 5.0),
        None => 2.5,
    };
    s += match cfo {
        Some(x) => scale(x, 0.3, 1.2, 15.0),
        None => 7.5,
    };
    s += if fcf == Some(1.0) { 5.0 } else { 0.0 };
    s += match (pe, peg) {
        (_, Some(p)) => scale(-p, -2.5, -0.8, 10.0),
        (Some(p), None) => scale(-p, -80.0, -8.0, 10.0),
        _ => 5.0,
    };
    (s.clamp(0.0, 100.0), keys)
}

fn quality_score(f: &Value, promoter: Option<f64>, change: Option<f64>) -> (f64, bool) {
    let de = fval(f, "debtEquity");
    let cfo = fval(f, "cfoPat");
    let mut s = 0.0f64;
    match promoter {
        Some(p) => {
            s += if p >= 50.0 {
                40.0
            } else if p >= 35.0 {
                32.0
            } else if p >= 25.0 {
                22.0
            } else {
                10.0
            };
        }
        None => s += 20.0,
    }
    match change {
        Some(c) if c >= 0.0 => s += 20.0,
        Some(c) if c >= -1.0 => s += 10.0,
        Some(_) => {}
        None => s += 10.0,
    }
    s += match de {
        Some(x) if x <= 0.3 => 20.0,
        Some(x) if x <= 1.0 => 15.0,
        Some(x) if x <= 2.0 => 8.0,
        Some(_) => 0.0,
        None => 10.0,
    };
    s += match cfo {
        Some(x) if x >= 1.0 => 20.0,
        Some(x) if x >= 0.6 => 12.0,
        Some(_) => 4.0,
        None => 10.0,
    };
    (s.clamp(0.0, 100.0), promoter.is_some())
}

fn liquidity_score(turnover_lacs: f64, deliv: f64) -> f64 {
    let base = if turnover_lacs >= 1000.0 {
        100.0
    } else if turnover_lacs >= 500.0 {
        90.0
    } else if turnover_lacs >= 200.0 {
        78.0
    } else if turnover_lacs >= 100.0 {
        65.0
    } else if turnover_lacs >= 50.0 {
        45.0
    } else if turnover_lacs >= 20.0 {
        25.0
    } else {
        8.0
    };
    (base + scale(deliv, 20.0, 60.0, 8.0)).clamp(0.0, 100.0)
}

fn total_score(fund: f64, tech: f64, qual: f64, liq: f64) -> f64 {
    (fund * 0.45 + tech * 0.30 + qual * 0.15 + liq * 0.10).clamp(0.0, 100.0)
}

fn multibagger_total(
    fund: f64,
    tech: f64,
    qual: f64,
    base: f64,
    val: f64,
    size: f64,
    liq: f64,
) -> f64 {
    (fund * 0.30
        + qual * 0.15
        + tech * 0.15
        + base * 0.15
        + val * 0.10
        + size * 0.10
        + liq * 0.05)
        .clamp(0.0, 100.0)
}

fn valuation_score(f: &Value, u: &Uni) -> f64 {
    let pe = fval(f, "pe").or(if u.pe > 0.0 { Some(u.pe) } else { None });
    let pb = fval(f, "pb").or(if u.pb > 0.0 { Some(u.pb) } else { None });
    let peg = fval(f, "peg");
    let div = fval(f, "divYield").or(if u.div > 0.0 { Some(u.div) } else { None });
    let mut s = 0.0;
    s += match pe {
        Some(x) if x > 0.0 => inverse(x, 10.0, 45.0, 30.0),
        _ => 10.0,
    };
    s += match pb {
        Some(x) if x > 0.0 => inverse(x, 1.5, 7.0, 25.0),
        _ => 8.0,
    };
    s += match peg {
        Some(x) if x > 0.0 => inverse(x, 0.8, 2.5, 30.0),
        _ => 12.0,
    };
    s += match div {
        Some(x) => scale(x, 0.0, 4.0, 15.0),
        None => 0.0,
    };
    s.clamp(0.0, 100.0)
}

fn size_score(u: &Uni) -> f64 {
    let cr = u.cap / 1e7;
    if cr < 50.0 {
        65.0
    } else if cr < 500.0 {
        100.0
    } else if cr < 2000.0 {
        95.0
    } else if cr < 5000.0 {
        85.0
    } else if cr < 20_000.0 {
        70.0
    } else if cr < 50_000.0 {
        50.0
    } else {
        30.0
    }
}

/// A stock can qualify for several multibagger playbooks at once. Every score is
/// 0..100; the UI lets the user rank by any single playbook or by the composite.
fn strategy_scores(
    f: &Value,
    t: &Value,
    u: &Uni,
    tech: f64,
    deliv: f64,
    promoter: Option<f64>,
    change: Option<f64>,
) -> (Value, Vec<String>) {
    let roe = fval(f, "roe");
    let de = fval(f, "debtEquity");
    let rev = fval(f, "revenueCagr3");
    let prof = fval(f, "profitCagr3");
    let rg = fval(f, "revGrowth");
    let eg = fval(f, "earnGrowth");
    let pe = fval(f, "pe");
    let pb = fval(f, "pb");
    let peg = fval(f, "peg");
    let div = fval(f, "divYield");
    let cfo = fval(f, "cfoPat");
    let cap_cr = u.cap / 1e7;
    let near_high = t.get("fromHighPct").and_then(|x| x.as_f64()).unwrap_or(-50.0);
    let rs6 = t.get("rs6m").and_then(|x| x.as_f64()).unwrap_or(0.0);
    let r12 = t.get("ret1y").and_then(|x| x.as_f64()).unwrap_or(0.0);
    let vol_ratio = t.get("volRatio").and_then(|x| x.as_f64()).unwrap_or(1.0);
    let base = t.get("base").cloned().unwrap_or_else(|| json!({}));
    let breakout = base.get("breakout26w").and_then(|x| x.as_bool()) == Some(true);
    let tight = base.get("tight20").and_then(|x| x.as_f64()).unwrap_or(20.0);
    let depth = base.get("depth120").and_then(|x| x.as_f64()).unwrap_or(60.0);
    let updown = base.get("updownVol").and_then(|x| x.as_f64()).unwrap_or(1.0);
    let stage2 = base.get("stage2").and_then(|x| x.as_bool()) == Some(true);

    let mut coffee = 0.0;
    coffee += roe.map(|x| scale(x, 12.0, 28.0, 30.0)).unwrap_or(12.0);
    coffee += de.map(|x| inverse(x, 0.2, 1.5, 20.0)).unwrap_or(8.0);
    coffee += rev.map(|x| scale(x, 8.0, 20.0, 20.0)).unwrap_or(8.0);
    coffee += prof.map(|x| scale(x, 8.0, 22.0, 20.0)).unwrap_or(8.0);
    coffee += if cap_cr >= 1000.0 { 10.0 } else { 4.0 };
    coffee = coffee.clamp(0.0, 100.0);

    let mut garp = 0.0;
    garp += peg
        .map(|x| inverse(x, 1.0, 2.5, 30.0))
        .or_else(|| pe.map(|x| inverse(x, 15.0, 45.0, 30.0)))
        .unwrap_or(10.0);
    garp += rev.map(|x| scale(x, 10.0, 25.0, 20.0)).unwrap_or(8.0);
    garp += prof.map(|x| scale(x, 10.0, 25.0, 20.0)).unwrap_or(8.0);
    garp += roe.map(|x| scale(x, 10.0, 25.0, 15.0)).unwrap_or(6.0);
    garp += de.map(|x| inverse(x, 0.3, 1.5, 15.0)).unwrap_or(6.0);
    garp = garp.clamp(0.0, 100.0);

    let mut micro = 0.0;
    micro += if (100.0..=2500.0).contains(&cap_cr) {
        30.0
    } else if (2500.0..=5000.0).contains(&cap_cr) {
        18.0
    } else {
        4.0
    };
    micro += rev.map(|x| scale(x, 15.0, 30.0, 20.0)).unwrap_or(6.0);
    micro += prof.map(|x| scale(x, 15.0, 35.0, 20.0)).unwrap_or(6.0);
    micro += roe.map(|x| scale(x, 10.0, 25.0, 15.0)).unwrap_or(5.0);
    micro += if breakout { 8.0 } else { 0.0 };
    micro += inverse(tight, 8.0, 25.0, 7.0);
    micro = micro.clamp(0.0, 100.0);

    let mut turn = 0.0;
    turn += rg.map(|x| scale(x, 0.0, 25.0, 20.0)).unwrap_or(6.0);
    turn += eg.map(|x| scale(x, 0.0, 40.0, 25.0)).unwrap_or(8.0);
    turn += if prof.map(|x| x < 12.0).unwrap_or(false)
        && eg
            .map(|e| rg.map(|r| e > r).unwrap_or(true))
            .unwrap_or(false)
    {
        15.0
    } else {
        0.0
    };
    turn += if stage2 { 15.0 } else { 3.0 };
    turn += inverse(depth, 25.0, 70.0, 20.0);
    turn = turn.clamp(0.0, 100.0);

    let mut value = 0.0;
    value += pe.map(|x| inverse(x, 10.0, 35.0, 25.0)).unwrap_or(8.0);
    value += pb.map(|x| inverse(x, 1.5, 5.0, 20.0)).unwrap_or(6.0);
    value += div.map(|x| scale(x, 0.0, 4.0, 15.0)).unwrap_or(0.0);
    value += de.map(|x| inverse(x, 0.3, 1.5, 15.0)).unwrap_or(6.0);
    value += if prof.unwrap_or(0.0) > 0.0 { 10.0 } else { 0.0 };
    value += if stage2 { 15.0 } else { 0.0 };
    value = value.clamp(0.0, 100.0);

    let mut mom = 0.0;
    mom += scale(rs6, 0.0, 30.0, 25.0);
    mom += scale(near_high, -25.0, 0.0, 20.0);
    mom += if breakout { 15.0 } else { 0.0 };
    mom += scale(vol_ratio, 0.9, 2.0, 15.0);
    mom += scale(r12, 0.0, 60.0, 15.0);
    mom += if stage2 { 10.0 } else { 0.0 };
    mom = mom.clamp(0.0, 100.0);

    let mut acc = 0.0;
    acc += if deliv > 0.0 {
        scale(deliv, 30.0, 70.0, 25.0)
    } else {
        10.0
    };
    acc += scale(vol_ratio, 0.9, 1.8, 20.0);
    acc += scale(updown, 1.0, 2.0, 20.0);
    acc += inverse(tight, 8.0, 25.0, 20.0);
    acc += match change {
        Some(c) if c >= 0.0 => 15.0,
        Some(_) => 2.0,
        None => match promoter {
            Some(p) if p >= 40.0 => 10.0,
            _ => 5.0,
        },
    };
    acc = acc.clamp(0.0, 100.0);

    let mut quality = 0.0;
    quality += roe.map(|x| scale(x, 10.0, 28.0, 25.0)).unwrap_or(10.0);
    quality += de.map(|x| inverse(x, 0.3, 1.5, 20.0)).unwrap_or(8.0);
    quality += cfo.map(|x| scale(x, 0.4, 1.2, 20.0)).unwrap_or(8.0);
    quality += promoter.map(|x| scale(x, 25.0, 60.0, 20.0)).unwrap_or(8.0);
    quality += prof.map(|x| scale(x, 5.0, 20.0, 15.0)).unwrap_or(6.0);
    quality = quality.clamp(0.0, 100.0);

    let _ = tech;
    let mut tags = Vec::new();
    for (name, score) in [
        ("Coffee Can", coffee),
        ("GARP", garp),
        ("Microcap", micro),
        ("Turnaround", turn),
        ("Deep Value", value),
        ("Momentum", mom),
        ("Accumulation", acc),
        ("Quality", quality),
    ] {
        if score >= 65.0 {
            tags.push(name.to_string());
        }
    }
    if near_high > -8.0 {
        tags.push("Near 52w high".to_string());
    }
    if breakout {
        tags.push("26w breakout".to_string());
    }
    if tight < 10.0 {
        tags.push("Tight base".to_string());
    }
    if deliv >= 55.0 {
        tags.push("Delivery accumulation".to_string());
    }
    if change.map(|c| c > 1.0).unwrap_or(false) {
        tags.push("Promoter rising".to_string());
    }
    tags.dedup();

    (
        json!({
            "coffee": nv(Some(coffee)),
            "garp": nv(Some(garp)),
            "microcap": nv(Some(micro)),
            "turnaround": nv(Some(turn)),
            "value": nv(Some(value)),
            "momentum": nv(Some(mom)),
            "accumulation": nv(Some(acc)),
            "quality": nv(Some(quality)),
        }),
        tags,
    )
}

fn thesis(
    row_tech: &Value,
    fund: &Value,
    flags: &[String],
    patterns: &[String],
) -> (Vec<String>, Vec<String>, String) {
    let mut supports = Vec::new();
    let mut risks = Vec::new();
    if let Some(v) = row_tech.get("ema200").and_then(|x| x.as_f64()) {
        if row_tech
            .get("ema50")
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0)
            > v
        {
            supports.push("Price structure above rising 200-DMA".to_string());
        }
    }
    if row_tech
        .get("breakout20")
        .and_then(|x| x.as_bool())
        .unwrap_or(false)
    {
        supports.push("20-day breakout with volume confirmation".to_string());
    }
    if let Some(v) = row_tech.get("rs6m").and_then(|x| x.as_f64()) {
        if v > 5.0 {
            supports.push(format!("Outperforming NIFTY by {:.1}% over 6 months", v));
        }
    }
    if let Some(v) = fval(fund, "roe") {
        if v >= 18.0 {
            supports.push(format!("ROE {v:.1}% - capital efficient business"));
        }
    }
    if let Some(v) = fval(fund, "revenueCagr3") {
        if v >= 15.0 {
            supports.push(format!("Revenue CAGR {v:.1}% over 3 years"));
        }
    }
    if patterns.iter().any(|p| p.contains("engulfing") || p.contains("Hammer")) {
        supports.push("Bullish reversal candle at the current level".to_string());
    }

    if let Some(v) = fval(fund, "debtEquity") {
        if v > 1.5 {
            risks.push(format!("Leverage elevated (D/E {v:.2})"));
        }
    }
    if let Some(v) = fval(fund, "cfoPat") {
        if v < 0.6 {
            risks.push(format!("Weak cash conversion (CFO/PAT {v:.2})"));
        }
    }
    if let Some(v) = fval(fund, "revenueCagr3") {
        if v < 0.0 {
            risks.push("Revenue shrinking over the 3-year window".to_string());
        }
    }
    if let Some(v) = row_tech.get("atrPct").and_then(|x| x.as_f64()) {
        if v > 5.0 {
            risks.push(format!("High volatility (ATR {v:.1}% of price)"));
        }
    }
    for f in flags {
        risks.push(f.clone());
    }
    risks.truncate(5);
    supports.truncate(5);

    let summary = if supports.is_empty() {
        "Mixed setup - wait for a cleaner trend or earnings trigger.".to_string()
    } else {
        format!(
            "{} | {}",
            supports[0],
            supports.get(1).map(|s| s.as_str()).unwrap_or("momentum improving")
        )
    };
    (supports, risks, summary)
}

// ---------------------------------------------------------------------------
// Backtest
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Series {
    symbol: String,
    cs: Vec<Candle>,
}

fn backtest(series: &[Series], bench: &[Candle]) -> Value {
    let months = 21usize;
    let warm = 252usize;
    let max_len = series.iter().map(|s| s.cs.len()).max().unwrap_or(0);
    if max_len < warm + months || bench.len() < warm + months {
        return json!({"available": false, "note": "Need at least ~13 months of history"});
    }
    let start = warm;
    let mut monthly: Vec<f64> = Vec::new();
    let mut bench_monthly: Vec<f64> = Vec::new();
    let mut picks: Vec<String> = Vec::new();
    let mut equity = 1.0f64;
    let mut peak = 1.0f64;
    let mut max_dd = 0.0f64;
    let mut wins = 0usize;

    let mut i = start;
    while i + months < max_len {
        let mut scored: Vec<(f64, &Series)> = Vec::new();
        for s in series {
            if s.cs.len() <= i + months {
                continue;
            }
            let b6 = if bench.len() > i && i >= 126 {
                (bench[i].close / bench[i - 126].close - 1.0) * 100.0
            } else {
                0.0
            };
            let (sc, _, _) = tech(&s.cs[..=i], b6, 0.0);
            scored.push((sc, s));
        }
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        let count = scored.len().min(5);
        if count == 0 {
            break;
        }
        let mut ret = 0.0;
        for (_, s) in scored.iter().take(count) {
            if s.cs[i].close > 0.0 {
                ret += s.cs[i + months].close / s.cs[i].close - 1.0;
            }
        }
        ret = ret / count as f64 - 0.005;
        let bench_ret = if bench[i].close > 0.0 {
            bench[i + months].close / bench[i].close - 1.0
        } else {
            0.0
        };
        if ret > 0.0 {
            wins += 1;
        }
        equity *= 1.0 + ret;
        peak = peak.max(equity);
        max_dd = max_dd.min(equity / peak - 1.0);
        picks.push(scored[0].1.symbol.clone());
        monthly.push(ret * 100.0);
        bench_monthly.push(bench_ret * 100.0);
        i += months;
    }
    if monthly.is_empty() {
        return json!({"available": false, "note": "Not enough monthly windows"});
    }
    let total = (equity - 1.0) * 100.0;
    let bench_total: f64 = bench_monthly.iter().map(|x| 1.0 + x / 100.0).product();
    let bench_total = (bench_total - 1.0) * 100.0;
    let avg = monthly.iter().sum::<f64>() / monthly.len() as f64;
    json!({
        "available": true,
        "months": monthly.len(),
        "totalReturnPct": nv(Some(total)),
        "avgMonthPct": nv(Some(avg)),
        "winRatePct": nv(Some(wins as f64 / monthly.len() as f64 * 100.0)),
        "maxDrawdownPct": nv(Some(max_dd * 100.0)),
        "benchmarkReturnPct": nv(Some(bench_total)),
        "vsBenchmarkPct": nv(Some(total - bench_total)),
        "monthly": monthly.iter().map(|x| nv(Some(*x))).collect::<Vec<_>>(),
        "note": "Equal-weight top-5 monthly momentum, 0.5% cost/side; fundamentals not point-in-time",
        "lastLeaders": picks.iter().rev().take(3).cloned().collect::<Vec<_>>(),
    })
}

fn pre_score(u: &Uni, tscore: f64, snap: &Value) -> f64 {
    let base = snap.get("base").unwrap_or(&Value::Null);
    (tscore * 0.55 + base_score(base) * 0.25 + u.disc * 0.20).clamp(0.0, 100.0)
}

fn select_fundamentals(
    tech: &[(Uni, f64, Value, Vec<String>)],
    max: usize,
) -> Vec<usize> {
    let mut buckets: [Vec<(usize, f64)>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for (i, (u, t, snap, _)) in tech.iter().enumerate() {
        let cap_cr = u.cap / 1e7;
        let idx = if cap_cr < 500.0 {
            0
        } else if cap_cr < 5000.0 {
            1
        } else if cap_cr < 50_000.0 {
            2
        } else {
            3
        };
        buckets[idx].push((i, pre_score(u, *t, snap)));
    }
    for b in &mut buckets {
        b.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    }
    let quotas = [60usize, 85usize, 45usize, 30usize];
    let mut out: Vec<(usize, f64)> = Vec::new();
    for (i, b) in buckets.iter_mut().enumerate() {
        let n = quotas[i].min(b.len());
        out.extend(b.drain(0..n));
    }
    if out.len() < max {
        let mut rest: Vec<(usize, f64)> = buckets.into_iter().flatten().collect();
        rest.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        out.extend(rest.into_iter().take(max - out.len()));
    }
    out.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    out.into_iter().take(max).map(|x| x.0).collect()
}

// ---------------------------------------------------------------------------
// Scan
// ---------------------------------------------------------------------------


fn bench_ret_to(bench: &[Candle], cutoff: i64) -> f64 {
    if bench.len() < 130 {
        return 0.0;
    }
    let idx = bench.partition_point(|c| c.time <= cutoff);
    if idx > 127 && bench[idx - 1].close > 0.0 {
        (bench[idx - 1].close / bench[idx - 127].close - 1.0) * 100.0
    } else {
        0.0
    }
}

/// Buy / avoid call derived from the composite score, data coverage and risk
/// flags. `upside` is a 12-month base-case estimate, never a promise.
fn buy_call(
    total: f64,
    conf: f64,
    snap: &Value,
    fund: &Value,
    flags: &[String],
    price: f64,
) -> Value {
    let g = |k: &str| snap.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
    let high52 = g("high52");
    let ema50 = g("ema50");
    let ema200 = g("ema200");
    let depth = snap
        .get("base")
        .and_then(|b| b.get("depth120"))
        .and_then(|x| x.as_f64())
        .unwrap_or(30.0);
    let gap_high = if price > 0.0 && high52 > price {
        (high52 / price - 1.0).min(0.8)
    } else {
        0.0
    };
    let base_proj = (depth / 100.0 * 0.5).min(0.5);
    let peg = fval(fund, "peg");
    let pe = fval(fund, "pe");
    let eg = fval(fund, "earnGrowth");
    let rg = fval(fund, "revGrowth");
    let val = match peg {
        Some(p) if p > 0.0 && p < 1.5 => (1.5 - p) / 1.5 * 0.30,
        _ => match pe {
            Some(p) if p > 0.0 && p < 15.0 => 0.15,
            _ => 0.0,
        },
    };
    let growth = if eg.map(|e| e > 25.0).unwrap_or(false) { 0.10 } else { 0.0 }
        + if rg.map(|r| r > 15.0).unwrap_or(false) { 0.05 } else { 0.0 };
    let illiquid = flags.iter().any(|f| f == "Illiquid");
    let no_fund = flags.iter().any(|f| f == "No fundamentals");
    let high_debt = flags.iter().any(|f| f == "High debt");
    let low_prom = flags
        .iter()
        .any(|f| f == "Low promoter holding" || f == "Promoter stake falling");
    let below200 = ema200 > 0.0 && price > 0.0 && price < ema200;
    let mut upside = 0.40 * gap_high + 0.25 * base_proj + 0.20 * val + 0.15 * growth;
    if below200 {
        upside *= 0.75;
    }
    if illiquid || no_fund {
        upside *= 0.7;
    }
    let up_pct = ((upside * 1000.0).round() / 10.0).clamp(0.0, 90.0);
    let low_pct = ((up_pct * 0.55).round() * 10.0 / 10.0).round();
    let high_pct = ((up_pct * 1.6).min(150.0) * 10.0).round() / 10.0;
    let target = price * (1.0 + up_pct / 100.0);
    let raw_stop = if ema50 > 0.0 && ema50 < price {
        ema50
    } else {
        price * 0.92
    };
    let stop = raw_stop.max(price * 0.85).min(price * 0.96).max(0.01);
    let risk_pct = if price > 0.0 {
        ((1.0 - stop / price) * 1000.0).round() / 10.0
    } else {
        8.0
    };
    let rr = if risk_pct > 0.0 {
        ((up_pct / risk_pct) * 10.0).round() / 10.0
    } else {
        0.0
    };
    let confidence = (conf + if total >= 65.0 { 4.0 } else if total < 50.0 { -4.0 } else { 0.0 }
        - if no_fund { 10.0 } else { 0.0 }
        - if illiquid { 6.0 } else { 0.0 }
        - if high_debt { 5.0 } else { 0.0 }
        - if low_prom { 4.0 } else { 0.0 }
        - if below200 { 3.0 } else { 0.0 })
        .clamp(15.0, 95.0);
    let grade = if total >= 73.0 {
        "A+"
    } else if total >= 66.0 {
        "A"
    } else if total >= 58.0 {
        "B"
    } else if total >= 50.0 {
        "C"
    } else {
        "D"
    };
    let mut status = if total >= 73.0 {
        "Strong Buy"
    } else if total >= 66.0 {
        "Buy"
    } else if total >= 58.0 {
        "Accumulate"
    } else if total >= 50.0 {
        "Watch"
    } else {
        "Avoid"
    };
    let soft = illiquid || no_fund || high_debt || low_prom;
    if soft || rr < 1.3 || confidence < 45.0 {
        status = match status {
            "Strong Buy" => "Buy",
            "Buy" => "Accumulate",
            other => other,
        };
    }
    if rr < 1.0 || total < 45.0 {
        status = "Watch";
    }
    if total < 38.0 {
        status = "Avoid";
    }
    let expected = ((up_pct * confidence / 100.0) * 10.0).round() / 10.0;
    json!({
        "status": status,
        "grade": grade,
        "confidence": nv(Some(confidence)),
        "upsidePct": nv(Some(up_pct)),
        "upsideLowPct": nv(Some(low_pct)),
        "upsideHighPct": nv(Some(high_pct)),
        "targetPrice": nv(Some(target)),
        "stopPrice": nv(Some(stop)),
        "riskPct": nv(Some(risk_pct)),
        "rr": nv(Some(rr)),
        "entryLow": nv(Some(price * 0.97)),
        "entryHigh": nv(Some(price * 1.02)),
        "expectedProfitPct": nv(Some(expected)),
        "horizonMonths": 12,
    })
}

fn count_outcome(entries: &[Value], name: &str) -> usize {
    entries
        .iter()
        .filter(|e| e.get("outcome").and_then(|x| x.as_str()) == Some(name))
        .count()
}

fn summarize_entries(entries: &[Value], label: &str) -> Value {
    let upgraded = entries
        .iter()
        .filter(|e| e.get("upgradedFrom").map(|x| !x.is_null()).unwrap_or(false))
        .count();
    let hit = count_outcome(entries, "hit");
    let near = count_outcome(entries, "near");
    let miss = count_outcome(entries, "miss");
    let stopped = count_outcome(entries, "stopped");
    let open = count_outcome(entries, "open");
    let evaluated = hit + near + miss + stopped;
    let mut rets: Vec<f64> = entries
        .iter()
        .filter(|e| e.get("outcome").and_then(|x| x.as_str()) != Some("open"))
        .filter_map(|e| e.get("returnPct").and_then(|x| x.as_f64()))
        .collect();
    rets.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let avg = if rets.is_empty() {
        0.0
    } else {
        rets.iter().sum::<f64>() / rets.len() as f64
    };
    let median = if rets.is_empty() {
        0.0
    } else {
        rets[rets.len() / 2]
    };
    // True win = the target was reached before the stop. "near" is a partial
    // touch, not a win, so it no longer inflates the headline rate.
    let wins = entries
        .iter()
        .filter(|e| e.get("win").and_then(|x| x.as_bool()).unwrap_or(false))
        .count();
    let win_rate = if evaluated > 0 {
        wins as f64 / evaluated as f64 * 100.0
    } else {
        0.0
    };
    let r_mult: Vec<f64> = entries
        .iter()
        .filter(|e| e.get("outcome").and_then(|x| x.as_str()) != Some("open"))
        .filter_map(|e| e.get("rMultiple").and_then(|x| x.as_f64()))
        .collect();
    let ev_r = if r_mult.is_empty() {
        0.0
    } else {
        r_mult.iter().sum::<f64>() / r_mult.len() as f64
    };
    let mut buckets: Vec<Value> = Vec::new();
    for (lo, hi) in [(0.0, 60.0), (60.0, 70.0), (70.0, 80.0), (80.0, 100.1)] {
        let b: Vec<&Value> = entries
            .iter()
            .filter(|e| {
                e.get("confidence")
                    .and_then(|x| x.as_f64())
                    .map(|c| c >= lo && c < hi)
                    .unwrap_or(false)
            })
            .filter(|e| e.get("outcome").and_then(|x| x.as_str()) != Some("open"))
            .collect();
        let n = b.len();
        let g = b
            .iter()
            .filter(|e| e.get("win").and_then(|x| x.as_bool()).unwrap_or(false))
            .count();
        let bev = if n > 0 {
            b.iter().filter_map(|e| e.get("rMultiple").and_then(|x| x.as_f64())).sum::<f64>() / n as f64
        } else {
            0.0
        };
        buckets.push(json!({
            "label": if hi > 100.0 { "80+".to_string() } else { format!("{:.0}-{:.0}", lo, hi - 0.1) },
            "n": n,
            "good": g,
            "rate": nv(Some(if n > 0 { g as f64 / n as f64 * 100.0 } else { 0.0 })),
            "evR": nv(Some(bev)),
        }));
    }
    let mut months: std::collections::BTreeMap<String, (usize, usize, f64)> =
        std::collections::BTreeMap::new();
    for e in entries {
        let date = e.get("date").and_then(|x| x.as_str()).unwrap_or("");
        if date.len() < 7 {
            continue;
        }
        let key = date[..7].to_string();
        let slot = months.entry(key).or_insert((0, 0, 0.0));
        slot.0 += 1;
        if e.get("win").and_then(|x| x.as_bool()).unwrap_or(false) {
            slot.1 += 1;
        }
        if e.get("outcome").and_then(|x| x.as_str()) != Some("open") {
            if let Some(r) = e.get("returnPct").and_then(|x| x.as_f64()) {
                slot.2 += r;
            }
        }
    }
    let month_rows: Vec<Value> = months
        .into_iter()
        .map(|(m, (n, g, sum))| {
            json!({
                "month": m,
                "n": n,
                "good": g,
                "rate": nv(Some(if n > 0 { g as f64 / n as f64 * 100.0 } else { 0.0 })),
                "avgRet": nv(Some(if n > 0 { sum / n as f64 } else { 0.0 })),
            })
        })
        .collect();
    let mut log: Vec<Value> = entries.to_vec();
    log.sort_by(|a, b| {
        b.get("date")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .cmp(a.get("date").and_then(|x| x.as_str()).unwrap_or(""))
    });
    log.truncate(40);
    json!({
        "label": label,
        "evaluated": evaluated,
        "hit": hit,
        "near": near,
        "miss": miss,
        "stopped": stopped,
        "wins": wins,
        "open": open,
        "upgraded": upgraded,
        "goodRate": nv(Some(win_rate)),
        "winRate": nv(Some(win_rate)),
        "hitRate": nv(Some(if evaluated > 0 { hit as f64 / evaluated as f64 * 100.0 } else { 0.0 })),
        "evR": nv(Some(ev_r)),
        "avgReturn": nv(Some(avg)),
        "medianReturn": nv(Some(median)),
        "buckets": buckets,
        "months": month_rows,
        "log": log,
    })
}


#[derive(Clone, Default)]
struct HSample {
    n: [usize; 3],
    hit: [usize; 3],
    days: Vec<f64>,
}

#[derive(Clone, Default)]
struct HorizonStats {
    overall: HSample,
    buckets: [HSample; 4],
    median_days: f64,
}

fn h_add(s: &mut HSample, e: &Value) {
    let avail = e.get("availBars").and_then(|x| x.as_u64()).unwrap_or(0) as usize;
    let bars = e.get("barsToTarget").and_then(|x| {
        x.as_u64()
            .map(|v| v as usize)
            .or_else(|| x.as_f64().map(|v| v.round() as usize))
    });
    for (i, h) in [63usize, 126, 252].iter().enumerate() {
        if avail >= *h {
            s.n[i] += 1;
            if bars.map(|b| b <= *h).unwrap_or(false) {
                s.hit[i] += 1;
            }
        }
    }
    if let Some(b) = bars {
        s.days.push(b as f64);
    }
}

fn horizon_stats(entries: &[Value]) -> HorizonStats {
    let mut out = HorizonStats::default();
    for e in entries {
        h_add(&mut out.overall, e);
        let c = e
            .get("confidence")
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0);
        let idx = if c < 60.0 {
            0
        } else if c < 70.0 {
            1
        } else if c < 80.0 {
            2
        } else {
            3
        };
        h_add(&mut out.buckets[idx], e);
    }
    let mut days = out.overall.days.clone();
    days.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    out.median_days = if days.is_empty() {
        45.0
    } else {
        days[days.len() / 2]
    };
    out
}

fn h_rate(s: &HSample, prior: &HSample, i: usize) -> f64 {
    let pn = prior.n[i] as f64;
    let pr = if prior.n[i] > 0 {
        prior.hit[i] as f64 / prior.n[i] as f64
    } else {
        0.5
    };
    ((s.hit[i] as f64 + pr * 20.0) / (s.n[i] as f64 + 20.0) * 100.0).clamp(2.0, 98.0)
}

/// Measured edge for one score band: how often a call of this composite score
/// actually reached its target BEFORE its stop, and the average R it returned.
#[derive(Clone, Default)]
struct EdgeStat {
    n: usize,
    wins: usize,
    r_sum: f64,
}

impl EdgeStat {
    fn win_rate(&self, prior_wr: f64, prior_n: f64) -> f64 {
        let pr = if prior_wr > 0.0 { prior_wr } else { 0.5 };
        ((self.wins as f64 + pr * prior_n) / (self.n as f64 + prior_n)).clamp(0.02, 0.98)
    }
    fn ev_r(&self) -> f64 {
        if self.n > 0 {
            self.r_sum / self.n as f64
        } else {
            0.0
        }
    }
}

/// Empirical calibration table built from the walk-forward replay. This is what
/// replaces the old "data coverage" number that was displayed as a probability
/// but measured nothing. Confidence is now the MEASURED win-rate of the same
/// score band, shrunk toward the book's base rate when a band has few samples.
struct Calib {
    /// 5 score bands: <50, 50-58, 58-66, 66-73, 73+.
    bands: [EdgeStat; 5],
    overall: EdgeStat,
}

fn score_band(total: f64) -> usize {
    if total >= 73.0 {
        4
    } else if total >= 66.0 {
        3
    } else if total >= 58.0 {
        2
    } else if total >= 50.0 {
        1
    } else {
        0
    }
}

fn build_calib(replay: &[Value]) -> Calib {
    let mut calib = Calib {
        bands: Default::default(),
        overall: EdgeStat::default(),
    };
    for e in replay {
        if e.get("outcome").and_then(|x| x.as_str()) == Some("open") {
            continue;
        }
        let Some(r) = e.get("rMultiple").and_then(|x| x.as_f64()) else {
            continue;
        };
        let win = e.get("win").and_then(|x| x.as_bool()).unwrap_or(r > 0.0);
        let total = e.get("total").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let band = score_band(total);
        for slot in [&mut calib.bands[band], &mut calib.overall] {
            slot.n += 1;
            slot.r_sum += r;
            if win {
                slot.wins += 1;
            }
        }
    }
    calib
}

/// Apply the empirical calibration to a call. Sets `call.confidence` to the
/// MEASURED win-rate (%) of the score band and exposes the measured EV in R.
/// Returns `true` when the plan has positive measured EV (or too little evidence
/// to judge), `false` when it is a proven loser and must be rejected.
fn apply_calibration(call: &mut Value, total: f64, calib: &Calib) -> bool {
    let band = score_band(total);
    let b = &calib.bands[band];
    let base_wr = calib.overall.win_rate(0.5, 30.0);
    // Shrink a thin band toward the overall base rate: 20 pseudo-samples.
    let prior_n = 20.0;
    let use_overall = b.n < 20;
    let src = if use_overall { &calib.overall } else { b };
    let wr = if use_overall {
        calib.overall.win_rate(0.5, 30.0)
    } else {
        b.win_rate(base_wr, prior_n)
    };
    let ev = src.ev_r();
    let n = src.n;
    // Honesty guard: with real evidence the displayed number can never be wildly
    // better than the measured rate; with none it stays neutral around 50.
    call["confidence"] = nv(Some((wr * 100.0).clamp(2.0, 98.0)));
    call["measuredWinRate"] = nv(Some((wr * 100.0).clamp(2.0, 98.0)));
    call["evR"] = nv(Some(ev));
    call["samples"] = json!(n);
    call["calibrated"] = json!(n >= 20);
    // Positive EV is required once there is enough evidence to judge.
    n < 20 || ev > 0.0
}

/// Attach an empirical timeframe and 3/6/12-month target probabilities from
/// the replay sample, nudged by the stock's volatility and relative strength.
fn apply_horizon(call: &mut Value, atr: f64, rs: f64, stats: &HorizonStats) {
    let conf = call
        .get("confidence")
        .and_then(|x| x.as_f64())
        .unwrap_or(60.0);
    let idx = if conf < 60.0 {
        0
    } else if conf < 70.0 {
        1
    } else if conf < 80.0 {
        2
    } else {
        3
    };
    let bucket = &stats.buckets[idx];
    let src = if bucket.n[1] >= 20 || bucket.n[2] >= 12 {
        bucket
    } else {
        &stats.overall
    };
    let atr = if atr > 0.0 { atr } else { 3.5 };
    let up = call
        .get("upsidePct")
        .and_then(|x| x.as_f64())
        .unwrap_or(15.0);
    let speed = (1.0 + (3.2 - atr) * 0.06 + rs.clamp(-40.0, 80.0) / 100.0 * 0.10)
        .clamp(0.75, 1.3);
    let p3 = (h_rate(src, &stats.overall, 0) * speed).clamp(2.0, 90.0);
    let p6 = (h_rate(src, &stats.overall, 1) * speed).clamp(p3, 94.0);
    let p12 = (h_rate(src, &stats.overall, 2) * speed).clamp(p6, 96.0);
    let mut days = if src.days.len() >= 8 {
        src.days.clone()
    } else {
        stats.overall.days.clone()
    };
    days.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let quant = |q: f64| -> f64 {
        if days.is_empty() {
            stats.median_days
        } else {
            let idx = ((days.len() as f64 - 1.0) * q).round() as usize;
            days[idx.min(days.len() - 1)]
        }
    };
    let factor = ((up / 15.0).clamp(0.5, 2.0)).sqrt()
        * (3.5 / atr.max(0.8)).clamp(0.5, 1.6).powf(0.4);
    let p50 = (quant(0.5) * factor).max(10.0);
    let p75 = (quant(0.75) * factor).max(p50 + 5.0);
    let (timeframe, expected_months) = if p75 <= 90.0 {
        let low = ((p50 / 7.0).round() as i64).max(1);
        let high = (((p75 / 7.0).round() as i64).max(low + 2)).min(24);
        (format!("{low}-{high} weeks"), p50 / 30.0)
    } else {
        let low = ((p50 / 30.0).round() as i64).max(1);
        let high = (((p75 / 30.0).round() as i64).max(low + 1)).min(18);
        (format!("{low}-{high} mo"), p50 / 30.0)
    };
    call["expectedMonths"] = nv(Some(expected_months));
    call["p50Days"] = nv(Some(p50));
    call["p75Days"] = nv(Some(p75));
    call["timeframe"] = json!(timeframe);
    call["prob3m"] = nv(Some(p3));
    call["prob6m"] = nv(Some(p6));
    call["prob12m"] = nv(Some(p12));
    call["baseSamples"] = json!(src.n[2]);
    call["medianDays"] = nv(Some(stats.median_days));
}


fn next_step(total: f64, rs6m: f64, tscore: f64) -> &'static str {
    if total >= 66.0 {
        return "Buy zone";
    }
    let gap = 66.0 - total;
    if gap <= 3.0 && rs6m > 0.0 && tscore >= 52.0 {
        "Near Buy - 1-2 scans"
    } else if gap <= 6.0 && rs6m > 0.0 {
        "Needs a breakout"
    } else if rs6m < -5.0 || tscore < 45.0 {
        "Momentum weak - upgrade hard"
    } else {
        "Far from Buy"
    }
}

fn load_status_history() -> (std::collections::HashMap<String, Value>, Vec<Value>) {
    let v = read_json(&data_path("equity_status.json")).unwrap_or_else(|| json!({}));
    let mut map = std::collections::HashMap::new();
    if let Some(obj) = v.get("symbols").and_then(|x| x.as_object()) {
        for (k, val) in obj {
            map.insert(k.clone(), val.clone());
        }
    }
    let upgrades = v
        .get("upgrades")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    (map, upgrades)
}

fn save_status_history(
    map: &std::collections::HashMap<String, Value>,
    upgrades: &[Value],
) {
    let mut obj = serde_json::Map::new();
    for (k, v) in map {
        obj.insert(k.clone(), v.clone());
    }
    let tail: Vec<Value> = upgrades.iter().rev().take(200).rev().cloned().collect();
    let _ = write_json(
        &data_path("equity_status.json"),
        &json!({"symbols": obj, "upgrades": tail, "updated": now_secs()}),
    );
}

fn load_preds() -> Vec<Value> {
    read_json(&data_path("equity_predictions.json"))
        .and_then(|v| v.get("entries").and_then(|x| x.as_array()).cloned())
        .unwrap_or_default()
}

fn save_preds(entries: &[Value]) {
    let tail: Vec<Value> = entries.iter().rev().take(3000).rev().cloned().collect();
    let _ = write_json(
        &data_path("equity_predictions.json"),
        &json!({"updated": now_secs(), "entries": tail}),
    );
}

/// Update the live prediction journal with the latest scan prices and append
/// this scan's high-conviction calls.
fn eval_live_predictions(prices: &HashMap<String, f64>, rows: &[Value]) -> Value {
    let now = now_secs();
    let today = ymd(today_ist());
    let mut current: HashMap<String, (String, f64)> = HashMap::new();
    for r in rows {
        let Some(sym) = r.get("symbol").and_then(|x| x.as_str()) else {
            continue;
        };
        let call = r.get("call").unwrap_or(&Value::Null);
        let st = call
            .get("status")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let conf = call
            .get("confidence")
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0);
        if !st.is_empty() {
            current.insert(sym.to_string(), (st, conf));
        }
    }
    let mut entries = load_preds();
    for e in entries.iter_mut() {
        if e.get("state").and_then(|x| x.as_str()) != Some("open") {
            continue;
        }
        let sym = e.get("symbol").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let Some(&last) = prices.get(&sym) else {
            continue;
        };
        if last <= 0.0 {
            continue;
        }
        let price = e.get("price").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let target = e.get("target").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let stop = e.get("stop").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let near = e.get("nearTarget").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let hwm = e
            .get("hwm")
            .and_then(|x| x.as_f64())
            .unwrap_or(price)
            .max(last);
        let lwm = e
            .get("lwm")
            .and_then(|x| x.as_f64())
            .unwrap_or(price)
            .min(last);
        let at = e.get("at").and_then(|x| x.as_i64()).unwrap_or(now);
        if let Some((cur_status, cur_conf)) = current.get(&sym) {
            let old_status = e
                .get("status")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            if matches!(cur_status.as_str(), "Strong Buy" | "Buy")
                && !matches!(old_status.as_str(), "Strong Buy" | "Buy")
            {
                e["origStatus"] = json!(old_status);
                e["upgradedFrom"] = json!(old_status);
                e["upgradedAt"] = json!(today);
                e["status"] = json!(cur_status);
                e["confidence"] = nv(Some(*cur_conf));
            }
        }
        e["hwm"] = nv(Some(hwm));
        e["lwm"] = nv(Some(lwm));
        e["last"] = nv(Some(last));
        e["lastAt"] = json!(now);
        let ret = if price > 0.0 { (last / price - 1.0) * 100.0 } else { 0.0 };
        e["returnPct"] = nv(Some(ret));
        let risk = if price > 0.0 && stop > 0.0 && stop < price { price - stop } else { 0.0 };
        // Stop-first honesty: if the low of the holding period ever touched the
        // stop, the trade was stopped - a later rebound must not be scored as a
        // win. This mirrors the walk-forward replay's stop-first rule.
        if stop > 0.0 && lwm <= stop {
            e["state"] = json!("closed");
            e["outcome"] = json!("stopped");
            e["win"] = json!(false);
            e["rMultiple"] = nv(Some(-1.0));
            e["returnPct"] = nv(Some((stop / price - 1.0) * 100.0));
            e["closedAt"] = json!(now);
        } else if hwm >= target && target > 0.0 {
            e["state"] = json!("closed");
            e["outcome"] = json!("hit");
            e["win"] = json!(true);
            e["rMultiple"] = nv(Some(if risk > 0.0 { (target - price) / risk } else { 1.0 }));
            e["returnPct"] = nv(Some((target / price - 1.0) * 100.0));
            e["closedAt"] = json!(now);
        } else if hwm >= near && near > 0.0 {
            e["outcome"] = json!("near");
        } else if now - at >= 180 * 86_400 {
            e["state"] = json!("closed");
            e["outcome"] = json!(if ret >= 0.0 { "expired" } else { "miss" });
            e["win"] = json!(ret > 0.0);
            e["rMultiple"] = nv(Some(if risk > 0.0 { (last - price) / risk } else { 0.0 }));
            e["closedAt"] = json!(now);
        }
    }
    let open_syms: std::collections::HashSet<String> = entries
        .iter()
        .filter(|e| e.get("state").and_then(|x| x.as_str()) == Some("open"))
        .filter_map(|e| e.get("symbol").and_then(|x| x.as_str()).map(|s| s.to_string()))
        .collect();
    let mut added = 0usize;
    for r in rows {
        if added >= 25 {
            break;
        }
        let call = r.get("call").unwrap_or(&Value::Null);
        let status = call.get("status").and_then(|x| x.as_str()).unwrap_or("");
        let conf = call.get("confidence").and_then(|x| x.as_f64()).unwrap_or(0.0);
        // Confidence is now the MEASURED replay win-rate, so the journal keeps
        // only calls that clear a realistic edge (>=52%) - not the old 55 on a
        // different scale.
        if !matches!(status, "Strong Buy" | "Buy" | "Accumulate") || conf < 52.0 {
            continue;
        }
        let sym = r.get("symbol").and_then(|x| x.as_str()).unwrap_or("").to_string();
        if sym.is_empty() || open_syms.contains(&sym) {
            continue;
        }
        let price = r.get("price").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let up = call.get("upsidePct").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let target = call.get("targetPrice").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let stop = call.get("stopPrice").and_then(|x| x.as_f64()).unwrap_or(0.0);
        entries.push(json!({
            "date": today,
            "at": now,
            "symbol": sym,
            "name": r.get("name").cloned().unwrap_or(Value::Null),
            "status": status,
            "confidence": nv(Some(conf)),
            "price": nv(Some(price)),
            "upsidePct": nv(Some(up)),
            "target": nv(Some(target)),
            "stop": nv(Some(stop)),
            "nearTarget": nv(Some(price * (1.0 + up * 0.6 / 100.0))),
            "hwm": nv(Some(price)),
            "lwm": nv(Some(price)),
            "last": nv(Some(price)),
            "lastAt": json!(now),
            "state": "open",
            "outcome": "open",
            "returnPct": 0.0,
        }));
        added += 1;
    }
    save_preds(&entries);
    summarize_entries(&entries, "live")
}

/// Merge dual listings so the same COMPANY can never appear twice in the
/// research book (and therefore never be fetched / traded twice by the equity
/// engines). The screener lists a company on both NSE and BSE under the same
/// ticker in most cases, but a few carry different tickers per exchange while
/// sharing one ISIN. Symbol-level merge already collapses the common case; this
/// second pass collapses the ISIN-level duplicates, always keeping the NSE row
/// (it carries delivery data and promoter holding).
fn dedupe_by_company(merged: HashMap<String, Uni>) -> HashMap<String, Uni> {
    let mut by_isin: HashMap<String, String> = HashMap::new();
    let mut out: HashMap<String, Uni> = HashMap::new();
    let mut rows: Vec<Uni> = merged.into_values().collect();
    // NSE first so it wins every tie, then the richer (higher turnover) listing.
    rows.sort_by(|a, b| {
        let an = (a.exch != "NSE") as u8;
        let bn = (b.exch != "NSE") as u8;
        an.cmp(&bn)
            .then_with(|| b.avg3.partial_cmp(&a.avg3).unwrap_or(std::cmp::Ordering::Equal))
    });
    for u in rows {
        let isin = u.isin.trim().to_uppercase();
        if !isin.is_empty() {
            if let Some(kept) = by_isin.get(&isin) {
                // Same company already kept under another ticker: merge the ISIN
                // duplicate away instead of adding a second row.
                let _ = kept;
                continue;
            }
            by_isin.insert(isin, u.symbol.clone());
        }
        out.insert(u.symbol.clone(), u);
    }
    out
}

async fn scan(dhan: &DhanState) -> Result<(), String> {
    let client = http().ok_or("http client unavailable")?;
    {
        let mut c = cache().lock().map_err(|_| "cache lock")?;
        c.busy = true;
        c.started_at = now_secs();
        c.errors.clear();
        c.note.clear();
        set_stage(&mut c, "loading listed universe (NSE + BSE)", 0, 0);
    }

    let filter = {
        let c = cache().lock().map_err(|_| "cache lock")?;
        c.filters.clone()
    };
    let full_scan = filter.scan_all;
    // Full scan enumerates deeper (NSE ~2,000 EQ names, BSE ~4,000 listings) and
    // tolerates rows the screener reports without a market cap / volume figure.
    let (nse_pages, bse_pages) = if full_scan { (40usize, 48usize) } else { (20usize, 26usize) };

    let bhav_fut = fetch_bhavcopy(&client);
    let mut auth = yahoo_auth().await;
    let mut universe: Vec<Uni> = Vec::new();
    if let Some(a) = auth.as_ref() {
        let mut nse = fetch_screener_universe(&client, a, "NSI", ".NS", nse_pages, full_scan).await;
        let mut bse = fetch_screener_universe(&client, a, "BSE", ".BO", bse_pages, full_scan).await;
        if nse.is_empty() && bse.is_empty() {
            auth = yahoo_auth().await;
            if let Some(a2) = auth.as_ref() {
                nse = fetch_screener_universe(&client, a2, "NSI", ".NS", nse_pages, full_scan).await;
                bse = fetch_screener_universe(&client, a2, "BSE", ".BO", bse_pages, full_scan).await;
            }
        }
        universe.extend(nse);
        universe.extend(bse);
    }
    let mut fallback = false;
    if universe.is_empty() {
        fallback = true;
        universe = fetch_equity_list(&client).await;
    }

    let (bhav, as_of) = match bhav_fut.await {
        Some((b, d)) => (b, d),
        None => {
            let mut c = cache().lock().map_err(|_| "cache lock")?;
            push_error(&mut c, "NSE bhavcopy unavailable - delivery data missing");
            (HashMap::new(), ymd(today_ist()))
        }
    };
    if fallback {
        let mut c = cache().lock().map_err(|_| "cache lock")?;
        push_error(&mut c, "Yahoo screener unavailable - NSE-only fallback");
    }

    // Merge dual listings: NSE wins because it carries delivery percentage and
    // promoter holding. BSE-only companies are kept as-is.
    let mut merged: HashMap<String, Uni> = HashMap::new();
    for mut u in universe {
        if let Some(b) = bhav.get(&u.symbol) {
            if b.close > 0.0 {
                u.price = b.close;
            }
        }
        match merged.get(&u.symbol) {
            None => {
                merged.insert(u.symbol.clone(), u);
            }
            Some(old) => {
                if u.exch == "NSE" && old.exch != "NSE" {
                    merged.insert(u.symbol.clone(), u);
                } else if old.exch == u.exch && u.avg3 > old.avg3 {
                    merged.insert(u.symbol.clone(), u);
                }
            }
        }
    }

    let merged = dedupe_by_company(merged);
    let listed = merged.len();
    let mut candidates: Vec<Uni> = Vec::new();
    for u in merged.into_values() {
        let turnover_lacs = bhav
            .get(&u.symbol)
            .map(|b| b.turnover_lacs)
            .filter(|x| *x > 0.0)
            .unwrap_or(u.price * u.avg3 / 1e5);
        // "Scan all listed stocks" skips the liquidity screen (price, turnover,
        // market-cap and delivery gates) so every discovered equity is scanned.
        // A price is still required: a history fetch for a zero/invalid quote
        // can never score, so those rows would only burn rate limit.
        if !filter.scan_all {
            if u.price < 10.0 || turnover_lacs < filter.min_turnover_lacs.max(1.0) {
                continue;
            }
            if u.cap > 0.0 && u.cap < 30.0 * 1e7 {
                continue;
            }
            if filter.max_price > 0.0 && u.price > filter.max_price {
                continue;
            }
            let deliv = bhav.get(&u.symbol).map(|b| b.deliv_pct).unwrap_or(0.0);
            if deliv < filter.min_delivery {
                continue;
            }
        } else if u.price <= 0.0 {
            continue;
        }
        candidates.push(u);
    }
    let liquid_universe = candidates.len();
    // In full-scan mode the cap-balanced 900-stock sample and the 300-row output
    // truncation are both lifted: otherwise "scan all" would still show only a
    // slice of what it just measured.
    let picked = if filter.scan_all {
        candidates
    } else {
        pick_universe(candidates)
    };
    let nse_count = picked.iter().filter(|u| u.exch == "NSE").count();
    let bse_count = picked.len() - nse_count;
    let total = picked.len();
    {
        let mut c = cache().lock().map_err(|_| "cache lock")?;
        c.universe = liquid_universe;
        c.listed = listed;
        c.nse = nse_count;
        c.bse = bse_count;
        set_stage(&mut c, "fetching price history", 0, total);
    }
    if total == 0 {
        let mut c = cache().lock().map_err(|_| "cache lock")?;
        c.busy = false;
        c.stage = "failed".to_string();
        push_error(&mut c, "no candidate passed the liquidity filter");
        return Err("empty universe".to_string());
    }

    let bench = yahoo_chart(&client, "^NSEI", "2y").await;
    let bench6 = if bench.len() > 126 {
        (bench[bench.len() - 1].close / bench[bench.len() - 127].close - 1.0) * 100.0
    } else {
        0.0
    };

    let mut series: HashMap<String, Vec<Candle>> = HashMap::new();
    let mut technical: Vec<(Uni, f64, Value, Vec<String>)> = Vec::new();
    let mut done = 0usize;
    for chunk in picked.chunks(CHART_CHUNK) {
        let futs: Vec<_> = chunk
            .iter()
            .map(|u| {
                let sym = u.ysym.clone();
                let cl = client.clone();
                async move { yahoo_chart(&cl, &sym, "2y").await }
            })
            .collect();
        let results = futures_util::future::join_all(futs).await;
        for (u, mut cs) in chunk.iter().zip(results) {
            done += 1;
            if cs.len() < 120 {
                tokio::time::sleep(Duration::from_millis(250)).await;
                cs = yahoo_chart(&client, &u.ysym, "2y").await;
            }
            if cs.len() < 120 {
                continue;
            }
            let deliv = bhav.get(&u.symbol).map(|b| b.deliv_pct).unwrap_or(0.0);
            let (score, snap, patterns) = tech(&cs, bench6, deliv);
            series.insert(u.symbol.clone(), cs);
            technical.push((u.clone(), score, snap, patterns));
        }
        {
            let mut c = cache().lock().map_err(|_| "cache lock")?;
            set_stage(&mut c, "fetching price history", done, total);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    technical.sort_by(|a, b| {
        let pa = pre_score(&a.0, a.1, &a.2);
        let pb = pre_score(&b.0, b.1, &b.2);
        pb.partial_cmp(&pa).unwrap_or(std::cmp::Ordering::Equal)
    });
    {
        let mut c = cache().lock().map_err(|_| "cache lock")?;
        if technical.is_empty() {
            push_error(&mut c, "Yahoo chart unavailable for every candidate");
        }
        set_stage(&mut c, "fetching fundamentals", 0, technical.len().min(MAX_FUNDAMENTALS));
    }

    let fund_targets = select_fundamentals(&technical, MAX_FUNDAMENTALS);
    let fund_total = fund_targets.len();
    let mut done = 0usize;
    for idx in fund_targets {
        let u = technical[idx].0.clone();
        let cached = fundamental_file()
            .lock()
            .ok()
            .and_then(|m| m.get(&u.symbol).cloned());
        let fresh = cached
            .as_ref()
            .and_then(|v| v.get("at").and_then(|x| x.as_i64()))
            .map(|at| now_secs() - at < 7 * 86_400)
            .unwrap_or(false);
        if !fresh {
            if auth.is_none() {
                auth = yahoo_auth().await;
            }
            if let Some(a) = auth.as_ref() {
                if let Some(parsed) = yahoo_fundamentals(&client, a, &u.ysym).await {
                    let entry = json!({"at": now_secs(), "data": parsed});
                    if let Ok(mut f) = fundamental_file().lock() {
                        f.insert(u.symbol.clone(), entry);
                    }
                } else if u.exch == "BSE" {
                    // Retry once on the NSE listing of dual-listed companies.
                    let ys = format!("{}.NS", u.symbol);
                    if let Some(parsed) = yahoo_fundamentals(&client, a, &ys).await {
                        let entry = json!({"at": now_secs(), "data": parsed});
                        if let Ok(mut f) = fundamental_file().lock() {
                            f.insert(u.symbol.clone(), entry);
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(220)).await;
        }
        done += 1;
        {
            let mut c = cache().lock().map_err(|_| "cache lock")?;
            set_stage(&mut c, "fetching fundamentals", done, fund_total);
        }
    }
    if let Ok(f) = fundamental_file().lock() {
        let value = serde_json::to_value(&*f).unwrap_or(Value::Null);
        let _ = write_json(&data_path("equity_fundamentals.json"), &value);
    }

    {
        let mut c = cache().lock().map_err(|_| "cache lock")?;
        set_stage(&mut c, "fetching shareholding", 0, 1);
    }
    let holdings = shareholding_master(&client).await;
    let mut holding_snapshot: HashMap<String, Value> = HashMap::new();
    for (sym, (promoter, public, date)) in &holdings {
        let change = promoter_change(sym, date, *promoter);
        holding_snapshot.insert(
            sym.clone(),
            json!({
                "available": true,
                "promoter": nv(Some(*promoter)),
                "promoterPrev": match change { Some(c) => nv(Some(*promoter - c)), None => Value::Null },
                "promoterChange": nv(change),
                "public": nv(Some(*public)),
                "date": date,
            }),
        );
    }
    save_holdings();

    let mut replay: Vec<Value> = Vec::new();
    let mut rows: Vec<(f64, Value, String)> = Vec::new();
    for (u, tscore, tsnap, patterns) in &technical {
        let cached = fundamental_file()
            .lock()
            .ok()
            .and_then(|x| x.get(&u.symbol).cloned())
            .and_then(|v| v.get("data").cloned())
            .unwrap_or_else(|| json!({}));
        let has_fund = cached.as_object().map(|o| !o.is_empty()).unwrap_or(false);
        let (fscore, fund_keys) = if has_fund {
            fundamental_score(&cached)
        } else {
            (50.0, 0)
        };
        let hold = holding_snapshot.get(&u.symbol).cloned();
        let promoter = hold
            .as_ref()
            .and_then(|x| x.get("promoter"))
            .and_then(|x| x.as_f64());
        let change = hold
            .as_ref()
            .and_then(|x| x.get("promoterChange"))
            .and_then(|x| x.as_f64());
        let (qscore, has_hold) = quality_score(&cached, promoter, change);
        let bscore = base_score(tsnap.get("base").unwrap_or(&Value::Null));
        let vscore = valuation_score(&cached, u);
        let sscore = size_score(u);
        let b = bhav.get(&u.symbol);
        let deliv = b.map(|x| x.deliv_pct).unwrap_or(0.0);
        let turnover_lacs = b
            .map(|x| x.turnover_lacs)
            .filter(|x| *x > 0.0)
            .unwrap_or(u.price * u.avg3 / 1e5);
        let lscore = liquidity_score(turnover_lacs, deliv);

        let mut flags: Vec<String> = Vec::new();
        if !has_fund {
            flags.push("No fundamentals".to_string());
        }
        if u.exch == "BSE" {
            flags.push("BSE - no delivery data".to_string());
        }
        if let Some(v) = fval(&cached, "debtEquity") {
            if v > 1.5 {
                flags.push("High debt".to_string());
            }
        }
        if let Some(v) = fval(&cached, "cfoPat") {
            if v < 0.6 {
                flags.push("Weak cash conversion".to_string());
            }
        }
        if let Some(v) = fval(&cached, "revenueCagr3") {
            if v < 0.0 {
                flags.push("Revenue declining".to_string());
            }
        }
        if let Some(p) = promoter {
            if p < 25.0 {
                flags.push("Low promoter holding".to_string());
            }
        }
        if change.map(|c| c < -1.0).unwrap_or(false) {
            flags.push("Promoter stake falling".to_string());
        }
        if turnover_lacs < 20.0 {
            flags.push("Illiquid".to_string());
        }

        let mut total = multibagger_total(
            fscore, *tscore, qscore, bscore, vscore, sscore, lscore,
        );
        if turnover_lacs < 20.0 {
            total -= 6.0;
        }
        if !has_fund {
            total -= 8.0;
        }
        if fval(&cached, "debtEquity").map(|x| x > 2.0).unwrap_or(false) {
            total -= 6.0;
        }
        if promoter.map(|p| p < 25.0).unwrap_or(false) {
            total -= 5.0;
        }
        if tsnap
            .get("ema200")
            .and_then(|x| x.as_f64())
            .map(|e| e > 0.0 && u.price < e)
            .unwrap_or(false)
        {
            total -= 4.0;
        }
        let total = total.clamp(0.0, 100.0);

        let sector = cached
            .get("sector")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let industry = cached
            .get("industry")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();

        let tech_cov = if series.get(&u.symbol).map(|s| s.len()).unwrap_or(0) >= 250 {
            1.0
        } else {
            0.5
        };
        let fund_cov = fund_keys as f64 / 6.0;
        let hold_cov = if has_hold { 1.0 } else { 0.0 };
        let base_cov = if tsnap
            .get("base")
            .and_then(|x| x.get("available"))
            .and_then(|x| x.as_bool())
            == Some(true)
        {
            1.0
        } else {
            0.0
        };
        let conf = (tech_cov * 0.25
            + fund_cov * 0.30
            + hold_cov * 0.15
            + base_cov * 0.15
            + 0.15)
            .clamp(0.0, 1.0)
            * 100.0;

        let call = buy_call(total, conf, tsnap, &cached, &flags, u.price);
        for back in [63usize, 84, 105, 126, 147, 168, 189, 252] {
            let Some(cs) = series.get(&u.symbol) else {
                continue;
            };
            let n = cs.len();
            if n < back + 63 {
                continue;
            }
            let k = n - back;
            if k < 140 {
                continue;
            }
            let entry_price = cs[k - 1].close;
            if entry_price <= 0.0 {
                continue;
            }
            let bench6p = bench_ret_to(&bench, cs[k - 1].time);
            let (tsp, snapp, _) = tech(&cs[..k], bench6p, 0.0);
            let bsp = base_score(snapp.get("base").unwrap_or(&Value::Null));
            let lsp = liquidity_score(turnover_lacs, 0.0);
            let mut totp = multibagger_total(fscore, tsp, qscore, bsp, vscore, sscore, lsp);
            if turnover_lacs < 20.0 {
                totp -= 6.0;
            }
            if !has_fund {
                totp -= 8.0;
            }
            if fval(&cached, "debtEquity").map(|x| x > 2.0).unwrap_or(false) {
                totp -= 6.0;
            }
            if promoter.map(|p| p < 25.0).unwrap_or(false) {
                totp -= 5.0;
            }
            if snapp
                .get("ema200")
                .and_then(|x| x.as_f64())
                .map(|e| e > 0.0 && entry_price < e)
                .unwrap_or(false)
            {
                totp -= 4.0;
            }
            let totp = totp.clamp(0.0, 100.0);
            let callp = buy_call(totp, conf, &snapp, &cached, &flags, entry_price);
            let st = callp.get("status").and_then(|x| x.as_str()).unwrap_or("");
            if !matches!(st, "Strong Buy" | "Buy" | "Accumulate") {
                continue;
            }
            let target = callp.get("targetPrice").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let stop = callp.get("stopPrice").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let up = callp.get("upsidePct").and_then(|x| x.as_f64()).unwrap_or(0.0);
            // A call with no usable target/stop cannot be replayed honestly, so it
            // is not measured (and therefore never inflates the hit-rate).
            if target <= entry_price || stop <= 0.0 || stop >= entry_price {
                continue;
            }
            let risk = entry_price - stop;
            let reward = target - entry_price;
            let horizon = 63usize; // ~3 months of trading bars
            let max_end = (k + 252).min(n);
            let mut bars_to_target: Option<usize> = None;
            let mut max_high = 0.0f64;
            let mut r_mult = 0.0f64;
            let mut exit_ret = 0.0f64;
            // "stopped" = hard stop hit, "hit" = target reached, "miss" = neither
            // within the 3-month horizon (closed at its expiry price).
            let mut outcome = "miss";
            let mut win = false;
            for (j, c) in cs[k..max_end].iter().enumerate() {
                if c.high > max_high {
                    max_high = c.high;
                }
                // STOP-FIRST: a bar that trades through the stop is a loss even if
                // the same bar also pokes the target. Measuring target-first is
                // what made the old replay look far better than reality.
                if c.low <= stop {
                    outcome = "stopped";
                    exit_ret = (stop / entry_price - 1.0) * 100.0;
                    r_mult = -1.0;
                    break;
                }
                if c.high >= target {
                    outcome = "hit";
                    bars_to_target = Some(j + 1);
                    exit_ret = (target / entry_price - 1.0) * 100.0;
                    r_mult = reward / risk;
                    win = true;
                    break;
                }
                if j + 1 >= horizon {
                    exit_ret = (c.close / entry_price - 1.0) * 100.0;
                    r_mult = (c.close - entry_price) / risk;
                    win = exit_ret > 0.0;
                    outcome = "miss";
                    break;
                }
            }
            let avail = max_end - k;
            replay.push(json!({
                "date": ymd((cs[k - 1].time + 19_800) / 86_400),
                "symbol": u.symbol,
                "cohortDays": back,
                "status": st,
                "total": nv(Some(totp)),
                "confidence": callp.get("confidence").cloned().unwrap_or(Value::Null),
                "entry": nv(Some(entry_price)),
                "target": nv(Some(target)),
                "stop": nv(Some(stop)),
                "upsidePct": nv(Some(up)),
                "returnPct": nv(Some(exit_ret)),
                "maxGainPct": nv(Some((max_high / entry_price - 1.0) * 100.0)),
                "outcome": outcome,
                "win": win,
                "rMultiple": nv(Some(r_mult)),
                "availBars": avail,
                "barsToTarget": match bars_to_target { Some(b) => json!(b), None => Value::Null },
            }));
        }
        let (strategies, tags) =
            strategy_scores(&cached, tsnap, u, *tscore, deliv, promoter, change);
        let (supports, risks, summary) = thesis(tsnap, &cached, &flags, patterns);
        let change_pct = match b {
            Some(x) if x.prev_close > 0.0 => (x.close / x.prev_close - 1.0) * 100.0,
            _ => u.chg,
        };

        let row = json!({
            "symbol": u.symbol,
            "name": u.name,
            "exchange": u.exch,
            "sector": sector,
            "industry": industry,
            "isin": u.isin,
            "price": nv(Some(u.price)),
            "changePct": nv(Some(change_pct)),
            "turnoverLacs": nv(Some(turnover_lacs)),
            "deliveryPct": nv(Some(deliv)),
            "marketCap": nv(Some(u.cap)),
            "fundamentals": if has_fund { cached.clone() } else { json!({"available": false}) },
            "technicals": tsnap,
            "options": {"available": false, "fno": false},
            "shareholding": hold.clone().unwrap_or_else(|| json!({"available": false})),
            "scores": {
                "fundamental": nv(Some(fscore)),
                "technical": nv(Some(*tscore)),
                "quality": nv(Some(qscore)),
                "base": nv(Some(bscore)),
                "valuation": nv(Some(vscore)),
                "size": nv(Some(sscore)),
                "liquidity": nv(Some(lscore)),
                "total": nv(Some(total)),
                "confidence": nv(Some(conf)),
            },
            "call": call,
            "strategies": strategies,
            "tags": tags,
            "flags": flags,
            "thesis": {"supports": supports, "risks": risks, "summary": summary},
        });
        rows.push((total, row, u.symbol.clone()));
    }

    let hstats = horizon_stats(&replay);
    let calib = build_calib(&replay);
    let now = now_secs();
    let (mut hist, mut upgrades) = load_status_history();
    if hist.is_empty() {
        if let Ok(c) = cache().lock() {
            for row in &c.rows {
                let sym = row.get("symbol").and_then(|x| x.as_str()).unwrap_or("");
                let call = row.get("call").unwrap_or(&Value::Null);
                let status = call.get("status").and_then(|x| x.as_str()).unwrap_or("");
                if sym.is_empty() || status.is_empty() {
                    continue;
                }
                hist.insert(
                    sym.to_string(),
                    json!({
                        "status": status,
                        "score": row.get("scores").and_then(|s| s.get("total")).cloned().unwrap_or(Value::Null),
                        "since": c.at.max(now - 21_600),
                        "prev": Value::Null,
                        "prevAt": Value::Null,
                        "upgradedFrom": Value::Null,
                        "upgradedAt": Value::Null,
                    }),
                );
            }
        }
    }
    for (_, row, _) in rows.iter_mut() {
        let atr = row
            .get("technicals")
            .and_then(|t| t.get("atrPct"))
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0);
        let rs = row
            .get("technicals")
            .and_then(|t| t.get("rs6m"))
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0);
        let tscore = row
            .get("scores")
            .and_then(|s| s.get("technical"))
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0);
        let total = row
            .get("scores")
            .and_then(|s| s.get("total"))
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0);
        let sym = row
            .get("symbol")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let status = row
            .get("call")
            .and_then(|c| c.get("status"))
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let mut upgraded_from = Value::Null;
        let mut upgraded_at = Value::Null;
        if !sym.is_empty() && !status.is_empty() {
            let prev = hist.get(&sym).cloned();
            let prev_status = prev
                .as_ref()
                .and_then(|x| x.get("status"))
                .and_then(|x| x.as_str())
                .unwrap_or("");
            let since = prev
                .as_ref()
                .and_then(|x| x.get("since"))
                .and_then(|x| x.as_i64())
                .unwrap_or(now);
            let is_buy = matches!(status.as_str(), "Strong Buy" | "Buy");
            let was_buy = matches!(prev_status, "Strong Buy" | "Buy");
            if prev_status != status {
                if is_buy && !was_buy && matches!(prev_status, "Accumulate" | "Watch") {
                    upgraded_from = json!(prev_status);
                    upgraded_at = json!(ymd((now + 19_800) / 86_400));
                    upgrades.push(json!({
                        "date": ymd((now + 19_800) / 86_400),
                        "at": now,
                        "symbol": sym,
                        "from": prev_status,
                        "to": status,
                        "price": row.get("price").cloned().unwrap_or(Value::Null),
                        "score": nv(Some(total)),
                    }));
                }
                hist.insert(
                    sym.clone(),
                    json!({
                        "status": status,
                        "score": nv(Some(total)),
                        "since": now,
                        "prev": prev_status,
                        "prevAt": since,
                        "upgradedFrom": if upgraded_from.is_null() { Value::Null } else { upgraded_from.clone() },
                        "upgradedAt": if upgraded_at.is_null() { Value::Null } else { upgraded_at.clone() },
                    }),
                );
            } else if let Some(rec) = hist.get_mut(&sym) {
                rec["score"] = nv(Some(total));
            }
        }
        if let Some(call) = row.get_mut("call") {
            // Replace the old data-coverage number with the MEASURED win-rate of
            // this score band, and reject plans with proven-negative EV so a
            // structurally losing setup is never presented as a "buy".
            let edge_ok = apply_calibration(call, total, &calib);
            if !edge_ok {
                call["status"] = json!("Avoid");
                call["rejected"] = json!(true);
                call["rejectReason"] = json!(format!(
                    "Measured EV {:.2}R over {} replayed trades is not positive",
                    call.get("evR").and_then(|x| x.as_f64()).unwrap_or(0.0),
                    call.get("samples").and_then(|x| x.as_u64()).unwrap_or(0)
                ));
            }
            apply_horizon(call, atr, rs, &hstats);
            call["nextStep"] = json!(next_step(total, rs, tscore));
            if !upgraded_from.is_null() {
                call["upgradedFrom"] = upgraded_from;
                call["upgradedAt"] = upgraded_at;
            }
            if let Some(rec) = hist.get(&sym) {
                if let Some(since) = rec.get("since").and_then(|x| x.as_i64()) {
                    call["statusSince"] = json!(ymd((since + 19_800) / 86_400));
                }
                if let Some(p) = rec.get("prev").and_then(|x| x.as_str()) {
                    call["prevStatus"] = json!(p);
                }
                let recent = rec
                    .get("upgradedAt")
                    .and_then(|x| x.as_str())
                    .and_then(parse_ymd)
                    .map(|d| today_ist() - d <= 45)
                    .unwrap_or(false);
                if recent {
                    if let Some(u) = rec.get("upgradedFrom").and_then(|x| x.as_str()) {
                        call["upgradedFrom"] = json!(u);
                    }
                    if let Some(u) = rec.get("upgradedAt").and_then(|x| x.as_str()) {
                        call["upgradedAt"] = json!(u);
                    }
                }
            }
        }
    }
    save_status_history(&hist, &upgrades);

    rows.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    // Full scan keeps every scored row; the default screen stays capped so the
    // polled snapshot and the UI table keep their original weight.
    if !full_scan {
        rows.truncate(MAX_ROWS);
    }

    // Options only for the leading F&O names. Dhan data calls are serialised
    // globally, so this is the most expensive step and runs last.
    let opt_targets: Vec<(usize, String, f64)> = rows
        .iter()
        .enumerate()
        .filter(|(_, (_, _, sym))| {
            scrip::get()
                .map(|sc| sc.has_options(&scrip::fno_underlying(sym), "NSE") == Some(true))
                .unwrap_or(false)
        })
        .take(MAX_OPTIONS)
        .map(|(i, (_, row, sym))| {
            (
                i,
                sym.clone(),
                row.get("price").and_then(|x| x.as_f64()).unwrap_or(0.0),
            )
        })
        .collect();
    {
        let mut c = cache().lock().map_err(|_| "cache lock")?;
        set_stage(&mut c, "fetching option metrics", 0, opt_targets.len());
    }
    let mut done = 0usize;
    let dhan_live = dhan.is_connected().await;
    if !dhan_live && !opt_targets.is_empty() {
        let mut c = cache().lock().map_err(|_| "cache lock")?;
        push_error(&mut c, "Dhan not connected - option metrics skipped");
    }
    for (idx, sym, spot) in opt_targets {
        let snap = if dhan_live {
            option_snapshot(dhan, &sym, spot).await
        } else {
            json!({"available": false, "fno": true})
        };
        if let Some((_, row, _)) = rows.get_mut(idx) {
            row["options"] = snap;
        }
        done += 1;
        {
            let mut c = cache().lock().map_err(|_| "cache lock")?;
            set_stage(&mut c, "fetching option metrics", done, done.max(1));
        }
    }

    let bt_series: Vec<Series> = rows
        .iter()
        .take(20)
        .filter_map(|(_, _, sym)| {
            series.get(sym).map(|cs| Series {
                symbol: sym.clone(),
                cs: cs.clone(),
            })
        })
        .collect();
    let backtest = backtest(&bt_series, &bench);

    let result_rows: Vec<Value> = rows.into_iter().map(|(_, r, _)| r).collect();
    let price_map: HashMap<String, f64> = technical
        .iter()
        .map(|(u, _, _, _)| (u.symbol.clone(), u.price))
        .collect();
    let live = eval_live_predictions(&price_map, &result_rows);
    let accuracy = json!({
        "replay": summarize_entries(&replay, "replay"),
        "live": live,
        "upgrades": upgrades.iter().rev().take(30).cloned().collect::<Vec<_>>(),
        "note": "Replay = 3-9 month old entries re-scored on point-in-time prices with current fundamentals (lookahead) and today's liquid universe (survivorship). Indicative only, not a guarantee.",
        "updated": now_secs(),
    });
    let (snapshot, pushed_now) = {
    let mut c = cache().lock().map_err(|_| "cache lock")?;
    c.as_of = as_of;
    c.at = now_secs();
    c.finished_at = now_secs();
    c.busy = false;
    c.stage = "done".to_string();
    c.done = result_rows.len();
    c.total = total;
    c.rows = result_rows;
    c.backtest = backtest;
    c.accuracy = accuracy;
    c.note = if c.errors.is_empty() {
        "Free sources: Yahoo (NSE+BSE universe, fundamentals), NSE bhavcopy, NSE shareholding, Dhan options".to_string()
    } else {
        format!("Completed with {} source warning(s)", c.errors.len())
    };
    // "Auto send scanned stocks to trading engines": every scan that brings
    // names the engines have not already been given pushes just those NEW names,
    // so a re-scan of the same screen sends nothing. The push is recorded in the
    // persisted filter state so a restart neither repeats it nor loses it.
    let mut pushed_now: Vec<String> = Vec::new();
    if c.filters.auto_push {
        let known: std::collections::HashSet<String> = c
            .filters
            .pushed_symbols
            .iter()
            .map(|x| x.to_uppercase())
            .collect();
        for row in &c.rows {
            if pushed_now.len() >= 50 {
                break;
            }
            let sym = row
                .get("symbol")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim()
                .to_uppercase();
            // A row without a price cannot be bought, so it is not a candidate.
            let px = row
                .get("price")
                .and_then(|x| x.as_f64())
                .or_else(|| row.get("lastPrice").and_then(|x| x.as_f64()))
                .unwrap_or(0.0);
            if sym.is_empty() || known.contains(&sym) || px <= 0.0 {
                continue;
            }
            if pushed_now.iter().any(|x| x == &sym) {
                continue;
            }
            pushed_now.push(sym);
        }
        for sym in &pushed_now {
            c.filters.pushed_symbols.push(sym.clone());
        }
        // Keep the ledger bounded; it only needs to remember recent names.
        let over = c.filters.pushed_symbols.len().saturating_sub(2_000);
        if over > 0 {
            c.filters.pushed_symbols.drain(0..over);
        }
    }
    DONE_AT.store(c.finished_at, Ordering::Relaxed);
    let snap = (*c).clone();
    drop(c);
    (snap, pushed_now)
    };
    let value = serde_json::to_value(&snapshot).unwrap_or(Value::Null);
    let _ = write_json(&data_path("equity_cache.json"), &value);
    // The cache lock MUST be released before this point: the push wakes the
    // equity books, which take their own locks and hit the network, and holding
    // a std MutexGuard across an await would make the scan future non-Send.
    if !pushed_now.is_empty() {
        let (paper, live) = crate::equity_engine::accept_pushed_symbols(&pushed_now);
        if paper > 0 || live > 0 {
            if let Ok(mut note) = cache().lock() {
                note.note = format!(
                    "{} new stock(s) auto-sent to equity engines (paper +{paper}, live +{live})",
                    pushed_now.len()
                );
            }
            // Both books consider the new names immediately.
            crate::equity_engine::run_armed_now(&dhan).await;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

pub fn spawn_refresher(dhan: DhanState) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(45)).await;
        loop {
            let age = now_secs() - DONE_AT.load(Ordering::Relaxed);
            // Respect the operator's Auto-scan cadence: with Auto-scan ON the
            // effective TTL is the chosen minutes, so a newer scan brings newly
            // listed / newly liquid names into the equity book on its own.
            let ttl = cache()
                .lock()
                .ok()
                .map(|c| auto_scan_secs(&c.filters))
                .unwrap_or(SCAN_TTL_SECS);
            if age > ttl && !BUSY.load(Ordering::SeqCst) {
                start_scan(dhan.clone());
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });
}

pub fn start_scan(dhan: DhanState) -> bool {
    if BUSY.swap(true, Ordering::SeqCst) {
        return false;
    }
    tokio::spawn(async move {
        if let Err(e) = scan(&dhan).await {
            if let Ok(mut c) = cache().lock() {
                c.busy = false;
                if c.stage != "failed" {
                    c.stage = "failed".to_string();
                }
                push_error(&mut c, format!("scan aborted: {e}"));
                c.note = format!("scan aborted: {e}");
            }
        }
        BUSY.store(false, Ordering::SeqCst);
    });
    true
}

pub async fn status(State(_st): State<DhanState>) -> Json<Value> {
    let c = cache().lock().ok();
    let (busy, stage, done, total, as_of, at, errors, universe, listed, nse, bse) =
        match c.as_ref() {
            Some(c) => (
                c.busy,
                c.stage.clone(),
                c.done,
                c.total,
                c.as_of.clone(),
                c.at,
                c.errors.clone(),
                c.universe,
                c.listed,
                c.nse,
                c.bse,
            ),
            None => (
                false,
                String::new(),
                0,
                0,
                String::new(),
                0,
                Vec::new(),
                0,
                0,
                0,
                0,
            ),
        };
    Json(json!({
        "busy": busy || BUSY.load(Ordering::Relaxed),
        "stage": stage,
        "done": done,
        "total": total,
        "asOf": as_of,
        "at": at,
        "universe": universe,
        "listed": listed,
        "nse": nse,
        "bse": bse,
        "errors": errors,
        "filters": c.as_ref().map(|x| x.filters.clone()),
        "pushedCount": c.as_ref().map(|x| x.filters.pushed_symbols.len()).unwrap_or(0),
        "nextRefreshAt": at + c.as_ref().map(|x| auto_scan_secs(&x.filters)).unwrap_or(SCAN_TTL_SECS),
        "autoScanSecs": c.as_ref().map(|x| auto_scan_secs(&x.filters)).unwrap_or(SCAN_TTL_SECS),
    }))
}

/// Expected return (%) a research row advertises: the analyst-style upside to
/// the call's target. Used to rank the equity books from the highest possible
/// return to the lowest.
pub fn row_expected_return(row: &Value) -> f64 {
    row.get("call")
        .and_then(|c| c.get("upsidePct"))
        .and_then(|x| x.as_f64())
        .filter(|v| v.is_finite())
        .unwrap_or(0.0)
}

/// Snapshot of the latest screened rows for the equity trade engines.
///
/// Rows are de-duplicated by SYMBOL and by ISIN (a common stock listed on both
/// exchanges must never be fetched twice) and then ordered by EXPECTED RETURN,
/// highest possible return first, so both equity books present the same
/// highest-to-lowest opportunity list.
pub fn engine_snapshot() -> (String, Vec<Value>) {
    let c = cache().lock().ok();
    match c.as_ref() {
        Some(c) => {
            let mut seen_sym = std::collections::HashSet::new();
            let mut seen_isin = std::collections::HashSet::new();
            let mut rows: Vec<Value> = c
                .rows
                .iter()
                .filter(|r| {
                    let sym = r.get("symbol").and_then(|x| x.as_str()).unwrap_or("").trim().to_uppercase();
                    if sym.is_empty() || !seen_sym.insert(sym) {
                        return false;
                    }
                    let isin = r.get("isin").and_then(|x| x.as_str()).unwrap_or("").trim().to_uppercase();
                    if !isin.is_empty() && !seen_isin.insert(isin) {
                        return false;
                    }
                    true
                })
                .cloned()
                .collect();
            rows.sort_by(|a, b| {
                row_expected_return(b)
                    .partial_cmp(&row_expected_return(a))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            (c.as_of.clone(), rows)
        }
        None => (String::new(), Vec::new()),
    }
}

pub async fn cache_json() -> Json<Value> {
    let c = cache().lock().ok();
    match c.as_ref() {
        Some(c) => Json(json!({
            "asOf": c.as_of,
            "at": c.at,
            "busy": c.busy,
            "stage": c.stage,
            "universe": c.universe,
            "listed": c.listed,
            "nse": c.nse,
            "bse": c.bse,
            "filters": c.filters,
            "pushedCount": c.filters.pushed_symbols.len(),
            "note": c.note,
            "errors": c.errors,
            "backtest": c.backtest,
            "accuracy": c.accuracy,
            "rows": c.rows,
        })),
        None => Json(json!({"rows": [], "backtest": {"available": false}})),
    }
}

pub async fn refresh(State(st): State<DhanState>) -> Json<Value> {
    let started = start_scan(st);
    Json(json!({"started": started, "busy": BUSY.load(Ordering::Relaxed)}))
}

/// Persist the "Scan all listed stocks" switch (and re-run the scan so the new
/// mode takes effect immediately). Accepts `{ "scanAll": true|false }`, but a
/// plain `{"scanAll": false}` reset must never silently drop the other filters,
/// so only the one key is touched.
pub async fn filters_set(State(st): State<DhanState>, Json(v): Json<Value>) -> Json<Value> {
    let scan_all = v.get("scanAll").and_then(|x| x.as_bool());
    let auto_scan = v.get("autoScan").and_then(|x| x.as_bool());
    let auto_mins = v.get("autoScanMinutes").and_then(|x| x.as_f64());
    let auto_push = v.get("autoPush").and_then(|x| x.as_bool());
    let mut changed = false;
    if let Some(mut c) = cache().lock().ok() {
        if let Some(on) = scan_all {
            if c.filters.scan_all != on {
                c.filters.scan_all = on;
                changed = true;
            }
        }
        // Auto-scan cadence is picked up by the refresher on its next 60-second
        // tick, so no manual kick is needed here (and none is wanted: an
        // explicit "Refresh now" must not immediately re-fire).
        if let Some(on) = auto_scan {
            c.filters.auto_scan = on;
        }
        if let Some(mins) = auto_mins {
            c.filters.auto_scan_minutes = mins.max(5.0).min(24.0 * 60.0);
        }
        if let Some(on) = auto_push {
            if on && !c.filters.auto_push {
                // Switching ON must not immediately dump the ENTIRE current
                // screen into the books. Seed the ledger with what is already
                // known, so only genuinely NEW names are sent from here on.
                let seed: Vec<String> = c
                    .rows
                    .iter()
                    .filter_map(|row| row.get("symbol").and_then(|x| x.as_str()))
                    .map(|s| s.trim().to_uppercase())
                    .filter(|s| !s.is_empty())
                    .collect();
                for sym in seed {
                    if !c.filters.pushed_symbols.iter().any(|x| x == &sym) {
                        c.filters.pushed_symbols.push(sym);
                    }
                }
            }
            c.filters.auto_push = on;
        }
        let snap = c.filters.clone();
        drop(c);
        // Patch only the filters key: writing the whole cache here could race a
        // running scan and lose its freshly scored rows.
        let path = data_path("equity_cache.json");
        let mut doc = read_json(&path).unwrap_or_else(|| json!({}));
        if let Some(obj) = doc.as_object_mut() {
            obj.insert("filters".to_string(), serde_json::to_value(&snap).unwrap_or(Value::Null));
        }
        let _ = write_json(&path, &doc);
        if changed {
            let _ = start_scan(st);
        }
        return Json(json!({"ok": true, "filters": snap, "rescan": changed, "autoScanSecs": auto_scan_secs(&snap)}));
    }
    Json(json!({"ok": false}))
}

/// Forget which symbols were already pushed, so the next scan re-sends the whole
/// current screen (the operator asked for a fresh hand-over).
pub async fn push_reset() -> Json<Value> {
    if let Ok(mut c) = cache().lock() {
        let n = c.filters.pushed_symbols.len();
        c.filters.pushed_symbols.clear();
        let path = data_path("equity_cache.json");
        let mut doc = read_json(&path).unwrap_or_else(|| json!({}));
        if let Some(obj) = doc.as_object_mut() {
            obj.insert(
                "filters".to_string(),
                serde_json::to_value(&c.filters).unwrap_or(Value::Null),
            );
        }
        let _ = write_json(&path, &doc);
        return Json(json!({"ok": true, "cleared": n}));
    }
    Json(json!({"ok": false}))
}

pub async fn stock(Query(q): Query<HashMap<String, String>>) -> Json<Value> {
    let symbol = q
        .get("symbol")
        .map(|s| s.trim().to_uppercase())
        .unwrap_or_default();
    if symbol.is_empty() {
        return Json(json!({"error": "symbol required"}));
    }
    let c = cache().lock().ok();
    let row = c
        .as_ref()
        .and_then(|c| {
            c.rows
                .iter()
                .find(|r| r.get("symbol").and_then(|x| x.as_str()) == Some(symbol.as_str()))
        })
        .cloned();
    match row {
        Some(r) => Json(r),
        None => Json(json!({"error": "not in the current screen", "symbol": symbol})),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(closes: &[f64]) -> Vec<Candle> {
        closes
            .iter()
            .enumerate()
            .map(|(i, c)| Candle {
                time: i as i64,
                open: *c * 0.995,
                high: *c * 1.01,
                low: *c * 0.99,
                close: *c,
                volume: 100_000.0,
            })
            .collect()
    }

    #[test]
    fn scan_all_defaults_off_and_round_trips_through_settings() {
        // The "Scan all listed stocks" switch must default OFF (the cheap
        // 900-stock sample) and still be encoded/decoded by the cached filters.
        let d = Filters::default();
        assert!(!d.scan_all);
        let v = serde_json::to_value(&d).unwrap();
        assert_eq!(v.get("scan_all").and_then(|x| x.as_bool()), Some(false));
        let back: Filters = serde_json::from_value(json!({"scan_all": true})).unwrap();
        assert!(back.scan_all);
        // An old cache file written before the switch existed must not fail.
        let old: Filters = serde_json::from_value(json!({"max_price": 0.0})).unwrap();
        assert!(!old.scan_all);
    }

    fn uni(sym: &str, exch: &str, isin: &str, avg3: f64) -> Uni {
        Uni {
            symbol: sym.to_string(),
            ysym: format!("{sym}.NS"),
            name: sym.to_string(),
            exch: exch.to_string(),
            isin: isin.to_string(),
            cap: 0.0,
            price: 100.0,
            chg: 0.0,
            avg3,
            avg10: 0.0,
            high52: 0.0,
            low52: 0.0,
            dma50: 0.0,
            dma200: 0.0,
            pe: 0.0,
            pb: 0.0,
            div: 0.0,
            disc: 0.0,
        }
    }

    #[test]
    fn auto_scan_defaults_off_and_clamps_the_cadence() {
        // Auto scan must default OFF so the app keeps the old 6-hour TTL, and the
        // operator's minutes must be clamped to a sane range so a bad payload
        // can never make the refresher spin.
        let d = Filters::default();
        assert!(!d.auto_scan);
        assert_eq!(d.auto_scan_minutes, 60.0);
        assert_eq!(auto_scan_secs(&d), SCAN_TTL_SECS);

        let mut f = Filters::default();
        f.auto_scan = true;
        f.auto_scan_minutes = 15.0;
        assert_eq!(auto_scan_secs(&f), 15 * 60);

        f.auto_scan_minutes = 0.0;
        assert_eq!(auto_scan_secs(&f), 5 * 60, "clamped up to 5 minutes");
        f.auto_scan_minutes = 99999.0;
        assert_eq!(auto_scan_secs(&f), 24 * 60 * 60, "clamped to 1 day");

        let back: Filters = serde_json::from_value(json!({"auto_scan": true})).unwrap();
        assert!(back.auto_scan);
        assert_eq!(back.auto_scan_minutes, 60.0, "old cache file still loads");
    }

    #[test]
    fn auto_push_defaults_off_and_keeps_a_ledger() {
        // The push must default OFF (an upgraded install must not suddenly send
        // its whole screen to the live book) and must round-trip its ledger.
        let d = Filters::default();
        assert!(!d.auto_push);
        assert!(d.pushed_symbols.is_empty());

        let back: Filters = serde_json::from_value(json!({
            "auto_push": true,
            "pushed_symbols": ["ITC", "BEL"]
        }))
        .unwrap();
        assert!(back.auto_push);
        assert_eq!(back.pushed_symbols, vec!["ITC".to_string(), "BEL".to_string()]);

        // An older cache file without the new keys still loads, with the switch
        // off and an empty ledger.
        let old: Filters = serde_json::from_value(json!({"auto_scan": true})).unwrap();
        assert!(!old.auto_push);
        assert!(old.pushed_symbols.is_empty());
    }

    #[test]
    fn dual_listings_are_merged_into_one_company_keeping_nse() {
        // The same company listed on NSE and BSE under different tickers shares
        // an ISIN: only the NSE row may survive so the books never fetch it twice.
        let mut merged: HashMap<String, Uni> = HashMap::new();
        merged.insert("TCS".into(), uni("TCS", "NSE", "INE467B01029", 500.0));
        merged.insert("TCSBO".into(), uni("TCSBO", "BSE", "INE467B01029", 900.0));
        let out = dedupe_by_company(merged);
        assert_eq!(out.len(), 1);
        assert!(out.contains_key("TCS"), "NSE row wins");
        assert!(!out.contains_key("TCSBO"));

        // Distinct companies (distinct ISINs) are both kept.
        let mut two: HashMap<String, Uni> = HashMap::new();
        two.insert("INFY".into(), uni("INFY", "NSE", "INE009A01021", 100.0));
        two.insert("WIPRO".into(), uni("WIPRO", "BSE", "INE075A01022", 90.0));
        assert_eq!(dedupe_by_company(two).len(), 2);

        // Rows without an ISIN fall back to symbol-level identity only.
        let mut no_isin: HashMap<String, Uni> = HashMap::new();
        no_isin.insert("AAA".into(), uni("AAA", "NSE", "", 10.0));
        no_isin.insert("BBB".into(), uni("BBB", "BSE", "", 20.0));
        assert_eq!(dedupe_by_company(no_isin).len(), 2);
    }

    #[test]
    fn expected_return_reads_the_call_upside() {
        let row = json!({"symbol": "X", "call": {"status": "Buy", "upsidePct": 18.5}});
        assert_eq!(row_expected_return(&row), 18.5);
        // Missing / malformed upside ranks as zero rather than breaking sorting.
        assert_eq!(row_expected_return(&json!({"symbol": "X"})), 0.0);
        assert_eq!(row_expected_return(&json!({"call": {"upsidePct": null}})), 0.0);
    }

    #[test]
    fn rsi_is_bounded_and_high_in_uptrend() {
        let mut c: Vec<f64> = (0..80).map(|i| 100.0 + i as f64).collect();
        let r = rsi(&c, 14);
        assert!(r > 90.0 && r <= 100.0);
        c = (0..80).map(|i| 200.0 - i as f64).collect();
        let r = rsi(&c, 14);
        assert!(r < 10.0 && r >= 0.0);
    }

    #[test]
    fn atr_is_positive_for_real_ranges() {
        let cs = mk(&(0..60).map(|i| 100.0 + i as f64).collect::<Vec<_>>());
        assert!(atr(&cs, 14) > 0.0);
    }

    #[test]
    fn technical_score_prefers_uptrend() {
        let up = mk(&(0..300).map(|i| 100.0 + i as f64 * 0.8).collect::<Vec<_>>());
        let down = mk(&(0..300).map(|i| 340.0 - i as f64 * 0.8).collect::<Vec<_>>());
        let (s_up, _, _) = tech(&up, 10.0, 0.0);
        let (s_dn, _, _) = tech(&down, 10.0, 0.0);
        assert!(s_up > s_dn, "up {s_up} down {s_dn}");
    }

    #[test]
    fn total_score_weights_are_sane() {
        assert_eq!(r2(total_score(100.0, 100.0, 100.0, 100.0)), 100.0);
        assert_eq!(r2(total_score(0.0, 0.0, 0.0, 0.0)), 0.0);
        let s = total_score(80.0, 70.0, 60.0, 50.0);
        assert!(s > 60.0 && s < 80.0);
    }

    #[test]
    fn fundamentals_metric_extraction() {
        let v = json!({"quoteSummary": {"result": [{
            "financialData": {
                "returnOnEquity": {"raw": 0.22},
                "debtToEquity": {"raw": 35.0},
                "totalDebt": {"raw": 1.0e9},
                "operatingMargins": {"raw": 0.18},
                "revenueGrowth": {"raw": 0.12}
            },
            "defaultKeyStatistics": {"trailingPE": {"raw": 24.0}},
            "incomeStatementHistory": {"incomeStatementHistory": [
                {"endDate": {"fmt": "2025-03-31"}, "totalRevenue": {"raw": 1200.0}, "netIncome": {"raw": 120.0}},
                {"endDate": {"fmt": "2022-03-31"}, "totalRevenue": {"raw": 800.0}, "netIncome": {"raw": 70.0}}
            ]},
            "cashflowStatementHistory": {"cashflowStatementHistory": [
                {"totalCashFromOperatingActivities": {"raw": 150.0}}
            ]}
        }]}});
        let f = parse_fundamentals(&v).expect("parsed");
        assert!((fval(&f, "roe").unwrap() - 22.0).abs() < 0.01);
        assert!((fval(&f, "debtEquity").unwrap() - 0.35).abs() < 0.01);
        assert!(fval(&f, "revenueCagr3").unwrap() > 10.0);
        let (score, keys) = fundamental_score(&f);
        assert!(keys >= 5);
        assert!(score > 40.0);
    }

    #[test]
    fn tight_base_scores_above_volatile_base() {
        let mut tight: Vec<Candle> = (0..150)
            .map(|i| Candle {
                time: i,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                volume: 100_000.0,
            })
            .collect();
        tight.push(Candle {
            time: 150,
            open: 101.0,
            high: 103.5,
            low: 100.5,
            close: 103.0,
            volume: 250_000.0,
        });
        let volatile: Vec<Candle> = (0..150)
            .map(|i| {
                let c = if i % 2 == 0 { 75.0 } else { 125.0 };
                Candle {
                    time: i,
                    open: c,
                    high: c * 1.06,
                    low: c * 0.94,
                    close: c,
                    volume: 100_000.0,
                }
            })
            .collect();
        let bt = base_metrics(&tight);
        let bv = base_metrics(&volatile);
        assert!(bt.get("breakout26w").and_then(|x| x.as_bool()).unwrap_or(false));
        assert!(base_score(&bt) > 70.0, "tight {}", base_score(&bt));
        assert!(base_score(&bt) > base_score(&bv));
    }

    #[test]
    fn strategy_tags_pick_up_multibagger_playbooks() {
        let f = json!({
            "roe": 25.0, "debtEquity": 0.3, "revenueCagr3": 22.0,
            "profitCagr3": 25.0, "revGrowth": 20.0, "earnGrowth": 35.0,
            "pe": 20.0, "pb": 3.0, "peg": 0.9, "divYield": 1.0, "cfoPat": 1.1
        });
        let t = json!({
            "fromHighPct": -2.0, "rs6m": 30.0, "ret1y": 50.0, "volRatio": 1.6,
            "base": {"available": true, "breakout26w": true, "tight20": 8.0,
                     "depth120": 25.0, "updownVol": 1.8, "stage2": true}
        });
        let u = Uni {
            symbol: "TEST".into(),
            cap: 1000.0 * 1e7,
            ..Uni::default()
        };
        let (s, tags) = strategy_scores(&f, &t, &u, 70.0, 60.0, Some(50.0), Some(1.5));
        let get = |k: &str| s.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
        assert!(get("coffee") > 65.0, "coffee {}", get("coffee"));
        assert!(get("garp") > 65.0, "garp {}", get("garp"));
        assert!(get("momentum") > 65.0, "momentum {}", get("momentum"));
        assert!(tags.iter().any(|x| x == "Momentum"));
        assert!(tags.iter().any(|x| x == "Near 52w high"));
        assert!(tags.iter().any(|x| x == "Promoter rising"));
    }

    #[test]
    fn buy_call_is_bounded_and_ordered() {
        let snap = json!({
            "high52": 150.0, "ema50": 90.0, "ema200": 80.0,
            "base": {"depth120": 25.0}
        });
        let fund = json!({"peg": 0.8, "pe": 12.0, "earnGrowth": 30.0, "revGrowth": 18.0});
        let call = buy_call(78.0, 80.0, &snap, &fund, &[], 100.0);
        let st = call.get("status").and_then(|x| x.as_str()).unwrap_or("");
        assert!(matches!(st, "Strong Buy" | "Buy"), "status {st}");
        let up = call.get("upsidePct").and_then(|x| x.as_f64()).unwrap();
        let low = call.get("upsideLowPct").and_then(|x| x.as_f64()).unwrap();
        let high = call.get("upsideHighPct").and_then(|x| x.as_f64()).unwrap();
        assert!(up > 5.0 && low <= up && high >= up);
        assert!(call.get("targetPrice").and_then(|x| x.as_f64()).unwrap() > 100.0);
        assert!(call.get("stopPrice").and_then(|x| x.as_f64()).unwrap() < 100.0);
        let conf = call.get("confidence").and_then(|x| x.as_f64()).unwrap();
        assert!((15.0..=95.0).contains(&conf));
        assert!(call.get("rr").and_then(|x| x.as_f64()).unwrap() > 1.0);

        let bad = buy_call(
            40.0,
            60.0,
            &snap,
            &json!({}),
            &["No fundamentals".to_string(), "Illiquid".to_string()],
            100.0,
        );
        let bst = bad.get("status").and_then(|x| x.as_str()).unwrap_or("");
        assert!(matches!(bst, "Watch" | "Avoid"), "status {bst}");
        let bconf = bad.get("confidence").and_then(|x| x.as_f64()).unwrap();
        assert!(bconf < conf);
    }

    #[test]
    fn accuracy_summary_counts_hits_and_near() {
        let entries = vec![
            json!({"date": "2026-01-10", "symbol": "A", "confidence": 72.0,
                   "upsidePct": 20.0, "outcome": "hit", "win": true, "rMultiple": 2.0, "returnPct": 20.0}),
            json!({"date": "2026-01-18", "symbol": "B", "confidence": 66.0,
                   "upsidePct": 15.0, "outcome": "near", "win": false, "rMultiple": -1.0, "returnPct": 9.0}),
            json!({"date": "2026-02-02", "symbol": "C", "confidence": 61.0,
                   "upsidePct": 12.0, "outcome": "miss", "win": false, "rMultiple": -0.5, "returnPct": -6.0}),
            json!({"date": "2026-02-10", "symbol": "D", "confidence": 80.0,
                   "upsidePct": 18.0, "outcome": "open", "returnPct": 0.0}),
        ];
        let s = summarize_entries(&entries, "test");
        assert_eq!(s.get("evaluated").and_then(|x| x.as_u64()), Some(3));
        assert_eq!(s.get("hit").and_then(|x| x.as_u64()), Some(1));
        assert_eq!(s.get("near").and_then(|x| x.as_u64()), Some(1));
        assert_eq!(s.get("miss").and_then(|x| x.as_u64()), Some(1));
        assert_eq!(s.get("open").and_then(|x| x.as_u64()), Some(1));
        // Only target-before-stop counts as a win: 1 of 3, not 2 of 3.
        assert_eq!(s.get("wins").and_then(|x| x.as_u64()), Some(1));
        assert!((s.get("goodRate").and_then(|x| x.as_f64()).unwrap() - 33.33).abs() < 0.1);
        // EV in R = (2.0 - 1.0 - 0.5) / 3.
        assert!((s.get("evR").and_then(|x| x.as_f64()).unwrap() - 0.17).abs() < 0.1);
        assert!((s.get("avgReturn").and_then(|x| x.as_f64()).unwrap() - 7.67).abs() < 0.1);
        assert_eq!(s.get("months").and_then(|x| x.as_array()).map(|a| a.len()), Some(2));
        assert_eq!(s.get("log").and_then(|x| x.as_array()).map(|a| a.len()), Some(4));
    }

    #[test]
    fn horizon_stats_give_monotonic_probabilities() {
        let entries: Vec<Value> = (0..8)
            .map(|i| {
                json!({
                    "confidence": 82.0,
                    "availBars": 252,
                    "barsToTarget": if i < 6 { json!(20 + i * 20) } else { Value::Null },
                    "outcome": if i < 6 { "hit" } else { "miss" },
                })
            })
            .collect();
        let st = horizon_stats(&entries);
        assert!(st.median_days >= 20.0 && st.median_days <= 120.0);
        let mut call = json!({"confidence": 84.0});
        apply_horizon(&mut call, 3.0, 20.0, &st);
        let p3 = call.get("prob3m").and_then(|x| x.as_f64()).unwrap();
        let p6 = call.get("prob6m").and_then(|x| x.as_f64()).unwrap();
        let p12 = call.get("prob12m").and_then(|x| x.as_f64()).unwrap();
        assert!(p3 <= p6 && p6 <= p12, "{p3} {p6} {p12}");
        assert!(p12 > p3);
        assert!((2.0..=98.0).contains(&p3) && (2.0..=98.0).contains(&p12));
        let tf = call.get("timeframe").and_then(|x| x.as_str()).unwrap();
        assert!(tf.ends_with("mo") || tf.ends_with("weeks"));
        let months = call
            .get("expectedMonths")
            .and_then(|x| x.as_f64())
            .unwrap();
        assert!(months > 0.0);
    }

    #[test]
    fn next_step_guides_upgrade_path() {
        assert_eq!(next_step(70.0, 10.0, 70.0), "Buy zone");
        assert!(next_step(64.0, 10.0, 60.0).starts_with("Near Buy"));
        assert!(next_step(63.0, 10.0, 50.0).contains("breakout"));
        assert!(next_step(50.0, -12.0, 40.0).contains("Momentum weak"));
        assert_eq!(next_step(50.0, 2.0, 55.0), "Far from Buy");
    }

    #[test]
    fn yahoo_date_codec_roundtrips() {
        let d = days_from_civil(2026, 10, 23);
        assert_eq!(ymd(d), "2026-10-23");
        assert_eq!(parse_ymd("2026-10-23"), Some(d));
        assert_eq!(weekday(d), 5);
    }

    #[test]
    fn calibration_rejects_a_proven_losing_band() {
        // 30 replays on the 66-73 band: 8 wins x +2R, 22 losses x -1R = -6R/30.
        let mut replay = Vec::new();
        for i in 0..30 {
            let win = i < 8;
            replay.push(json!({
                "total": 68.0,
                "outcome": if win { "hit" } else { "stopped" },
                "win": win,
                "rMultiple": if win { 2.0 } else { -1.0 },
            }));
        }
        let calib = build_calib(&replay);
        // The band EV is negative (-0.2R) and there is enough evidence to judge.
        let mut call = json!({});
        let ok = apply_calibration(&mut call, 68.0, &calib);
        assert!(!ok, "a measured-negative-EV band must be rejected");
        assert!(call.get("evR").and_then(|x| x.as_f64()).unwrap() < 0.0);
        // Confidence must be the measured win-rate (~27%), never a high fake.
        let conf = call.get("confidence").and_then(|x| x.as_f64()).unwrap();
        assert!(conf < 40.0, "confidence {conf} should reflect the low win-rate");
        assert_eq!(call.get("calibrated").and_then(|x| x.as_bool()), Some(true));
    }

    #[test]
    fn calibration_accepts_a_profitable_band() {
        // 30 replays: 18 wins x +2R, 12 losses x -1R = +24R/30 = +0.8R.
        let mut replay = Vec::new();
        for i in 0..30 {
            let win = i < 18;
            replay.push(json!({
                "total": 68.0,
                "outcome": if win { "hit" } else { "stopped" },
                "win": win,
                "rMultiple": if win { 2.0 } else { -1.0 },
            }));
        }
        let calib = build_calib(&replay);
        let mut call = json!({});
        let ok = apply_calibration(&mut call, 68.0, &calib);
        assert!(ok);
        assert!(call.get("evR").and_then(|x| x.as_f64()).unwrap() > 0.0);
    }

    #[test]
    fn calibration_is_neutral_with_no_evidence() {
        // No replay data: never reject, and confidence stays an honest ~50.
        let calib = build_calib(&[]);
        let mut call = json!({});
        let ok = apply_calibration(&mut call, 75.0, &calib);
        assert!(ok, "no evidence must not reject");
        let conf = call.get("confidence").and_then(|x| x.as_f64()).unwrap();
        assert!((conf - 50.0).abs() < 10.0, "conf {conf} should be ~neutral");
        assert_eq!(call.get("calibrated").and_then(|x| x.as_bool()), Some(false));
    }

    #[test]
    fn replay_scores_stop_before_target() {
        // The summariser must treat a stop-touch as a loss even if the same sample
        // also touched the target: this is what makes the accuracy panel honest.
        let entries = vec![
            json!({"date": "2026-03-01", "symbol": "X", "confidence": 70.0, "total": 68.0,
                   "outcome": "stopped", "win": false, "rMultiple": -1.0, "returnPct": -5.0}),
        ];
        let s = summarize_entries(&entries, "t");
        assert_eq!(s.get("wins").and_then(|x| x.as_u64()), Some(0));
        assert_eq!(s.get("goodRate").and_then(|x| x.as_f64()), Some(0.0));
        assert_eq!(s.get("evR").and_then(|x| x.as_f64()), Some(-1.0));
    }
}
