use algo_core::model::Candle;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use crate::broker::DhanState;

const CTX_TTL_SECS: u64 = 300;
const DAILY_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const IST_OFFSET_SECS: i64 = 19800;

const YAHOO: &[(&str, &str)] = &[
    ("^GSPC", "S&P 500"),
    ("^IXIC", "Nasdaq"),
    ("^N225", "Nikkei"),
    ("^HSI", "Hang Seng"),
    ("DX-Y.NYB", "Dollar index"),
    ("^TNX", "US 10Y"),
    ("CL=F", "WTI crude"),
    ("BZ=F", "Brent crude"),
    ("GC=F", "COMEX gold"),
    ("SI=F", "COMEX silver"),
    ("HG=F", "COMEX copper"),
    ("NG=F", "Henry Hub gas"),
    ("USDINR=X", "USD/INR"),
    ("^INDIAVIX", "India VIX"),
];

const SECTOR_KEYS: &[&str] = &[
    "NIFTY 50",
    "NIFTY BANK",
    "NIFTY FIN SERVICE",
    "NIFTY IT",
    "NIFTY AUTO",
    "NIFTY FMCG",
    "NIFTY METAL",
    "NIFTY PHARMA",
    "NIFTY ENERGY",
    "NIFTY PSU BANK",
    "NIFTY REALTY",
    "NIFTY MIDCAP 100",
    "NIFTY SMALLCAP 100",
];

fn ctx_store() -> &'static Mutex<Value> {
    static CTX: OnceLock<Mutex<Value>> = OnceLock::new();
    CTX.get_or_init(|| Mutex::new(json!({ "ok": false, "at": 0 })))
}

pub fn snapshot() -> Value {
    ctx_store()
        .lock()
        .map(|g| g.clone())
        .unwrap_or_else(|_| json!({ "ok": false }))
}

pub fn context_json() -> Value {
    snapshot()
}

pub fn spawn_refresher() {
    tokio::spawn(async move {
        loop {
            refresh_context().await;
            tokio::time::sleep(Duration::from_secs(CTX_TTL_SECS)).await;
        }
    });
}

async fn refresh_context() {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AlgoDhanResearch")
        .build();
    let Ok(client) = client else { return };

    let mut global = serde_json::Map::new();
    let mut global_err = String::new();
    let mut usdinr = 0.0;
    let mut usdinr_chg = 0.0;
    for (sym, label) in YAHOO {
        match yahoo_quote(&client, sym).await {
            Some((last, chg)) => {
                if *sym == "USDINR=X" {
                    usdinr = last;
                    usdinr_chg = chg;
                }
                global.insert(
                    sym.to_string(),
                    json!({ "symbol": sym, "label": label, "last": round(last, 4), "chgPct": round(chg, 3) }),
                );
            }
            None => {
                if global_err.is_empty() {
                    global_err = format!("{sym} unavailable");
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(120)).await;
    }

    let (sectors, flows) = tokio::join!(nse_sectors(&client), nse_flows(&client));
    let events = load_events();

    let value = json!({
        "ok": !global.is_empty() || sectors.get("available").and_then(|v| v.as_bool()).unwrap_or(false),
        "at": now_ms(),
        "global": {
            "available": !global.is_empty(),
            "items": Value::Object(global),
            "error": global_err,
            "source": "Yahoo Finance",
        },
        "sectors": sectors,
        "flows": flows,
        "usdinr": { "last": round(usdinr, 4), "chgPct": round(usdinr_chg, 3) },
        "events": events,
    });
    if let Ok(mut g) = ctx_store().lock() {
        *g = value;
    }
}

async fn yahoo_quote(client: &reqwest::Client, sym: &str) -> Option<(f64, f64)> {
    let url = format!(
        "https://query1.finance.yahoo.com/v8/finance/chart/{}?range=5d&interval=1d",
        urlencode(sym)
    );
    let text = client.get(url).send().await.ok()?.text().await.ok()?;
    let v: Value = serde_json::from_str(&text).ok()?;
    let r = v.get("chart")?.get("result")?.get(0)?;
    let meta = r.get("meta");
    let mut last = meta
        .and_then(|m| m.get("regularMarketPrice"))
        .and_then(|x| x.as_f64())
        .unwrap_or(0.0);
    let mut prev = meta
        .and_then(|m| m.get("previousClose"))
        .and_then(|x| x.as_f64())
        .or_else(|| meta.and_then(|m| m.get("chartPreviousClose")).and_then(|x| x.as_f64()))
        .unwrap_or(0.0);
    if let Some(closes) = r
        .get("indicators")
        .and_then(|x| x.get("quote"))
        .and_then(|x| x.get(0))
        .and_then(|x| x.get("close"))
        .and_then(|x| x.as_array())
    {
        let vals: Vec<f64> = closes.iter().filter_map(|x| x.as_f64()).collect();
        if let Some(v) = vals.last() {
            if last <= 0.0 {
                last = *v;
            }
        }
        if prev <= 0.0 && vals.len() >= 2 {
            prev = vals[vals.len() - 2];
        }
    }
    if last <= 0.0 || prev <= 0.0 {
        return None;
    }
    Some((last, (last - prev) / prev * 100.0))
}

async fn nse_sectors(client: &reqwest::Client) -> Value {
    let body = match client
        .get("https://www.nseindia.com/api/allIndices")
        .header("Referer", "https://www.nseindia.com/")
        .header("Accept", "application/json")
        .send()
        .await
    {
        Ok(r) => r.text().await.unwrap_or_default(),
        Err(e) => {
            return json!({ "available": false, "error": e.to_string(), "items": {}, "source": "NSE allIndices" });
        }
    };
    let Ok(v) = serde_json::from_str::<Value>(&body) else {
        return json!({ "available": false, "error": "NSE allIndices parse failed", "items": {}, "source": "NSE allIndices" });
    };
    let mut items = serde_json::Map::new();
    if let Some(rows) = v.get("data").and_then(|x| x.as_array()) {
        for r in rows {
            let name = r
                .get("indexSymbol")
                .or_else(|| r.get("index"))
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim()
                .to_uppercase();
            if name.is_empty() {
                continue;
            }
            let chg = r.get("percentChange").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let last = r.get("last").and_then(|x| x.as_f64()).unwrap_or(0.0);
            items.insert(name, json!({ "chgPct": round(chg, 3), "last": round(last, 2) }));
        }
    }
    let mut up = 0usize;
    let mut total = 0usize;
    let mut rows: Vec<(String, f64)> = Vec::new();
    for k in SECTOR_KEYS {
        if let Some(x) = items.get(*k) {
            total += 1;
            let chg = x.get("chgPct").and_then(|v| v.as_f64()).unwrap_or(0.0);
            if chg > 0.0 {
                up += 1;
            }
            rows.push(((*k).to_string(), chg));
        }
    }
    rows.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let leaders: Vec<Value> = rows.iter().take(3).map(|(n, c)| json!({ "name": n, "chgPct": round(*c, 3) })).collect();
    let laggards: Vec<Value> = rows.iter().rev().take(3).map(|(n, c)| json!({ "name": n, "chgPct": round(*c, 3) })).collect();
    let nifty = items
        .get("NIFTY 50")
        .and_then(|x| x.get("chgPct"))
        .and_then(|x| x.as_f64())
        .unwrap_or(0.0);
    json!({
        "available": total > 0,
        "breadthPct": if total > 0 { round(up as f64 / total as f64 * 100.0, 1) } else { 0.0 },
        "niftyPct": round(nifty, 3),
        "up": up,
        "total": total,
        "leaders": leaders,
        "laggards": laggards,
        "items": Value::Object(items),
        "source": "NSE allIndices",
        "asOf": now_ms(),
    })
}

async fn nse_flows(client: &reqwest::Client) -> Value {
    let body = match client
        .get("https://www.nseindia.com/api/fiidiiTradeReact")
        .header("Referer", "https://www.nseindia.com/")
        .header("Accept", "application/json")
        .send()
        .await
    {
        Ok(r) => r.text().await.unwrap_or_default(),
        Err(e) => return json!({ "available": false, "error": e.to_string(), "source": "NSE FII/DII" }),
    };
    let Ok(rows) = serde_json::from_str::<Value>(&body) else {
        return json!({ "available": false, "error": "NSE FII/DII parse failed", "source": "NSE FII/DII" });
    };
    let mut fii = 0.0;
    let mut dii = 0.0;
    let mut date = String::new();
    if let Some(arr) = rows.as_array() {
        for r in arr {
            let cat = r.get("category").and_then(|x| x.as_str()).unwrap_or("").to_uppercase();
            let net = r
                .get("netValue")
                .and_then(|x| x.as_str())
                .and_then(|x| x.parse::<f64>().ok())
                .or_else(|| r.get("netValue").and_then(|x| x.as_f64()))
                .unwrap_or(0.0);
            if date.is_empty() {
                date = r.get("date").and_then(|x| x.as_str()).unwrap_or("").to_string();
            }
            if cat.contains("FII") || cat.contains("FPI") {
                fii += net;
            } else if cat.contains("DII") {
                dii += net;
            }
        }
    }
    json!({
        "available": !date.is_empty(),
        "date": date,
        "fiiNet": round(fii, 2),
        "diiNet": round(dii, 2),
        "totalNet": round(fii + dii, 2),
        "source": "NSE FII/DII provisional",
        "asOf": now_ms(),
    })
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => out.push(c),
            _ => {
                for b in c.to_string().as_bytes() {
                    out.push_str(&format!("%{b:02X}"));
                }
            }
        }
    }
    out
}

fn events_path() -> std::path::PathBuf {
    std::path::PathBuf::from("data/target_events.json")
}

fn load_events() -> Value {
    let raw = std::fs::read_to_string(events_path()).unwrap_or_default();
    if raw.trim().is_empty() {
        return json!([]);
    }
    let v: Value = serde_json::from_str(&raw).unwrap_or_else(|_| json!([]));
    match v {
        Value::Array(a) => Value::Array(a),
        Value::Object(o) => o.get("events").cloned().unwrap_or_else(|| json!([])),
        _ => json!([]),
    }
}

pub fn events_get() -> Value {
    json!({ "ok": true, "events": load_events() })
}

pub fn events_post(body: Value) -> Value {
    let list = match body.get("events") {
        Some(Value::Array(a)) => Value::Array(a.clone()),
        _ => match body {
            Value::Array(a) => Value::Array(a),
            _ => load_events(),
        },
    };
    if let Some(dir) = events_path().parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let _ = std::fs::write(events_path(), serde_json::to_string_pretty(&list).unwrap_or_default());
    if let Ok(mut g) = ctx_store().lock() {
        g["events"] = list.clone();
    }
    json!({ "ok": true, "events": list })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn round(v: f64, dp: i32) -> f64 {
    if !v.is_finite() {
        return 0.0;
    }
    let f = 10f64.powi(dp);
    (v * f).round() / f
}

fn clamp(v: f64, lo: f64, hi: f64) -> f64 {
    v.max(lo).min(hi)
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let mut y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    if m <= 2 {
        y += 1;
    }
    (y, m, d)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64;
    let doy = (153 * mp + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

fn today_ist_days() -> i64 {
    (now_ms() / 1000 + IST_OFFSET_SECS) / 86_400
}

fn parse_ymd(s: &str) -> Option<i64> {
    let parts: Vec<&str> = s.trim().split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let y = parts[0].parse::<i64>().ok()?;
    let m = parts[1].parse::<u32>().ok()?;
    let d = parts[2].parse::<u32>().ok()?;
    Some(days_from_civil(y, m, d))
}

fn ema(vals: &[f64], n: usize) -> f64 {
    if vals.is_empty() || n == 0 {
        return 0.0;
    }
    let k = 2.0 / (n as f64 + 1.0);
    let mut e = vals[0];
    for v in vals.iter().skip(1) {
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
    let mut count = 0;
    for i in (start + 1)..cs.len() {
        let prev = cs[i - 1].close;
        let tr = (cs[i].high - cs[i].low)
            .max((cs[i].high - prev).abs())
            .max((cs[i].low - prev).abs());
        sum += tr;
        count += 1;
    }
    if count == 0 {
        0.0
    } else {
        sum / count as f64
    }
}

fn adx(cs: &[Candle], n: usize) -> f64 {
    let len = cs.len();
    if len < n * 3 || n == 0 {
        return 0.0;
    }
    let mut tr = vec![0.0; len];
    let mut pdm = vec![0.0; len];
    let mut ndm = vec![0.0; len];
    for i in 1..len {
        let up = cs[i].high - cs[i - 1].high;
        let dn = cs[i - 1].low - cs[i].low;
        pdm[i] = if up > dn && up > 0.0 { up } else { 0.0 };
        ndm[i] = if dn > up && dn > 0.0 { dn } else { 0.0 };
        let prev = cs[i - 1].close;
        tr[i] = (cs[i].high - cs[i].low)
            .max((cs[i].high - prev).abs())
            .max((cs[i].low - prev).abs());
    }
    let mut str_ = tr[1..=n].iter().sum::<f64>();
    let mut spdm = pdm[1..=n].iter().sum::<f64>();
    let mut sndm = ndm[1..=n].iter().sum::<f64>();
    let mut dxs: Vec<f64> = Vec::new();
    for i in (n + 1)..len {
        str_ = str_ - str_ / n as f64 + tr[i];
        spdm = spdm - spdm / n as f64 + pdm[i];
        sndm = sndm - sndm / n as f64 + ndm[i];
        let pdi = if str_ > 0.0 { 100.0 * spdm / str_ } else { 0.0 };
        let ndi = if str_ > 0.0 { 100.0 * sndm / str_ } else { 0.0 };
        let sum = pdi + ndi;
        dxs.push(if sum > 0.0 { 100.0 * (pdi - ndi).abs() / sum } else { 0.0 });
    }
    if dxs.len() < n {
        return 0.0;
    }
    let tail = &dxs[dxs.len() - n..];
    tail.iter().sum::<f64>() / tail.len() as f64
}

fn chop(cs: &[Candle], n: usize) -> f64 {
    if cs.len() < n + 1 || n < 2 {
        return 50.0;
    }
    let s = &cs[cs.len() - n..];
    let hh = s.iter().map(|c| c.high).fold(f64::MIN, f64::max);
    let ll = s.iter().map(|c| c.low).fold(f64::MAX, f64::min);
    let mut tr = 0.0;
    for i in 1..s.len() {
        let prev = s[i - 1].close;
        tr += (s[i].high - s[i].low)
            .max((s[i].high - prev).abs())
            .max((s[i].low - prev).abs());
    }
    if hh <= ll || tr <= 0.0 {
        return 50.0;
    }
    let v = 100.0 * (tr / (hh - ll)).ln() / (n as f64).ln();
    clamp(v, 0.0, 100.0)
}

fn efficiency(cs: &[Candle], n: usize) -> f64 {
    if cs.len() < n + 1 || n == 0 {
        return 0.0;
    }
    let s = &cs[cs.len() - n..];
    let net = (s[s.len() - 1].close - s[0].close).abs();
    let mut path = 0.0;
    for i in 1..s.len() {
        path += (s[i].close - s[i - 1].close).abs();
    }
    if path <= 0.0 {
        0.0
    } else {
        clamp(net / path, 0.0, 1.0)
    }
}

fn resample(cs: &[Candle], factor: usize) -> Vec<Candle> {
    if factor <= 1 {
        return cs.to_vec();
    }
    let mut out = Vec::new();
    let mut i = 0;
    while i < cs.len() {
        let end = (i + factor).min(cs.len());
        let chunk = &cs[i..end];
        if let (Some(first), Some(last)) = (chunk.first(), chunk.last()) {
            out.push(Candle {
                time: first.time,
                open: first.open,
                high: chunk.iter().map(|c| c.high).fold(f64::MIN, f64::max),
                low: chunk.iter().map(|c| c.low).fold(f64::MAX, f64::min),
                close: last.close,
                volume: chunk.iter().map(|c| c.volume).sum(),
            });
        }
        i += factor;
    }
    out
}

pub fn regime(cs: &[Candle], spot: f64, atr_v: f64) -> Value {
    let adx_v = adx(cs, 14);
    let chop_v = chop(cs, 14);
    let eff = efficiency(cs, 20);
    let atr_pct = if spot > 0.0 { atr_v / spot * 100.0 } else { 0.0 };
    let med_atr = {
        let mut v: Vec<f64> = Vec::new();
        if cs.len() > 30 {
            for i in (cs.len() - 30)..cs.len() {
                let a = atr(&cs[..=i], 14);
                if a > 0.0 {
                    v.push(a);
                }
            }
        }
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        v.get(v.len() / 2).copied().unwrap_or(atr_v)
    };
    let atr_ratio = if med_atr > 0.0 { atr_v / med_atr } else { 1.0 };
    let label = if atr_ratio > 1.45 && adx_v < 22.0 {
        "VOLATILE"
    } else if adx_v >= 25.0 && eff >= 0.30 {
        "TREND"
    } else if adx_v < 20.0 && chop_v > 61.8 {
        "RANGE"
    } else {
        "MIXED"
    };
    json!({
        "label": label,
        "adx": round(adx_v, 1),
        "chop": round(chop_v, 1),
        "efficiency": round(eff, 3),
        "atrPct": round(atr_pct, 3),
        "atrRatio": round(atr_ratio, 2),
        "score": match label {
            "TREND" => 5.0,
            "RANGE" => -6.0,
            "VOLATILE" => -4.0,
            _ => 0.0,
        },
    })
}

fn side_of(cs: &[Candle]) -> i64 {
    if cs.len() < 25 {
        return 0;
    }
    let closes: Vec<f64> = cs.iter().map(|c| c.close).collect();
    let e20 = ema(&closes, 20);
    let e50 = ema(&closes, 50usize.min(closes.len()));
    let last = *closes.last().unwrap_or(&0.0);
    let score: i64 = (if last > e20 { 1 } else if last < e20 { -1 } else { 0 })
        + (if e20 > e50 { 1 } else if e20 < e50 { -1 } else { 0 });
    score.signum()
}

pub fn mtf(cs: &[Candle], daily: &[Candle], dir: i64) -> Value {
    let m15 = resample(cs, 3);
    let h1 = resample(cs, 12);
    let s5 = side_of(cs);
    let s15 = side_of(&m15);
    let s1 = side_of(&h1);
    let sd = side_of(daily);
    let mut aligned = 0;
    let mut against = 0;
    if dir != 0 {
        for s in [s5, s15, s1, sd] {
            if s == dir {
                aligned += 1;
            } else if s == -dir {
                against += 1;
            }
        }
    }
    json!({
        "m5": s5,
        "m15": s15,
        "h1": s1,
        "d1": sd,
        "aligned": aligned,
        "against": against,
        "score": if against >= 2 { -9.0 } else if aligned >= 3 { 6.0 } else if aligned >= 2 { 2.0 } else { 0.0 },
    })
}

pub fn seasonality(daily: &[Candle], month: u32) -> Value {
    if daily.len() < 60 {
        return json!({ "available": false, "reason": "daily history short" });
    }
    let mut vals: Vec<f64> = Vec::new();
    for i in 1..daily.len() {
        let prev = daily[i - 1].close;
        if prev <= 0.0 {
            continue;
        }
        let (_, m, _) = civil_from_days(daily[i].time / 86_400);
        if m == month {
            vals.push((daily[i].close - prev) / prev * 100.0);
        }
    }
    if vals.len() < 4 {
        return json!({ "available": false, "reason": "not enough seasonality samples", "samples": vals.len() });
    }
    let avg = vals.iter().sum::<f64>() / vals.len() as f64;
    let win = vals.iter().filter(|v| **v > 0.0).count() as f64 / vals.len() as f64 * 100.0;
    json!({
        "available": true,
        "month": month,
        "avgPct": round(avg, 3),
        "winRate": round(win, 1),
        "samples": vals.len(),
    })
}

fn gex(chain: &[algo_core::option::ChainRow], spot: f64) -> Value {
    if chain.is_empty() || spot <= 0.0 {
        return json!({ "available": false });
    }
    let mut net = 0.0;
    let mut call = 0.0;
    let mut put = 0.0;
    let mut strikes: Vec<(f64, f64)> = Vec::new();
    for r in chain {
        let cg = r.ce.gamma * r.ce.oi * spot * spot * 0.01;
        let pg = r.pe.gamma * r.pe.oi * spot * spot * 0.01;
        if cg > 0.0 || pg > 0.0 {
            call += cg;
            put += pg;
            let g = cg - pg;
            net += g;
            strikes.push((r.strike, g));
        }
    }
    if strikes.is_empty() {
        return json!({ "available": false });
    }
    strikes.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let mut cum = 0.0;
    let mut flip = 0.0;
    for (s, g) in &strikes {
        let before = cum;
        cum += g;
        if before <= 0.0 && cum > 0.0 && *s > 0.0 {
            flip = *s;
            break;
        }
    }
    json!({
        "available": true,
        "net": round(net, 0),
        "call": round(call, 0),
        "put": round(put, 0),
        "flip": round(flip, 2),
        "regime": if net >= 0.0 { "dampening" } else { "amplifying" },
    })
}

fn global_score(ctx: &Value, commodity: bool, key: &str) -> (f64, Vec<Value>, Vec<String>) {
    let items = ctx.get("global").and_then(|g| g.get("items"));
    let get = |sym: &str| -> Option<f64> {
        items?
            .get(sym)?
            .get("chgPct")?
            .as_f64()
    };
    let mut notes = Vec::new();
    let mut used = Vec::new();
    let mut score = 0.0;
    if commodity {
        let map: &[(&str, f64)] = match key {
            "GOLD" | "GOLDM" => &[("GC=F", 1.0)],
            "SILVER" | "SILVERM" => &[("SI=F", 1.0)],
            "CRUDEOIL" | "CRUDEOILM" => &[("CL=F", 0.8), ("BZ=F", 0.2)],
            "NATURALGAS" | "NATGASMINI" => &[("NG=F", 1.0)],
            "COPPER" => &[("HG=F", 1.0)],
            "ZINC" => &[("HG=F", 0.5)],
            _ => &[("HG=F", 0.3), ("CL=F", 0.3)],
        };
        for (sym, w) in map {
            if let Some(c) = get(sym) {
                score += c * 20.0 * w;
                should_note(&mut notes, format!("{sym} {:+.2}%", c));
                used.push(json!({ "name": sym, "chgPct": round(c, 3), "weight": w }));
            }
        }
        if let Some(fx) = ctx
            .get("usdinr")
            .and_then(|x| x.get("chgPct"))
            .and_then(|x| x.as_f64())
        {
            score += fx * 10.0;
            should_note(&mut notes, format!("USDINR {:+.2}%", fx));
            used.push(json!({ "name": "USDINR", "chgPct": round(fx, 3), "weight": 0.5 }));
        }
        if matches!(key, "GOLD" | "GOLDM" | "SILVER" | "SILVERM" | "COPPER" | "ZINC") {
            if let Some(c) = get("DX-Y.NYB") {
                score -= c * 8.0;
                used.push(json!({ "name": "DXY", "chgPct": round(c, 3), "weight": -0.4 }));
            }
        }
        if matches!(key, "GOLD" | "GOLDM" | "SILVER" | "SILVERM") {
            if let Some(c) = get("^TNX") {
                score -= c * 4.0;
                used.push(json!({ "name": "US10Y", "chgPct": round(c, 3), "weight": -0.2 }));
            }
        }
    } else {
        let map: &[(&str, f64)] = &[
            ("^GSPC", 0.30),
            ("^IXIC", 0.20),
            ("^N225", 0.10),
            ("^HSI", 0.05),
            ("DX-Y.NYB", -0.15),
            ("^TNX", -0.10),
            ("CL=F", -0.05),
            ("USDINR=X", -0.05),
        ];
        for (sym, w) in map {
            if let Some(c) = get(sym) {
                score += c * 25.0 * w;
                used.push(json!({ "name": sym, "chgPct": round(c, 3), "weight": w }));
            }
        }
        if let Some(vix) = get("^INDIAVIX") {
            if vix > 3.0 {
                score -= (vix - 3.0) * 3.0;
                notes.push(format!("India VIX {:+.1}% - risk-off", vix));
            }
            used.push(json!({ "name": "India VIX", "chgPct": round(vix, 3), "weight": -0.2 }));
        }
    }
    (clamp(score, -100.0, 100.0), used, notes)
}

fn flows_score(ctx: &Value) -> (f64, Vec<String>) {
    let f = ctx.get("flows").unwrap_or(&Value::Null);
    if !f.get("available").and_then(|x| x.as_bool()).unwrap_or(false) {
        return (0.0, Vec::new());
    }
    let fii = f.get("fiiNet").and_then(|x| x.as_f64()).unwrap_or(0.0);
    let dii = f.get("diiNet").and_then(|x| x.as_f64()).unwrap_or(0.0);
    let score = clamp((fii * 0.7 + dii * 0.3) / 2500.0 * 50.0, -100.0, 100.0);
    let mut notes = Vec::new();
    notes.push(format!("FII {:+.0} cr / DII {:+.0} cr", fii, dii));
    (score, notes)
}

fn sectors_score(ctx: &Value, key: &str) -> (f64, Vec<String>) {
    let s = ctx.get("sectors").unwrap_or(&Value::Null);
    if !s.get("available").and_then(|x| x.as_bool()).unwrap_or(false) {
        return (0.0, Vec::new());
    }
    let breadth = s.get("breadthPct").and_then(|x| x.as_f64()).unwrap_or(50.0);
    let nifty = s.get("niftyPct").and_then(|x| x.as_f64()).unwrap_or(0.0);
    let sector = match key {
        "BANKNIFTY" => "NIFTY BANK",
        "FINNIFTY" => "NIFTY FIN SERVICE",
        "MIDCPNIFTY" => "NIFTY MIDCAP 100",
        _ => "NIFTY 50",
    };
    let sec_chg = s
        .get("items")
        .and_then(|x| x.get(sector))
        .and_then(|x| x.get("chgPct"))
        .and_then(|x| x.as_f64())
        .unwrap_or(nifty);
    let score = clamp((breadth - 50.0) * 1.5 + sec_chg * 12.0, -100.0, 100.0);
    let mut notes = Vec::new();
    notes.push(format!("breadth {:.0}% · {} {:+.2}%", breadth, sector, sec_chg));
    (score, notes)
}

fn should_note(notes: &mut Vec<String>, s: String) {
    if notes.len() < 6 {
        notes.push(s);
    }
}

fn event_blackout(ctx: &Value, key: &str, commodity: bool, expiry_dte: f64) -> Value {
    let today = today_ist_days();
    let scope = if commodity { "commodity" } else { "index" };
    let mut reasons: Vec<String> = Vec::new();
    if expiry_dte > 0.0 && expiry_dte <= 1.2 {
        reasons.push(format!("expiry day (DTE {:.1})", expiry_dte));
    }
    if let Some(list) = ctx.get("events").and_then(|x| x.as_array()) {
        for e in list {
            let date = e.get("date").and_then(|x| x.as_str()).unwrap_or("");
            let Some(day) = parse_ymd(date) else { continue };
            let win = e.get("windowDays").and_then(|x| x.as_i64()).unwrap_or(0);
            if (day - today).abs() > win {
                continue;
            }
            let es = e.get("scope").and_then(|x| x.as_str()).unwrap_or("all").to_uppercase();
            if es != "ALL" && es != scope.to_uppercase() && es != key.to_uppercase() {
                continue;
            }
            let label = e.get("label").and_then(|x| x.as_str()).unwrap_or("event");
            reasons.push(format!("{} ({})", label, date));
        }
    }
    json!({
        "blackout": !reasons.is_empty() && reasons.iter().any(|r| !r.starts_with("expiry day")),
        "expiryDay": expiry_dte > 0.0 && expiry_dte <= 1.2,
        "reasons": reasons,
    })
}

struct Feasibility {
    distance_atr: f64,
    expected_atr: f64,
    cost_pct: f64,
    option_move_pct: f64,
    net_option_pct: f64,
    net_rr: f64,
    minutes: f64,
    feasible: bool,
}

fn feasibility(
    spot: f64,
    target: f64,
    stop: f64,
    atr_v: f64,
    eff: f64,
    premium: f64,
    lot: f64,
) -> Feasibility {
    let distance = (target - spot).abs();
    let risk = (spot - stop).abs();
    let move_pct = if spot > 0.0 { distance / spot * 100.0 } else { 0.0 };
    let dist_atr = if atr_v > 0.0 { distance / atr_v } else { 0.0 };
    let expected = atr_v * 2.0;
    let exp_atr = if atr_v > 0.0 { expected / atr_v } else { 0.0 };
    let prem = if premium > 0.0 { premium } else { spot * 0.01 };
    let brokerage = if lot > 0.0 && prem > 0.0 {
        40.0 / (prem * lot) * 100.0
    } else {
        0.5
    };
    let exchange = 0.03503 * 2.0;
    let sebi = 0.0001 * 2.0;
    let gst = 0.18 * (brokerage + exchange + sebi);
    let cost_pct = brokerage + 0.0625 + exchange + sebi + 0.003 + gst + 0.10;
    let delta = 0.5;
    let option_move_pct = if prem > 0.0 {
        delta * spot * (move_pct / 100.0) / prem * 100.0
    } else {
        0.0
    };
    let net_option_pct = option_move_pct - cost_pct;
    let cost_pts = cost_pct / 100.0 * prem;
    let reward_pts = delta * distance;
    let risk_pts = delta * risk;
    let net_rr = if risk_pts + cost_pts > 0.0 {
        (reward_pts - cost_pts) / (risk_pts + cost_pts)
    } else {
        0.0
    };
    let minutes = if atr_v > 0.0 {
        (distance / (atr_v * eff.max(0.2))).clamp(1.0, 120.0) * 5.0
    } else {
        0.0
    };
    Feasibility {
        distance_atr: dist_atr,
        expected_atr: exp_atr,
        cost_pct,
        option_move_pct,
        net_option_pct,
        net_rr,
        minutes,
        feasible: dist_atr >= 0.5 && net_rr >= 1.0 && net_option_pct >= 1.0 && minutes <= 480.0,
    }
}

/// Empirical probability from measured walk-forward results.
///
/// The reported confidence is the actual hit-rate of the same target/stop rule
/// on historical bars, shrunk toward 50% with a Beta(1,1) prior so a handful of
/// trades cannot produce a fake 95% (or 5%) number. The structural setup score
/// (`raw`) is used ONLY as a weak prior while there is little evidence; once 30+
/// samples exist the measured hit-rate dominates (>75% weight) and the reported
/// value converges to what the strategy actually delivered. It is deliberately
/// never allowed to exceed the measured hit-rate by more than a small margin
/// once the sample is meaningful, so the UI cannot show an inflated probability.
fn calibrate(raw: f64, trades: i64, hit: f64, regime: &str, blackout: bool) -> Value {
    let n = trades.max(0) as f64;
    // Beta(1,1)-smoothed empirical win probability: (wins+1)/(n+2).
    let wins = (hit / 100.0 * n).clamp(0.0, n);
    let p_emp = if n > 0.0 { (wins + 1.0) / (n + 2.0) } else { 0.5 };
    // Weak structural prior: only spans 0.5..0.75 so it can never by itself
    // manufacture a high-confidence plan.
    let p_prior = (0.5 + (raw - 50.0) / 200.0).clamp(0.2, 0.75);
    // Evidence weight: 0 trades -> prior, 30 -> half, 120 -> 0.8, 300 -> 0.91.
    let w = (n / (n + 30.0)).min(0.95);
    let mut p = p_prior * (1.0 - w) + p_emp * w;
    if regime == "RANGE" {
        p *= 0.92;
    } else if regime == "VOLATILE" {
        p *= 0.95;
    }
    if blackout {
        p *= 0.55;
    }
    let p = clamp(p, 0.02, 0.95);
    // Transparency: when the sample is meaningful, cap the displayed value so
    // it can never sit more than 5 points above the measured hit-rate.
    let capped = if n >= 20.0 {
        (p * 100.0).min(hit + 5.0).max(hit - 15.0)
    } else {
        p * 100.0
    };
    json!({
        "raw": round(raw, 1),
        "probability": round(capped, 1),
        "empirical": round(p_emp * 100.0, 1),
        "prior": round(p_prior * 100.0, 1),
        "history": round(hit, 1),
        "samples": trades,
        "weight": round(w, 2),
        "basis": if n >= 20.0 { "backtest (measured)" } else { "thin evidence (prior+backtest)" },
    })
}

fn atm_premium(chain: &[algo_core::option::ChainRow], spot: f64, dir: i64) -> f64 {
    let Some(r) = chain
        .iter()
        .min_by(|a, b| {
            (a.strike - spot)
                .abs()
                .partial_cmp(&(b.strike - spot).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    else {
        return 0.0;
    };
    let p = if dir < 0 { r.pe.ltp } else { r.ce.ltp };
    if p > 0.0 {
        p
    } else {
        (r.ce.ltp + r.pe.ltp) / 2.0
    }
}

pub struct Enrich<'a> {
    pub key: &'a str,
    pub commodity: bool,
    pub candles: &'a [Candle],
    pub daily: &'a [Candle],
    pub chain: &'a [algo_core::option::ChainRow],
    pub lot: f64,
    pub roll_basis_pct: Option<f64>,
}

pub fn enrich(row: &mut Value, e: &Enrich) {
    let dir = row.get("direction").and_then(|v| v.as_i64()).unwrap_or(0);
    let spot = row.get("spot").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let target = row.get("target").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let stop = row.get("stop").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let atr_v = row.get("atr").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let raw = row.get("confidence").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let expiry_dte = row
        .get("chain")
        .and_then(|c| c.get("dteDays"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let ctx = snapshot();

    let reg = regime(e.candles, spot, atr_v);
    let regime_label = reg.get("label").and_then(|v| v.as_str()).unwrap_or("MIXED");
    let mt = mtf(e.candles, e.daily, dir);
    let (g_score, g_items, g_notes) = global_score(&ctx, e.commodity, e.key);
    let (f_score, f_notes) = if e.commodity { (0.0, Vec::new()) } else { flows_score(&ctx) };
    let (s_score, s_notes) = if e.commodity { (0.0, Vec::new()) } else { sectors_score(&ctx, e.key) };
    let gx = if e.commodity { json!({ "available": false }) } else { gex(e.chain, spot) };
    let events = event_blackout(&ctx, e.key, e.commodity, expiry_dte);
    let blackout = events.get("blackout").and_then(|v| v.as_bool()).unwrap_or(false);
    let prem = atm_premium(e.chain, spot, dir);
    let eff = reg.get("efficiency").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let feas = feasibility(spot, target, stop, atr_v, eff, prem, e.lot);
    let month = civil_from_days((now_ms() / 1000 + IST_OFFSET_SECS) / 86_400).1;
    let season = seasonality(e.daily, month);

    let mut adjustments: Vec<Value> = Vec::new();
    let mut adj = |label: &str, d: f64| {
        if d.abs() > 0.01 {
            adjustments.push(json!({ "label": label, "delta": round(d, 1) }));
        }
    };

    let mut conf = raw;
    let reg_score = reg.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
    if dir != 0 {
        adj("regime", reg_score);
        conf += reg_score;
    }
    let mt_score = mt.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
    if dir != 0 {
        adj("multi-timeframe", mt_score);
        conf += mt_score;
    }
    let g_adj = if dir > 0 { g_score * 0.10 } else if dir < 0 { -g_score * 0.10 } else { 0.0 };
    if dir != 0 {
        adj("global cues", g_adj);
        conf += g_adj;
    }
    let f_adj = if dir > 0 { f_score * 0.06 } else if dir < 0 { -f_score * 0.06 } else { 0.0 };
    if dir != 0 && f_score != 0.0 {
        adj("FII/DII flows", f_adj);
        conf += f_adj;
    }
    let s_adj = if dir > 0 { s_score * 0.05 } else if dir < 0 { -s_score * 0.05 } else { 0.0 };
    if dir != 0 && s_score != 0.0 {
        adj("sector breadth", s_adj);
        conf += s_adj;
    }
    if let Some(basis) = e.roll_basis_pct {
        let b_adj = if dir > 0 { -basis * 6.0 } else { basis * 6.0 };
        adj("roll/basis", clamp(b_adj, -5.0, 5.0));
        conf += clamp(b_adj, -5.0, 5.0);
    }
    let gex_adj = if gx.get("available").and_then(|v| v.as_bool()).unwrap_or(false) {
        let flip = gx.get("flip").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let net = gx.get("net").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let mut a = 0.0;
        if flip > 0.0 && dir != 0 {
            a += if (dir > 0 && spot > flip) || (dir < 0 && spot < flip) { 4.0 } else { -3.0 };
        }
        if net < 0.0 && dir != 0 {
            a += 1.5;
        }
        a
    } else {
        0.0
    };
    if dir != 0 {
        adj("option gamma", gex_adj);
        conf += gex_adj;
    }
    if dir != 0 && !feas.feasible {
        adj("feasibility", -6.0);
        conf -= 6.0;
    }
    if blackout {
        adj("event blackout", -18.0);
        conf -= 18.0;
    }
    let conf = clamp(conf, 0.0, 100.0);
    let bt = row.get("backtest").cloned().unwrap_or_else(|| json!({}));
    let trades = bt.get("trades").and_then(|v| v.as_i64()).unwrap_or(0);
    let hit = bt.get("hitRate").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let cal = calibrate(conf, trades, hit, regime_label, blackout);
    let calibrated = cal.get("probability").and_then(|v| v.as_f64()).unwrap_or(conf);

    let mut warnings: Vec<Value> = row
        .get("warnings")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if regime_label == "RANGE" {
        warnings.push(json!("Range regime - breakout targets fail more often"));
    }
    if regime_label == "VOLATILE" {
        warnings.push(json!("Volatility expanded - wider stops, smaller size"));
    }
    if mt.get("against").and_then(|v| v.as_i64()).unwrap_or(0) >= 2 {
        warnings.push(json!("Higher timeframes oppose the setup"));
    }
    if let Some(flip) = gx.get("flip").and_then(|v| v.as_f64()).filter(|v| *v > 0.0) {
        if dir != 0 && ((dir > 0 && spot < flip) || (dir < 0 && spot > flip)) {
            warnings.push(json!(format!("Price is on the wrong side of gamma flip {:.2}", flip)));
        }
    }
    if !feas.feasible {
        warnings.push(json!(format!(
            "Target feasibility weak: {:.1} ATR, net {:.2} RR, cost {:.2}%",
            feas.distance_atr, feas.net_rr, feas.cost_pct
        )));
    }
    if blackout {
        let reasons = events
            .get("reasons")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str()).collect::<Vec<_>>().join(", "))
            .unwrap_or_default();
        warnings.push(json!(format!("Event blackout: {reasons}")));
    }

    let supports = {
        let mut out: Vec<String> = Vec::new();
        if regime_label == "TREND" {
            out.push(format!("ADX {:.1} with efficiency {:.2} - trending tape", reg.get("adx").and_then(|v| v.as_f64()).unwrap_or(0.0), eff));
        }
        if mt.get("aligned").and_then(|v| v.as_i64()).unwrap_or(0) >= 3 {
            out.push("5m/15m/1h/daily aligned".to_string());
        }
        if dir > 0 && g_score > 5.0 || dir < 0 && g_score < -5.0 {
            out.push(format!("Global cues supportive ({:+.0})", g_score));
        }
        if !e.commodity && dir > 0 && f_score > 5.0 || !e.commodity && dir < 0 && f_score < -5.0 {
            out.push(format!("Flows supportive ({:+.0})", f_score));
        }
        if !e.commodity && dir > 0 && s_score > 5.0 || !e.commodity && dir < 0 && s_score < -5.0 {
            out.push(format!("Sector breadth supportive ({:+.0})", s_score));
        }
        if feas.feasible {
            out.push(format!("Target feasible: {:.1} ATR, net RR {:.2}", feas.distance_atr, feas.net_rr));
        }
        for f in row.get("factors").and_then(|v| v.as_array()).into_iter().flatten().take(3) {
            if let (Some(k), Some(d)) = (
                f.get("kind").and_then(|v| v.as_str()),
                f.get("detail").and_then(|v| v.as_str()),
            ) {
                out.push(format!("{k}: {d}"));
            }
        }
        out.truncate(6);
        out
    };
    let mut risks = Vec::new();
    if regime_label == "RANGE" {
        risks.push("Range regime can fade breakouts".to_string());
    }
    if regime_label == "VOLATILE" {
        risks.push("Expanded volatility can hit the stop first".to_string());
    }
    if blackout {
        risks.push("Event/expiry blackout".to_string());
    }
    if !feas.feasible {
        risks.push("Costs or distance make the target unattractive".to_string());
    }
    if dir > 0 && g_score < -10.0 || dir < 0 && g_score > 10.0 {
        risks.push("Global cues oppose the direction".to_string());
    }
    risks.truncate(6);

    let summary = format!(
        "{} · {} regime · MTF {}/4 · global {:+.0} · calibrated {:.1}% · target {:.2} ({:.1} ATR)",
        if dir > 0 { "BUY CALL" } else if dir < 0 { "BUY PUT" } else { "NO BUY" },
        regime_label,
        mt.get("aligned").and_then(|v| v.as_i64()).unwrap_or(0),
        g_score,
        calibrated,
        target,
        feas.distance_atr
    );

    let research = json!({
        "regime": reg,
        "multiTimeframe": mt,
        "global": {
            "available": ctx.get("global").and_then(|g| g.get("available")).and_then(|v| v.as_bool()).unwrap_or(false),
            "score": round(g_score, 1),
            "items": g_items,
            "notes": g_notes,
            "source": "Yahoo Finance",
            "asOf": ctx.get("at").cloned().unwrap_or(Value::Null),
        },
        "flows": {
            "available": ctx.get("flows").and_then(|f| f.get("available")).and_then(|v| v.as_bool()).unwrap_or(false),
            "score": round(f_score, 1),
            "fiiNet": ctx.get("flows").and_then(|f| f.get("fiiNet")).cloned().unwrap_or(Value::Null),
            "diiNet": ctx.get("flows").and_then(|f| f.get("diiNet")).cloned().unwrap_or(Value::Null),
            "date": ctx.get("flows").and_then(|f| f.get("date")).cloned().unwrap_or(Value::Null),
            "notes": f_notes,
        },
        "sectors": {
            "available": ctx.get("sectors").and_then(|s| s.get("available")).and_then(|v| v.as_bool()).unwrap_or(false),
            "score": round(s_score, 1),
            "breadthPct": ctx.get("sectors").and_then(|s| s.get("breadthPct")).cloned().unwrap_or(Value::Null),
            "niftyPct": ctx.get("sectors").and_then(|s| s.get("niftyPct")).cloned().unwrap_or(Value::Null),
            "leaders": ctx.get("sectors").and_then(|s| s.get("leaders")).cloned().unwrap_or(Value::Null),
            "laggards": ctx.get("sectors").and_then(|s| s.get("laggards")).cloned().unwrap_or(Value::Null),
            "notes": s_notes,
        },
        "gamma": gx,
        "commodity": {
            "rollBasisPct": e.roll_basis_pct.map(|v| round(v, 3)),
            "seasonality": season,
        },
        "feasibility": {
            "distanceAtr": round(feas.distance_atr, 2),
            "expectedAtr": round(feas.expected_atr, 2),
            "costPct": round(feas.cost_pct, 3),
            "optionMovePct": round(feas.option_move_pct, 2),
            "netOptionPct": round(feas.net_option_pct, 2),
            "netRr": round(feas.net_rr, 2),
            "minutes": round(feas.minutes, 0),
            "feasible": feas.feasible,
            "atmPremium": round(prem, 2),
        },
        "events": events,
        "calibration": cal,
        "attribution": adjustments,
        "thesis": { "summary": summary, "supports": supports, "risks": risks },
        "contextAt": ctx.get("at").cloned().unwrap_or(Value::Null),
    });
    row["confidenceRaw"] = json!(round(raw, 1));
    if dir != 0 && target > 0.0 {
        row["confidence"] = json!(round(calibrated, 1));
    }
    row["research"] = research;
    row["warnings"] = Value::Array(warnings);
}

pub async fn daily_cached(st: &DhanState, sid: i64, seg: &str, inst: &str) -> Vec<Candle> {
    static DAILY: OnceLock<Mutex<HashMap<i64, (i64, Vec<Candle>)>>> = OnceLock::new();
    let store = DAILY.get_or_init(|| Mutex::new(HashMap::new()));
    let now = now_ms();
    if let Ok(g) = store.lock() {
        if let Some((at, v)) = g.get(&sid) {
            if now - *at < DAILY_TTL_MS {
                return v.clone();
            }
        }
    }
    match st.fetch_candles(sid, seg, inst, "1day").await {
        Ok(v) if !v.is_empty() => {
            if let Ok(mut g) = store.lock() {
                g.insert(sid, (now, v.clone()));
            }
            v
        }
        _ => store
            .lock()
            .ok()
            .and_then(|g| g.get(&sid).map(|(_, v)| v.clone()))
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(t: i64, c: f64, h: f64, l: f64) -> Candle {
        Candle { time: t, open: c, high: h, low: l, close: c, volume: 1000.0 }
    }

    #[test]
    fn regime_flags_a_clean_trend() {
        let mut cs = Vec::new();
        for i in 0..120 {
            let c = 100.0 + i as f64 * 0.7;
            cs.push(candle(i * 300, c, c + 0.4, c - 0.4));
        }
        let r = regime(&cs, 183.0, atr(&cs, 14));
        assert_eq!(r["label"], "TREND");
        assert!(r["adx"].as_f64().unwrap_or(0.0) > 25.0);
    }

    #[test]
    fn regime_flags_a_range() {
        let mut cs = Vec::new();
        for i in 0..120 {
            let c = 100.0 + if i % 2 == 0 { 0.2 } else { -0.2 };
            cs.push(candle(i * 300, c, c + 0.25, c - 0.25));
        }
        let r = regime(&cs, 100.0, atr(&cs, 14));
        assert_eq!(r["label"], "RANGE");
        assert!(r["chop"].as_f64().unwrap_or(0.0) > 60.0);
    }

    #[test]
    fn calibration_is_monotonic_and_shrinks_on_blackout() {
        let a = calibrate(40.0, 0, 0.0, "MIXED", false);
        let b = calibrate(70.0, 0, 0.0, "MIXED", false);
        let c = calibrate(70.0, 0, 0.0, "MIXED", true);
        assert!(b["probability"].as_f64().unwrap() > a["probability"].as_f64().unwrap());
        assert!(c["probability"].as_f64().unwrap() < b["probability"].as_f64().unwrap());
    }

    #[test]
    fn feasibility_penalises_tiny_targets() {
        let f = feasibility(100.0, 100.2, 99.8, 1.0, 0.4, 2.0, 75.0);
        assert!(!f.feasible);
        let g = feasibility(100.0, 106.0, 99.0, 1.0, 0.4, 2.0, 75.0);
        assert!(g.net_rr > 0.0);
    }

    #[test]
    fn seasonality_needs_samples() {
        let d = vec![candle(0, 100.0, 101.0, 99.0)];
        let s = seasonality(&d, 10);
        assert_eq!(s["available"], false);
    }

    #[test]
    fn event_blackout_matches_scope() {
        let today = today_ist_days();
        let (y, m, d) = civil_from_days(today);
        let date = format!("{y:04}-{m:02}-{d:02}");
        let ctx = json!({ "events": [{ "date": date, "label": "RBI policy", "scope": "index" }] });
        let idx = event_blackout(&ctx, "NIFTY", false, 5.0);
        let com = event_blackout(&ctx, "GOLD", true, 5.0);
        assert_eq!(idx["blackout"], true);
        assert_eq!(com["blackout"], false);
    }
}
