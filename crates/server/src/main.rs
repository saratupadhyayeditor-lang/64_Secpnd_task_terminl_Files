#![recursion_limit = "256"]

use axum::{
    extract::{DefaultBodyLimit, FromRef, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use serde_json::json;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::set_header::SetResponseHeaderLayer;

use algo_core::model::TIMEFRAMES;

mod adapters;
mod autoexp;
mod backup;
mod brain;
mod broker;
mod brokers;
mod diagnostics;
mod equity;
mod equity_engine;
mod instruments;
mod market;
mod marketanalysis;
mod optionchain;
mod order_rate;
mod realtime;
mod research;
mod scrip;
mod shared_margin;
mod stats;
mod target;

use broker::DhanState;
use market::MarketState;
use realtime::{shared_template_pool, RealtimeState};

async fn order_rate_get() -> Json<serde_json::Value> {
    Json(order_rate::status_json())
}

async fn research_context() -> Json<serde_json::Value> {
    Json(research::context_json())
}

async fn research_events_get() -> Json<serde_json::Value> {
    Json(research::events_get())
}

async fn research_events_post(Json(v): Json<serde_json::Value>) -> Json<serde_json::Value> {
    Json(research::events_post(v))
}

async fn order_rate_post(Json(v): Json<serde_json::Value>) -> Json<serde_json::Value> {
    let (mut paper, mut real) = order_rate::caps();
    if let Some(n) = v.get("paper").and_then(|x| x.as_i64()) {
        paper = n;
    }
    if let Some(n) = v.get("real").and_then(|x| x.as_i64()) {
        real = n;
    }
    order_rate::set_caps(paper, real);
    Json(order_rate::status_json())
}

/// Combined router state: the Dhan session/feed plus the realtime trading
/// engine. Handlers keep extracting the narrower `State<DhanState>` /
/// `State<MarketState>` / `State<RealtimeState>` via `FromRef`.
#[derive(Clone)]
pub struct AppState {
    pub dhan: DhanState,
    pub rt: RealtimeState,
}

impl FromRef<AppState> for DhanState {
    fn from_ref(state: &AppState) -> DhanState {
        state.dhan.clone()
    }
}

impl FromRef<AppState> for MarketState {
    fn from_ref(state: &AppState) -> MarketState {
        state.dhan.market.clone()
    }
}

impl FromRef<AppState> for RealtimeState {
    fn from_ref(state: &AppState) -> RealtimeState {
        state.rt.clone()
    }
}

#[derive(Deserialize)]
struct CandleReq {
    #[serde(default)]
    security_id: i64,
    #[serde(default)]
    exchange_segment: String,
    #[serde(default)]
    instrument_type: String,
    #[serde(default)]
    timeframe: String,
}

async fn candles_post(
    State(st): State<DhanState>,
    Json(req): Json<CandleReq>,
) -> impl IntoResponse {
    // Operator-facing request: let the background pollers stand down so the
    // chart gets Dhan's single ~1/s data slot immediately.
    st.mark_user_activity();
    let tf = if req.timeframe.is_empty() {
        "5min"
    } else {
        &req.timeframe
    };
    // Only real Dhan candles are served. Without a live session (or when Dhan
    // returns nothing) the chart gets an empty series instead of synthetic data.
    if req.exchange_segment.is_empty() {
        return Json(json!({
            "ok": false,
            "data": [],
            "error": "not connected to Dhan",
        }));
    }
    match st
        .fetch_candles(
            req.security_id,
            &req.exchange_segment,
            &req.instrument_type,
            tf,
        )
        .await
    {
        Ok(data) if !data.is_empty() => Json(json!({ "ok": true, "data": data })),
        Ok(_) => {
            tracing::warn!(
                "Dhan returned empty candles: sec={} exch={} inst={} tf={}",
                req.security_id,
                req.exchange_segment,
                req.instrument_type,
                tf
            );
            Json(json!({ "ok": false, "data": [], "error": "no candles returned" }))
        }
        Err(e) => {
            tracing::warn!(
                "Dhan candle fetch failed: sec={} exch={} inst={} tf={} err={:?}",
                req.security_id,
                req.exchange_segment,
                req.instrument_type,
                tf,
                e
            );
            Json(json!({ "ok": false, "data": [], "error": e.to_string() }))
        }
    }
}

async fn candles_get(
    State(st): State<DhanState>,
    Query(req): Query<CandleReq>,
) -> impl IntoResponse {
    candles_post(State(st), Json(req)).await
}

async fn indicators() -> impl IntoResponse {
    Json(algo_core::indicators::catalog_json())
}

async fn timeframes() -> impl IntoResponse {
    let list: Vec<_> = TIMEFRAMES
        .iter()
        .map(|t| json!({ "key": t.key, "label": t.label }))
        .collect();
    Json(list)
}

async fn health() -> impl IntoResponse {
    Json(json!({ "ok": true }))
}

async fn autoexp_status() -> impl IntoResponse {
    Json(autoexp::status_json())
}

async fn autoexp_start(
    State(st): State<DhanState>,
    Json(v): Json<serde_json::Value>,
) -> impl IntoResponse {
    Json(autoexp::start(st, &v))
}

async fn autoexp_stop() -> impl IntoResponse {
    Json(autoexp::stop())
}

async fn autoexp_clear() -> impl IntoResponse {
    Json(autoexp::clear())
}

async fn autoexp_save(Json(v): Json<serde_json::Value>) -> impl IntoResponse {
    Json(autoexp::save(&v))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_target(false).init();

    // Resolve the UI directory at runtime so an installed/packaged build finds
    // its assets next to the executable (or in the current working directory),
    // and only falls back to the source-tree path during development.
    let static_dir: PathBuf = {
        let mut candidates: Vec<PathBuf> = Vec::new();
        if let Ok(exe) = std::env::current_exe() {
            if let Some(dir) = exe.parent() {
                candidates.push(dir.join("static"));
            }
        }
        candidates.push(PathBuf::from("static"));
        candidates.push(PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/static")));
        candidates
            .into_iter()
            .find(|p| p.join("index.html").is_file())
            .unwrap_or_else(|| PathBuf::from("static"))
    };
    let index = ServeFile::new(Path::new(&static_dir).join("index.html"));

    let market = MarketState::new();
    let dhan = DhanState::new(market.clone());
    dhan.clone().spawn_positions_poller();
    dhan.spawn_watchdog();
    dhan.spawn_api_reconnect();
    dhan.spawn_gift_nifty_fix();
    target::spawn_refresher(dhan.clone());
    research::spawn_refresher();
    equity::spawn_refresher(dhan.clone());
    equity_engine::spawn_engine(dhan.clone());
    scrip::spawn_warm();
    let template_pool = shared_template_pool();
    autoexp::init(template_pool.clone());
    autoexp::boot();
    let rt = RealtimeState::new(dhan.clone()).with_template_pool(template_pool.clone());
    let realtime2_rt = RealtimeState::new_realtime2(dhan.clone()).with_template_pool(template_pool.clone());
    let realtime3_rt = RealtimeState::new_realtime3(dhan.clone()).with_template_pool(template_pool.clone());
    let paper_rt = RealtimeState::new_paper(dhan.clone()).with_template_pool(template_pool.clone());
    let paper2_rt = RealtimeState::new_paper2(dhan.clone()).with_template_pool(template_pool.clone());
    let paper3_rt = RealtimeState::new_paper3(dhan.clone()).with_template_pool(template_pool.clone());
    let paper4_rt = RealtimeState::new_paper4(dhan.clone()).with_template_pool(template_pool.clone());
    let autoexp_rt = RealtimeState::new_autoexp(dhan.clone()).with_template_pool(template_pool.clone());
    let aitrader_rt = RealtimeState::new_aitrader(dhan.clone()).with_template_pool(template_pool.clone());
    let aitraderlive_rt = RealtimeState::new_aitraderlive(dhan.clone()).with_template_pool(template_pool.clone());
    // Startup self-check + the auto-capture driver. Both only READ state and
    // write local files, so they can never interfere with trading. The capture
    // task is what makes a bug on an installed Windows machine debuggable: the
    // frozen moment (blockers + margin + feed + log tail) is written to disk by
    // the app itself, without the operator having to press anything.
    diagnostics::startup_self_check(&dhan, &rt);
    {
        // Once the scrip master is authoritative, purge saved commodity ids that
        // are not live MCX contracts. The master downloads/parses in the
        // background (~25-30s on a fresh install), so this waits for it instead
        // of running against an empty view. Every engine tab is repaired; each
        // call is a no-op that writes nothing when the saved set is already
        // valid (so a normal install never rewrites its state files).
        let engines = vec![
            rt.clone(),
            realtime2_rt.clone(),
            realtime3_rt.clone(),
            paper_rt.clone(),
            paper2_rt.clone(),
            paper3_rt.clone(),
            paper4_rt.clone(),
            autoexp_rt.clone(),
            aitrader_rt.clone(),
            aitraderlive_rt.clone(),
        ];
        tokio::spawn(async move {
            let mut waited = 0u64;
            while !crate::scrip::is_ready() && waited < 180 {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                waited += 1;
            }
            if !crate::scrip::is_ready() {
                return;
            }
            for e in engines {
                e.reconcile_commodity_contracts();
            }
        });
    }
    {
        let dhan = dhan.clone();
        let rt = rt.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(90)).await;
            loop {
                diagnostics::maybe_auto_capture(&dhan, &rt).await;
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        });
    }

    let state = AppState { dhan, rt };

    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/experiment/status", get(autoexp_status))
        .route("/api/experiment/start", post(autoexp_start))
        .route("/api/experiment/stop", post(autoexp_stop))
        .route("/api/experiment/clear", post(autoexp_clear))
        .route("/api/experiment/save", post(autoexp_save))
        .route("/api/indicators", get(indicators))
        .route("/api/timeframes", get(timeframes))
        .route("/api/candles", get(candles_get).post(candles_post))
        .route("/api/symbols", get(market::symbols))
        .route("/api/watchlists", get(market::symbols))
        .route("/api/commodities", get(market::commodities))
        .route("/api/liquid", get(market::liquid))
        .route("/api/market/analysis", get(marketanalysis::analysis))
        .route("/api/market/gap-stats", get(marketanalysis::gap_stats))
        .route("/api/quotes", post(market::quotes_post))
        .route("/api/expiries", post(optionchain::expiries))
        .route("/api/option_chain", post(optionchain::option_chain))
        .route("/api/option_chain_all", post(optionchain::option_chain_all))
        .route("/api/auto_strikes", post(optionchain::auto_strikes))
        .route("/api/option_security", post(optionchain::option_security))
        .route("/api/oc/subscribe", post(optionchain::oc_subscribe))
        .route("/api/manual_strikes/chain", post(optionchain::manual_strike_chain))
        .route("/api/instruments", get(market::instruments))
        .route("/api/lot_sizes", get(optionchain::lot_sizes))
        .route("/ws", get(market::ws_handler))
        .route("/api/brokers", get(brokers::catalog_get).post(brokers::account_post))
        .route("/api/brokers/test", post(brokers::test_post))
        .route("/api/connect", post(broker::connect))
        .route("/api/status", get(broker::status))
        .route("/api/feed/status", get(broker::feed_status))
        .route("/api/feed/reset", post(broker::feed_reset))
        .route("/api/feed/restart", post(broker::feed_restart))
        .route("/api/account", get(realtime::account_overview))
        .route("/api/order-rate", get(order_rate_get).post(order_rate_post))
        .route("/api/research/context", get(research_context))
        .route("/api/research/events", get(research_events_get).post(research_events_post))
        .route("/api/equity/status", get(equity::status))
        .route("/api/equity/cache", get(equity::cache_json))
        .route("/api/equity/refresh", post(equity::refresh))
        .route("/api/equity/filters", post(equity::filters_set))
        .route("/api/equity/push-reset", post(equity::push_reset))
        .route("/api/equity/stock", get(equity::stock))
        .route("/api/equity/engines", get(equity_engine::engines))
        .route("/api/equity/engines/trades", get(equity_engine::trades))
        .route("/api/equity/engines/toggle", post(equity_engine::toggle))
        .route("/api/equity/engines/settings", post(equity_engine::settings))
        .route("/api/equity/engines/exit", post(equity_engine::exit_trade))
        .route("/api/equity/engines/enter", post(equity_engine::enter_trade))
        .route("/api/equity/engines/reset-pnl", post(equity_engine::reset_pnl))
        .route("/api/equity/engines/clear", post(equity_engine::clear_book))
        .route("/api/margin/shared", get(shared_margin::get))
        .route("/api/margin/shared/total", post(shared_margin::set_total))
        .route("/api/margin/shared/allot", post(shared_margin::set_allot))
        .route("/api/target/analyze", post(target::analyze))
        .route("/api/target/cache", get(target::cache_handler))
        .route("/api/target/refresh", post(target::refresh))
        .route("/api/backup/config", get(backup::config_get).post(backup::config_post))
        .route("/api/backup/path_test", post(backup::path_test_handler))
        .route("/api/backup/snapshot", post(backup::snapshot))
        .route("/api/backup/list", get(backup::list))
        .route("/api/backup/read", get(backup::read))
        .route("/api/backup/import", post(backup::import))
        .route("/api/diagnostics/snapshot", get(diagnostics::get))
        .route("/api/diagnostics/capture", post(diagnostics::capture))
        .route("/api/diagnostics/captures", get(diagnostics::captures))
        .route("/api/diagnostics/bundle", get(diagnostics::bundle))
        .route("/api/diagnostics/bundle-file", post(diagnostics::bundle_file))
        .merge(realtime::router::<AppState>())
        .with_state(state)
        .merge(realtime::realtime2_router().with_state(realtime2_rt))
        .merge(realtime::realtime3_router().with_state(realtime3_rt))
        .merge(realtime::paper_router().with_state(paper_rt))
        .merge(realtime::paper2_router().with_state(paper2_rt))
        .merge(realtime::paper3_router().with_state(paper3_rt))
        .merge(realtime::paper4_router().with_state(paper4_rt))
        .merge(realtime::autoexp_router().with_state(autoexp_rt))
        .merge(realtime::aitrader_router().with_state(aitrader_rt))
        .merge(realtime::aitraderlive_router().with_state(aitraderlive_rt))
        .fallback_service(
            ServeDir::new(static_dir)
                .precompressed_gzip()
                .fallback(index),
        )
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("no-cache"),
        ))
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        // Backups (localStorage + two full engine states incl. the whole closed
        // ledger) routinely exceed axum's 2 MB default body limit. Allow large
        // restore uploads on every route instead of failing with HTTP 413.
        .layer(DefaultBodyLimit::max(256 * 1024 * 1024));

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("new Rust algo app listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
    let _ = StatusCode::OK;
}
