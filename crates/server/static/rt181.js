// Realtime Trading Engine tab (Rust engine, `/api/rt/*`).
//
// This mirrors the old Python app's "Realtime Trading Engine" tab: an
// AI Smart Trading header with a live P&L summary, an ORDER PLACEMENT METHOD
// block (Normal / Super / Forever-GTT / Slice-Iceberg name-only checkboxes; the
// selected method plus all of its order options live in the single Risk &
// Quantity row), the universal + AI-risk engine controls, and the
// Running / Closed trade tables.
//
// Every decision (signals, quantity, trail, arm gating, order routing) lives in
// the Rust engine. Nothing here talks to Dhan directly. The "AI Smart Trading"
// button is the master switch: ON starts + arms + force-ticks, OFF stops.

import { istTime } from "./ist.js?v=1";
import { mountSharedMargin, realtimeEngineId } from "./sharedmargin.js?v=8";

const API = {
  snapshot: () => get("/api/rt/snapshot"),
  closed: () => get("/api/rt/closed"),
  pool: (force) => get("/api/rt/pool" + (force ? "?refresh=1" : "")),
  movers: () => get("/api/rt/movers"),
  liquid: () => get("/api/rt/liquid"),
  trend: () => get("/api/rt/trend"),
  niftyPane: () => get("/api/rt/nifty-pane"),
  commodities: () => get("/api/rt/commodities"),
  templates: () => get("/api/rt/templates"),
  template: (v) => post("/api/rt/templates", v),
  selection: () => get("/api/rt/selection"),
  selectionSet: (v) => post("/api/rt/selection", v),
  staging: () => get("/api/rt/staging"),
  stagingSet: (v) => post("/api/rt/staging", v),
  entryTiming: () => get("/api/rt/entry-timing"),
  entryTimingSet: (v) => post("/api/rt/entry-timing", v),
  final: () => get("/api/rt/final"),
  finalSet: (v) => post("/api/rt/final", v),
  container: () => get("/api/rt/container"),
  stats: (range) => get("/api/rt/stats?range=" + encodeURIComponent(range || "today")),
  settings: (v) => post("/api/rt/settings", v),
  method: (v) => post("/api/rt/method", v),
  strategySave: (v) => post("/api/rt/strategies", v),
  strategyDelete: (v) => post("/api/rt/strategy/delete", v),
  engine: (v) => post("/api/rt/engine", v),
  tick: () => post("/api/rt/tick", {}),
  autoLots: (v) => post("/api/rt/autolots", v),
  brokers: () => get("/api/rt/brokers"),
  brokersSet: (v) => post("/api/rt/brokers", v),
  entry: (v) => post("/api/rt/entry", v),
  close: (v) => post("/api/rt/close", v),
  squareOff: () => post("/api/rt/square_off", {}),
  reset: (v) => post("/api/rt/reset", v),
  freeze: (sid, underlying) =>
    get(`/api/rt/freeze_qty?securityId=${encodeURIComponent(sid)}&underlying=${encodeURIComponent(underlying || "")}`),
  account: () => get("/api/rt/account"),
  logs: () => get("/api/rt/logs"),
  blockers: () => get("/api/rt/blockers"),
  blockersClear: () => post("/api/rt/blockers", { action: "clear" }),
  // Live diagnostics are shared across every engine tab (one process-wide
  // report), so these deliberately do NOT go through the `/api/rt` -> engine
  // rewrite that the paper frames install.
  diagSnapshot: () => get("/api/diagnostics/snapshot"),
  diagCapture: () => post("/api/diagnostics/capture", { reason: "manual" }),
  diagBundleFile: () => post("/api/diagnostics/bundle-file", {}),
};

// Paper Trade runs this exact module inside an isolated frame that pre-sets
// `window.__PAPER__`; only the wording of the operator-facing confirmations and
// status pills changes, so a paper user is never told real orders will fire.
const PAPER = (() => {
  try { return !!window.__PAPER__; } catch (_) { return false; }
})();

// Paper Trade 3 is a stripped paper engine: it trades only the operator-run
// strategies + Top Movers stocks. `__PAPER3__` lets this shared module remove
// the Index / Commodity controls that engine will never act on.
const PAPER3 = (() => {
  try { return !!window.__PAPER3__; } catch (_) { return false; }
})();

// Paper Trade 2 is an indices-only book and Paper Trade 3 is a commodities-only
// book. Both still use `__PAPER2__` for the shared "no Top Movers stocks / no
// NIFTY trend / no F&O routing" stripping, while these two flags select which of
// the two instrument sections stays visible.
const INDICES_ONLY = (() => {
  try { return !!window.__INDICES_ONLY__; } catch (_) { return false; }
})();
const COMMODITIES_ONLY = (() => {
  try { return !!window.__COMMODITIES_ONLY__; } catch (_) { return false; }
})();

// Equity Indicator Based Trade (Paper Trade 4) is a cash-equity-only book: it
// trades liquid NSE stocks on BULLISH indicator filters and has no option
// machinery at all. `__EQUITY_ONLY__` makes this shared module hide every
// option-specific control (strike row, option type, Execute Trade In, strike
// count, +green/fallback/fastest-rising, manual strike select, premium chart,
// premium chart only, run-in/trade-in and Picked Strikes) and the bearish
// filter column, and show the liquid cash-equity universe instead.
const EQUITY_ONLY = (() => {
  try { return !!window.__EQUITY_ONLY__; } catch (_) { return false; }
})();

// Paper Trade 2 is an indices + option-chain commodities only book: the shared
// module hides the Top Movers stock controls, the NIFTY Trend Following section,
// the F&O-stock routing selects and the NIFTY BB%b (Pane) for this frame. The
// backend `/api/paper2/*` engine enforces the same at its layer.
const PAPER2 = (() => {
  try { return !!window.__PAPER2__; } catch (_) { return false; }
})();

// Auto Experiment is an independent Paper Trade clone, but the "Auto Trading
// (trend + SL + trail + target)" preset is removed from that tab only. This flag
// hides the control in the shared pane so no other engine is affected.
const AUTO_TRADE_REMOVED = (() => {
  try { return !!window.__AUTO_TRADE_REMOVED__; } catch (_) { return false; }
})();

// AI Trader tabs (paper + live) are autonomous: the brain is always in charge,
// so the "Auto Trading" preset toggle is hidden (the engine pins it ON in the
// backend). The pane still shows the brain readout (regime, ranked filters,
// stop/trail/target) so the operator can see exactly what the AI is doing.
const AI_TRADER = (() => {
  try { return !!window.__AITRADER__; } catch (_) { return false; }
})();

async function get(url) {
  const r = await fetch(url);
  return r.json();
}
async function post(url, body) {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  return r.json();
}

// Same method catalogue + per-method options as the old realtimeorders.js.
const METHODS = [
  {
    key: "normal", name: "Normal Order", sdk: "place_order",
    desc: "Ek single entry order (MARKET / LIMIT / SL / SL-M).",
    extras: [
      { k: "orderType", label: "Order type", type: "select", opts: [["MARKET", "MARKET"], ["LIMIT", "LIMIT"], ["SL", "SL"], ["SL-M", "SL-M"]], def: "MARKET" },
    ],
  },
  {
    key: "super", name: "Super Order", sdk: "place_super_order",
    desc: "Entry + broker-side target + stop-loss + trailing (LIMIT / MARKET).",
    extras: [
      { k: "orderType", label: "Entry type", type: "select", opts: [["MARKET", "MARKET"], ["LIMIT", "LIMIT"]], def: "MARKET" },
      { k: "autoSlice", label: "Auto Order Slicing", type: "check", def: false },
      { k: "sliceQty", label: "Slice qty (0=freeze)", type: "number", def: 0, cls: "rtom-slice-qty" },
    ],
  },
  {
    key: "forever", name: "Forever / GTT", sdk: "place_forever",
    desc: "Good-till-triggered order, single ya OCO leg.",
    extras: [
      { k: "orderType", label: "Order type", type: "select", opts: [["LIMIT", "LIMIT"], ["MARKET", "MARKET"]], def: "LIMIT" },
      { k: "flag", label: "Leg flag", type: "select", opts: [["SINGLE", "SINGLE"], ["OCO", "OCO"]], def: "SINGLE" },
      { k: "validity", label: "Validity", type: "select", opts: [["DAY", "DAY"], ["IOC", "IOC"]], def: "DAY" },
      { k: "triggerPrice", label: "Trigger px (0=auto)", type: "number", def: 0 },
    ],
  },
  {
    key: "slice", name: "Slice / Iceberg", sdk: "place_slice_order",
    desc: "Badi qty ko slices me todkar bhejta hai; disclosed qty dikhai deti hai.",
    extras: [
      { k: "orderType", label: "Order type", type: "select", opts: [["MARKET", "MARKET"], ["LIMIT", "LIMIT"]], def: "MARKET" },
      { k: "disclosedQty", label: "Disclosed qty", type: "number", def: 0 },
    ],
  },
];

let STATE = null;
// Guards against the 1s snapshot poll clobbering UI the user just touched.
// `refreshSeq` drops out-of-order snapshot responses; `filtersBusy` freezes the
// Bullish/Bearish checkbox row while its save round-trips, so a snapshot that
// was fetched before the click can never revert the tick.
let refreshSeq = 0;
// A snapshot response can be large and the poll is only 1s. Without this guard a
// slow/tunneled link lets a new poll start before the previous response lands, so
// every response looks "stale" and is dropped -> the whole pane freezes. Keep one
// snapshot in flight at a time; a request made while one is running is queued and
// run immediately after, so a button-driven refresh never gets lost.
let refreshInFlight = false;
let refreshQueued = false;
let filtersBusy = false;
// The strike-selection base ("Only +green premium strikes" + "Opposite side
// fallback") must stay ON because the "Execute Trade In" dropdown is designed to
// run on it. If a legacy snapshot arrives with either off, it is turned back on
// (with a one-time notice) instead of silently shipping a mismatched config.
let strikeBaseNormalized = false;
// The per-second snapshot only carries the newest slice of the closed ledger;
// the full (uncapped) ledger is paged in on demand via `/closed` and cached here.
let CLOSED_CACHE = null;
let closedLoading = false;
let closedLoadedAt = 0;
// "Show all": the Closed Trades wrapper clips the (already fully rendered) ledger
// at 230px. Toggling it lifts the cap so the whole closed book is visible at once.
// Preference persists across reloads; localStorage may be unavailable in a
// sandboxed frame, so every access is guarded.
function readClosedExpanded() {
  try { return localStorage.getItem("rtClosedExpand") === "1"; } catch (e) { return false; }
}
let closedExpanded = readClosedExpanded();
function applyClosedExpand() {
  const wrap = document.getElementById("rtClosedWrap");
  const btn = document.getElementById("rtClosedExpand");
  if (wrap) wrap.classList.toggle("rt-scroll-open", closedExpanded);
  if (btn) btn.textContent = closedExpanded ? "Collapse" : "Show all";
}
// The 1s poll rebuilds the whole DOM for the closed ledger (thousands of rows)
// and the condition log; doing that every second is what made the pane hang.
// Keep an append-only tail for the closed table so a fresh close (which only ever
// prepends, rows are newest-first) inserts just its row instead of rebuilding the
// whole innerHTML; the condition log keeps its own repaint signature below.
let closedRenderedCount = 0;
let closedRenderedSource = "";
let closedRenderedCharges = null;
let closedTailId = "";
let logRenderSig = "";
// Day-scoped P&L summary for the engine panes: the summary strip shows TODAY
// only, while the full history lives in the Trade Stats tabs. Loaded on a
// throttle (and whenever a trade closes), never on the 1s poll.
let DAY_STATS = null;
let dayStatsAt = 0;
let dayStatsSig = "";
let dayStatsLoading = false;
let scannerPollAt = 0;
const CLOSED_SNAPSHOT_SLICE = 100;
let CATALOG = { indicators: [], timeframes: [], symbols: [] };
let activeMethod = "super";
let methodInit = false;
// Per-method "Est. premium" auto-fill flag: stays true (live chart LTP) until the
// user types a value, then respects the manual entry for that method.
let priceAuto = {};
let marginReqTimer = null;
// Lot size resolved from Dhan scrip-master for the active chart instrument.
let chartLot = 0;
let timer = null;
let booted = false;
// Old-app AST selection + entry timing diagnostics.
let AST = { selected: {}, entryTiming: [] };
let astBusy = false;
let commodityPopulated = false;
let COMM_NAMES = {};
// Liquid F&O scanner: the dropdown universe (index-membership basket) and the
// "already populated" guards for the three pickers.
let LIQUID_STOCKS = [];
let liquidPopulated = false;
let liquidNames = {};
let astSelectsPopulated = false;

// Registry ids of the straight-line family. Only these may drive the NIFTY
// Trend Following "Confirm indicators" picker: a rising line assigns to the Top
// Gainer (CE) leg, a falling line to the Top Loser (PE) leg.
const STRAIGHT_LINE_IDS = [
  "slconsensus", "ovlconsensus", "autotrend", "projline", "zzline",
  "trendmaster", "panemaster", "srema", "slema", "stzigzag", "supline", "resline",
  "pitchfork", "fibfan", "gannfan", "supplydemand", "wavefib",
  "pastruct", "vl", "bbpctsl", "padiv", "padivo", "slallconsensus", "slallconsensuso",
];

// Extra NIFTY trend-following "Confirm indicators" choices that are not registry
// ids: the ZigZag EMA length variants (resolved server-side with Length N,
// Source Close, Swing 17, Min leg 5). They render with clear names in the
// dropdown and in the added-indicator chips.
const EXTRA_CONF_INDICATORS = [
  ["slema1", "ZigZag EMA 1"], ["slema9", "ZigZag EMA 9"], ["slema21", "ZigZag EMA 21"],
  ["slema30", "ZigZag EMA 30"], ["slema50", "ZigZag EMA 50"], ["slema100", "ZigZag EMA 100"],
];

// Paper Trade 3 drives its NIFTY Trend Follow engine with the BB%b indicator
// family only. The confirm picker there offers exactly these registry ids; the
// server reads each one's VISIBLE drawn line (green/rising = bullish, red/
// falling = bearish) exactly like the other straight-line confirms.
const BB_PCT_CONF_INDICATORS = [
  ["bbpct", "BB%b"],
  ["bbpctsl", "BB%b Straight Line"],
  ["bbpctslp", "BB%b Straightline Pane"],
  ["bollingerB", "Bollinger Bands %B"],
];

// Full indicator-filter catalogue from the old AST engine (Bullish / Bearish
// sections). Keys are the old id minus the "astFilter" prefix, so "BullEmaTrend9"
// maps to `astFilterBullEmaTrend9` in the Python app. Every toggle is persisted
// to the Rust engine's `settings.filters` map.
const FILTER_BULL_LEGACY = [
  ["IncUp", "Detect bullish trend"],
  ["GapUp", "Gap increasing"],
  ["IncUpAll", "Detect bullish trend (all)"],
  ["CrossUp", "Crossed above"],
  ["GtUp", "Greater than"],
  ["LtUp", "Less than"],
  ["PaneCrossUp", "Pane crossover"],
  ["PaneIncUpAll", "Detect bullish trend (pane lines, all)"],
  ["BullBbwInc", "BBW increasing"],
  ["BullSmf", "Smart Money Flow bullish (SMF above signal)"],
  ["BullAsr", "Support gap widening (price rising away from support)"],
  ["BullOit", "Detect bullish trend (OI Trend)"],
  ["BullBbCrossAbove", "Close (EMA1) crossed above BB middle band (upper+lower expanding, overall trend vote: BB middle)"],
  ["BullPcCrossAbove", "Close (EMA1) crossed above price channel middle line (overall trend vote: PC middle)"],
  ["BullEma9_21", "EMA 9 crossed above EMA 21 (overall trend vote: EMA 21)"],
  ["BullEma21_35", "EMA 21 crossed above EMA 35 (overall trend vote: EMA 35)"],
  ["BullEma35_50", "EMA 35 crossed above EMA 50 (overall trend vote: EMA 50)"],
  ["BullEma50_100", "EMA 50 crossed above EMA 100 (overall trend vote: EMA 100)"],
  ["BullEma100_200", "EMA 100 crossed above EMA 200 (overall trend vote: EMA 200)"],
  ["BullEma200_300", "EMA 200 crossed above EMA 300 (overall trend vote: EMA 300)"],
  ["BullEma1_9", "EMA 1 crossed above EMA 9 (overall trend vote: EMA 9)"],
  ["BullEma1_21", "EMA 1 crossed above EMA 21 (overall trend vote: EMA 21)"],
  ["BullEma1_35", "EMA 1 crossed above EMA 35 (overall trend vote: EMA 35)"],
  ["BullEma1_50", "EMA 1 crossed above EMA 50 (overall trend vote: EMA 50)"],
  ["BullEma1_100", "EMA 1 crossed above EMA 100 (overall trend vote: EMA 100)"],
  ["BullEma1_200", "EMA 1 crossed above EMA 200 (overall trend vote: EMA 200)"],
  ["BullEma1_300", "EMA 1 crossed above EMA 300 (overall trend vote: EMA 300)"],
  ["BullEmaTrend9", "EMA 9 bullish (line rising)"],
  ["BullEmaTrend21", "EMA 21 bullish (line rising)"],
  ["BullEmaTrend35", "EMA 35 bullish (line rising)"],
  ["BullEmaTrend50", "EMA 50 bullish (line rising)"],
  ["BullEmaTrend100", "EMA 100 bullish (line rising)"],
  ["BullEmaTrend200", "EMA 200 bullish (line rising)"],
  ["BullEmaTrend300", "EMA 300 bullish (line rising)"],
  ["BullSt10_1_2", "Supertrend(10,1) crossed above Supertrend(10,2) (overall trend vote: Supertrend(10,2))"],
  ["BullSt10_2_3", "Supertrend(10,2) crossed above Supertrend(10,3) (overall trend vote: Supertrend(10,3))"],
  ["BullSt1CloseCrossAbove", "Close (EMA1) crossed above ST (overall trend vote: Supertrend(10,1))"],
  ["BullVwapCloseCrossAbove", "Close (EMA1) crossed above VWAP (overall trend vote: VWAP)"],
  ["BullPbrAo", "Detect bullish trend (AO line)"],
  ["BullPbrSmiio", "Detect bullish trend (SMI line)"],
  ["BullPbrDpo", "Detect bullish trend (DPO line)"],
  ["BullPbrMfi", "Detect bullish trend (MFI line)"],
  ["BullPbrUo", "Detect bullish trend (UO line)"],
  ["BullPbrWilliamsR", "Detect bullish trend (Williams %R line)"],
  ["BullPbrBbpct", "Detect bullish trend (BB%b line)"],
  ["BullPbrPvt", "Detect bullish trend (PVT line)"],
  ["BullPbrAd", "Detect bullish trend (A/D line)"],
  ["BullPbrSmf", "Detect bullish trend (SMF line)"],
  ["BullPbrBbwUp", "BBW line direction upward (bands expanding)"],
  ["BullPbrAtrUp", "ATR line direction upward (wide candles)"],
  ["BullPbrVoloscUp", "VolOsc line direction upward (volume picking up)"],
  ["BullMeetEma9_21", "EMA 9 above EMA 21 (level, overall trend vote: EMA 21)"],
  ["BullMeetEma21_35", "EMA 21 above EMA 35 (level, overall trend vote: EMA 35)"],
  ["BullMeetEma35_50", "EMA 35 above EMA 50 (level, overall trend vote: EMA 50)"],
  ["BullMeetEma50_100", "EMA 50 above EMA 100 (level, overall trend vote: EMA 100)"],
  ["BullMeetEma100_200", "EMA 100 above EMA 200 (level, overall trend vote: EMA 200)"],
  ["BullMeetEma200_300", "EMA 200 above EMA 300 (level, overall trend vote: EMA 300)"],
  ["BullMeetSt10_1_2", "Supertrend(10,1) above Supertrend(10,2) (level, overall trend vote: Supertrend(10,2))"],
  ["BullMeetSt10_2_3", "Supertrend(10,2) above Supertrend(10,3) (level, overall trend vote: Supertrend(10,3))"],
  ["BullMeetCloseSt", "Close (EMA1) above Supertrend (level, overall trend vote: Supertrend(10,1))"],
  ["BullMeetCloseVwap", "Close (EMA1) above VWAP (level, overall trend vote: VWAP)"],
  ["BullMeetPaneCross", "Pane crossed line pair still above (level)"],
  ["BullMeetCross", "Crossed-above indicator pair still above (level)"],
  ["BullMeetCloseBb", "Close (EMA1) above BB middle (level, overall trend vote: BB middle)"],
  ["BullMeetClosePc", "Close (EMA1) above PC middle line (level, overall trend vote: PC middle)"],
  ["BullCandle", "Candlestick patterns"],
  ["BullElliott", "Elliott Wave"],
  ["BullIndicator", "Indicators"],
  ["BullPane", "Pane indicators"],
  ["BullSymmetry", "Symmetry"],
  ["BullStructure", "Chart structure"],
  ["BullAtr", "ATR / Volatility"],
];

const FILTER_BEAR_LEGACY = [
  ["IncDown", "Detect bearish trend"],
  ["GapDown", "Gap decreasing"],
  ["IncDownAll", "Detect bearish trend (all)"],
  ["CrossDown", "Crossed below"],
  ["GtDown", "Greater than"],
  ["LtDown", "Less than"],
  ["PaneCrossDown", "Pane crossover"],
  ["PaneIncDownAll", "Detect bearish trend (pane lines, all)"],
  ["BearBbwInc", "BBW increasing"],
  ["BearSmf", "Smart Money Flow bearish (SMF below signal)"],
  ["BearAsr", "Resistance gap widening (price falling away below resistance)"],
  ["BearOit", "Detect bearish trend (OI Trend)"],
  ["BearBbCrossBelow", "Close (EMA1) crossed below BB middle band (upper+lower expanding, overall trend vote: BB middle)"],
  ["BearPcCrossBelow", "Close (EMA1) crossed below price channel middle line (overall trend vote: PC middle)"],
  ["BearEma9_21", "EMA 9 crossed below EMA 21 (overall trend vote: EMA 21)"],
  ["BearEma21_35", "EMA 21 crossed below EMA 35 (overall trend vote: EMA 35)"],
  ["BearEma35_50", "EMA 35 crossed below EMA 50 (overall trend vote: EMA 50)"],
  ["BearEma50_100", "EMA 50 crossed below EMA 100 (overall trend vote: EMA 100)"],
  ["BearEma100_200", "EMA 100 crossed below EMA 200 (overall trend vote: EMA 200)"],
  ["BearEma200_300", "EMA 200 crossed below EMA 300 (overall trend vote: EMA 300)"],
  ["BearEma1_9", "EMA 1 crossed below EMA 9 (overall trend vote: EMA 9)"],
  ["BearEma1_21", "EMA 1 crossed below EMA 21 (overall trend vote: EMA 21)"],
  ["BearEma1_35", "EMA 1 crossed below EMA 35 (overall trend vote: EMA 35)"],
  ["BearEma1_50", "EMA 1 crossed below EMA 50 (overall trend vote: EMA 50)"],
  ["BearEma1_100", "EMA 1 crossed below EMA 100 (overall trend vote: EMA 100)"],
  ["BearEma1_200", "EMA 1 crossed below EMA 200 (overall trend vote: EMA 200)"],
  ["BearEma1_300", "EMA 1 crossed below EMA 300 (overall trend vote: EMA 300)"],
  ["BearEmaTrend9", "EMA 9 bearish (line falling)"],
  ["BearEmaTrend21", "EMA 21 bearish (line falling)"],
  ["BearEmaTrend35", "EMA 35 bearish (line falling)"],
  ["BearEmaTrend50", "EMA 50 bearish (line falling)"],
  ["BearEmaTrend100", "EMA 100 bearish (line falling)"],
  ["BearEmaTrend200", "EMA 200 bearish (line falling)"],
  ["BearEmaTrend300", "EMA 300 bearish (line falling)"],
  ["BearSt10_1_2", "Supertrend(10,1) crossed below Supertrend(10,2) (overall trend vote: Supertrend(10,2))"],
  ["BearSt10_2_3", "Supertrend(10,2) crossed below Supertrend(10,3) (overall trend vote: Supertrend(10,3))"],
  ["BearSt1CloseCrossBelow", "Close (EMA1) crossed below ST (overall trend vote: Supertrend(10,1))"],
  ["BearVwapCloseCrossBelow", "Close (EMA1) crossed below VWAP (overall trend vote: VWAP)"],
  ["BearPbrAo", "Detect bearish trend (AO line)"],
  ["BearPbrSmiio", "Detect bearish trend (SMI line)"],
  ["BearPbrDpo", "Detect bearish trend (DPO line)"],
  ["BearPbrMfi", "Detect bearish trend (MFI line)"],
  ["BearPbrUo", "Detect bearish trend (UO line)"],
  ["BearPbrWilliamsR", "Detect bearish trend (Williams %R line)"],
  ["BearPbrBbpct", "Detect bearish trend (BB%b line)"],
  ["BearPbrPvt", "Detect bearish trend (PVT line)"],
  ["BearPbrAd", "Detect bearish trend (A/D line)"],
  ["BearPbrSmf", "Detect bearish trend (SMF line)"],
  ["BearPbrBbwUp", "BBW line direction upward (bands expanding)"],
  ["BearPbrAtrUp", "ATR line direction upward (wide candles)"],
  ["BearPbrVoloscUp", "VolOsc line direction upward (volume picking up)"],
  ["BearMeetEma9_21", "EMA 9 below EMA 21 (level, overall trend vote: EMA 21)"],
  ["BearMeetEma21_35", "EMA 21 below EMA 35 (level, overall trend vote: EMA 35)"],
  ["BearMeetEma35_50", "EMA 35 below EMA 50 (level, overall trend vote: EMA 50)"],
  ["BearMeetEma50_100", "EMA 50 below EMA 100 (level, overall trend vote: EMA 100)"],
  ["BearMeetEma100_200", "EMA 100 below EMA 200 (level, overall trend vote: EMA 200)"],
  ["BearMeetEma200_300", "EMA 200 below EMA 300 (level, overall trend vote: EMA 300)"],
  ["BearMeetSt10_1_2", "Supertrend(10,1) below Supertrend(10,2) (level, overall trend vote: Supertrend(10,2))"],
  ["BearMeetSt10_2_3", "Supertrend(10,2) below Supertrend(10,3) (level, overall trend vote: Supertrend(10,3))"],
  ["BearMeetCloseSt", "Close (EMA1) below Supertrend (level, overall trend vote: Supertrend(10,1))"],
  ["BearMeetCloseVwap", "Close (EMA1) below VWAP (level, overall trend vote: VWAP)"],
  ["BearMeetPaneCross", "Pane crossed line pair still below (level)"],
  ["BearMeetCross", "Crossed-below indicator pair still below (level)"],
  ["BearMeetCloseBb", "Close (EMA1) below BB middle (level, overall trend vote: BB middle)"],
  ["BearMeetClosePc", "Close (EMA1) below PC middle line (level, overall trend vote: PC middle)"],
  ["BearCandle", "Candlestick patterns"],
  ["BearElliott", "Elliott Wave"],
  ["BearIndicator", "Indicators"],
  ["BearPane", "Pane indicators"],
  ["BearSymmetry", "Symmetry"],
  ["BearStructure", "Chart structure"],
  ["BearAtr", "ATR / Volatility"],
];

// Complete AST indicator-filter catalogue, SECTION-WISE, exactly like the old
// app's Bullish/Bearish filter sections (static rows + the dynamically built
// Pane-extra / Momentum-Gap / Overlay / Overlay-Meet / Straight-Line groups).
// Keys are the old element id minus the "astFilter" prefix.
function _filterLayout(isBull) {
  const B = isBull ? "Bull" : "Bear";
  const above = isBull ? "above" : "below";
  const bull = isBull ? "bullish" : "bearish";
  const rise = isBull ? "rising" : "falling";
  const pbG1 = (name) => `Detect ${bull} trend (${name} line)`;
  const emaCross = (a, b) => `EMA ${a} crossed ${above} EMA ${b} (overall trend vote: EMA ${b})`;
  const meetLevel = (a, b) => `EMA ${a} ${above} EMA ${b} (level, overall trend vote: EMA ${b})`;
  const pbgSimple = (name) => `${name} main line crossed ${above} signal line (level holding - no gap logic)`;
  const pbgGap = (name) => `${name} gap holding/widening ${isBull ? "upward" : "downward"} (level)`;
  const obr = (name) => pbG1(name);
  const ovl = (name) => `Close (EMA1) ${above} ${name} (level)`;
  const sl = (name) => `${name} ${bull} trend`;
  const vsl = (name) => `${name} ${bull} trend`;

  const groups = [];

  // Base rows (side-neutral element ids without a Bull/Bear prefix for the
  // first eight; the rest carry the side prefix).
  groups.push({
    head: "",
    rows: [
      [isBull ? "IncUp" : "IncDown", `Detect ${bull} trend`],
      [isBull ? "GapUp" : "GapDown", isBull ? "Gap increasing" : "Gap decreasing"],
      [isBull ? "IncUpAll" : "IncDownAll", `Detect ${bull} trend (all)`],
      [isBull ? "CrossUp" : "CrossDown", isBull ? "Crossed above" : "Crossed below"],
      [isBull ? "GtUp" : "GtDown", "Greater than"],
      [isBull ? "LtUp" : "LtDown", "Less than"],
      [isBull ? "PaneCrossUp" : "PaneCrossDown", "Pane crossover"],
      [isBull ? "PaneIncUpAll" : "PaneIncDownAll", `Detect ${bull} trend (pane lines, all)`],
      [`${B}BbwInc`, "BBW increasing"],
      [`${B}Smf`, `Smart Money Flow ${bull} (SMF ${above} signal)`],
      [`${B}Asr`, isBull ? "Support gap widening (price rising away from support)" : "Resistance gap widening (price falling away below resistance)"],
      [`${B}Oit`, `Detect ${bull} trend (OI Trend)`],
      [`${B}${isBull ? "BbCrossAbove" : "BbCrossBelow"}`, `Close (EMA1) crossed ${above} BB middle band (upper+lower expanding, overall trend vote: BB middle)`],
      [`${B}${isBull ? "PcCrossAbove" : "PcCrossBelow"}`, `Close (EMA1) crossed ${above} price channel middle line (overall trend vote: PC middle)`],
      ...[["9", "21"], ["21", "35"], ["35", "50"], ["50", "100"], ["100", "200"], ["200", "300"]].map(([a, b]) => [`${B}Ema${a}_${b}`, emaCross(a, b)]),
      ...[["1", "5"], ["5", "9"], ["1", "9"], ["9", "21"], ["21", "35"], ["35", "50"], ["50", "100"], ["100", "200"]].map(([a, b]) => [`${B}Slema${a}_${b}`, `Straight Line EMA ${a} crossed ${above} Straight Line EMA ${b} (overall trend vote: Straight Line EMA ${b})`]),
      [`${B}St10_1_2`, `Supertrend(10,1) crossed ${above} Supertrend(10,2) (overall trend vote: Supertrend(10,2))`],
      [`${B}St10_1_3`, `Supertrend(10,1) crossed ${above} Supertrend(10,3) (overall trend vote: Supertrend(10,3))`],
      [`${B}St10_2_3`, `Supertrend(10,2) crossed ${above} Supertrend(10,3) (overall trend vote: Supertrend(10,3))`],
      [`${B}${isBull ? "St1CloseCrossAbove" : "St1CloseCrossBelow"}`, `Close (EMA1) crossed ${above} ST (overall trend vote: Supertrend(10,1))`],
      [`${B}${isBull ? "VwapCloseCrossAbove" : "VwapCloseCrossBelow"}`, `Close (EMA1) crossed ${above} VWAP (overall trend vote: VWAP)`],
    ],
  });

  // EMA & EMA 1 crossover.
  groups.push({
    head: "EMA & EMA 1 crossover (Close / EMA1 vs each EMA - cross event)",
    rows: [9, 21, 35, 50, 100, 200, 300].map((b) => [`${B}Ema1_${b}`, `EMA 1 crossed ${above} EMA ${b} (overall trend vote: EMA ${b})`]),
  });

  // EMA 1 level: the held side of the same comparison, so an entry can fire
  // while EMA 1 (close) stays above/below each EMA - not only on the one cross
  // bar. Same rows as the crossover group, different (latched) semantics.
  groups.push({
    head: "EMA 1 level (Close / EMA1 vs each EMA - held above/below, stays true while it holds)",
    rows: [9, 21, 35, 50, 100, 200, 300].map((b) => [`${B}MeetEma1_${b}`, `EMA 1 ${above} EMA ${b} (level, overall trend vote: EMA ${b})`]),
  });

  // EMA trend (line slope).
  groups.push({
    head: "EMA trend (each EMA line's own slope)",
    rows: [9, 21, 35, 50, 100, 200, 300].map((n) => [`${B}EmaTrend${n}`, `EMA ${n} ${bull} (line ${rise})`]),
  });

  // Pane Indicator Behaviour - Group 1 (direction mirror).
  groups.push({
    head: "Pane Indicator Behaviour - Group 1 (direction mirror - line behaviour)",
    rows: [
      ["Ao", "AO"], ["Smiio", "SMI"], ["Dpo", "DPO"], ["Mfi", "MFI"], ["Uo", "UO"],
      ["WilliamsR", "Williams %R"], ["Bbpct", "BB%b"], ["Pvt", "PVT"], ["Ad", "A/D"], ["Smf", "SMF"],
    ].map(([tok, name]) => [`${B}Pbr${tok}`, pbG1(name)]),
  });

  // Pane-extra direction rows (inserted right before the Momentum Gap group).
  groups.push({
    head: "",
    rows: [
      ["Cmf", "CMF"], ["Cci", "CCI"], ["Sqzmom", "Squeeze Mom."],
    ].map(([tok, name]) => [`${B}Pbr${tok}`, pbG1(name)]).concat([
      [`${B}PbrElderforce`, "Force Index soft-confirm (confirmation only - never blocks entry)"],
    ]),
  });

  // Multi-Line Momentum Gap (level).
  groups.push({
    head: `Multi-Line Momentum Gap (level - main line above signal and gap holding/widening ${isBull ? "up" : "down"})`,
    rows: [
      ["Macd", "MACD"], ["Ppo", "PPO"], ["Tsi", "TSI"], ["Stochrsi", "Stoch RSI"],
      ["Rsi", "RSI"], ["Obv", "OBV"], ["Fisher", "Fisher"],
    ].map(([tok, name]) => [`${B}Pbg${tok}`, pbgSimple(name)]).concat([
      [`${B}PbgSmiio`, isBull
        ? "SMI & Signal below Histogram, both direction downward & histogram falling (level)"
        : "SMI & Signal above Histogram, both direction upward & histogram rising (level)"],
      [`${B}PbgSmf`, pbgGap("SMF")],
      [`${B}PbgAroon`, isBull
        ? "Aroon Up above Aroon Down (level - line direction not required, gap holding/widening)"
        : "Aroon Up below Aroon Down (level - line direction not required, gap holding/widening)"],
      [`${B}PbgVortex`, isBull
        ? "Vortex VI+ above VI-, VI+ direction upward & VI- direction downward, gap widening (level)"
        : "Vortex VI- above VI+, VI- direction upward & VI+ direction downward, gap widening (level)"],
      [`${B}PbgAdx`, isBull
        ? "ADX direction upward, +DI above -DI, +DI direction upward & -DI direction downward, +DI/-DI gap widening (level)"
        : "ADX direction downward, +DI below -DI, +DI direction downward & -DI direction upward, +DI/-DI gap widening (level)"],
    ]),
  });

  // Group 2 & 3 (strength / participation).
  groups.push({
    head: "Pane Indicator Behaviour - Group 2 & 3 (strength / participation - same in both lists)",
    rows: [
      [`${B}PbrBbwUp`, "BBW line direction upward (bands expanding)"],
      [`${B}PbrAtrUp`, "ATR line direction upward (wide candles)"],
      [`${B}PbrVoloscUp`, "VolOsc line direction upward (volume picking up)"],
    ],
  });

  // Meet Condition (level).
  groups.push({
    head: "Meet Condition (level - active while relationship holds, cross not required)",
    rows: [
      ...[["9", "21"], ["21", "35"], ["35", "50"], ["50", "100"], ["100", "200"], ["200", "300"]].map(([a, b]) => [`${B}MeetEma${a}_${b}`, meetLevel(a, b)]),
      [`${B}MeetSt10_1_2`, `Supertrend(10,1) ${above} Supertrend(10,2) (level, overall trend vote: Supertrend(10,2))`],
      [`${B}MeetSt10_2_3`, `Supertrend(10,2) ${above} Supertrend(10,3) (level, overall trend vote: Supertrend(10,3))`],
      [`${B}MeetCloseSt`, `Close (EMA1) ${above} Supertrend (level, overall trend vote: Supertrend(10,1))`],
      [`${B}MeetCloseVwap`, `Close (EMA1) ${above} VWAP (level, overall trend vote: VWAP)`],
      [`${B}MeetPaneCross`, `Pane crossed line pair still ${above} (level)`],
      [`${B}MeetCross`, `Crossed-${above} indicator pair still ${above} (level)`],
      [`${B}MeetCloseBb`, `Close (EMA1) ${above} BB middle (level, overall trend vote: BB middle)`],
      [`${B}MeetClosePc`, `Close (EMA1) ${above} PC middle line (level, overall trend vote: PC middle)`],
    ],
  });

  // Candle CLOSED above / below each indicator (persistent level).
  //
  // A level read, not a crossover: true on every scan while the last CLOSED
  // candle's close sits on this side of the indicator, false as soon as it does
  // not. Deliberately pinned to the last closed candle so an intrabar repaint of
  // the forming bar can never flip the gate mid-bar (the server ignores the
  // forming-bar options for this family).
  const closedRows = [];
  const closedAdd = (tok, name) => closedRows.push([
    `${B}${isBull ? "ClosedAbove" : "ClosedBelow"}${tok}`,
    `Candle closed ${above} ${name} (level - stays true while it holds)`,
  ]);
  [5, 9, 21, 35, 50, 100, 200, 300].forEach((n) => closedAdd(`Ema${n}`, `EMA ${n}`));
  closedAdd("St10_1", "Supertrend (10,1)");
  closedAdd("St10_2", "Supertrend (10,2)");
  closedAdd("St10_3", "Supertrend (10,3)");
  closedAdd("Vwap", "VWAP");
  closedAdd("BbUpper", "Bollinger upper band");
  closedAdd("BbMid", "Bollinger middle band");
  closedAdd("BbLower", "Bollinger lower band");
  closedAdd("PcMid", "Price Channel middle line");
  closedAdd("Hma", "HMA 20");
  closedAdd("Ma", "MA 20");
  closedAdd("Smma", "Smoothed MA 20");
  closedAdd("KeltnerUpper", "Keltner upper band");
  closedAdd("KeltnerMid", "Keltner middle line");
  closedAdd("KeltnerLower", "Keltner lower band");
  closedAdd("DonchianUpper", "Donchian upper band");
  closedAdd("DonchianMid", "Donchian middle line");
  closedAdd("DonchianLower", "Donchian lower band");
  closedAdd("Tenkan", "Ichimoku Tenkan-sen");
  closedAdd("Kijun", "Ichimoku Kijun-sen");
  closedAdd("SenkouA", "Ichimoku Senkou A");
  closedAdd("SenkouB", "Ichimoku Senkou B");
  closedAdd("Chandelier", "Chandelier long stop line");
  groups.push({
    head: `Candle closed ${above} indicator (level - persistent, reads the last CLOSED candle only)`,
    rows: closedRows,
  });

  // Straight Line EMA Crossover (level): the latching cross the operator asked
  // for, read on the chart's ZigZag "Straight Line EMA" (`slema`) lines instead
  // of the plain close EMAs. Fast vs slow straight-line EMA is read on the LIVE
  // bar, so the instant fast crosses above (bull) / below (bear) slow the row
  // fires; it then STAYS ON every bar the crossover still stands (relationship
  // holds - a fresh cross every tick is NOT required), and drops the moment the
  // OPPOSITE cross prints. Same ZigZag EMA settings as the "Straight Line
  // Indicators" rows (length N, swing 5, min leg 5; the length-1 line uses swing
  // 17), so the gate reads the SAME line the operator sees.
  groups.push({
    head: "Straight Line EMA Crossover (level - cross fires instantly, holds while the cross stands, OFF on the opposite cross)",
    rows: [[1, 5], [5, 9], [1, 9], [9, 21], [21, 35], [35, 50], [50, 100], [100, 200]]
      .map(([a, b]) => [`${B}MeetSlema${a}_${b}`, `Straight Line EMA ${a} crossed ${above} Straight Line EMA ${b} (level - holds while the cross stands, off on the opposite cross)`]),
  });

  // Overlay Meet Condition rows.
  groups.push({
    head: "",
    rows: [["Hma", "HMA"], ["Tenkan", "Ichimoku Tenkan-sen"], ["Kijun", "Ichimoku Kijun-sen"], ["Keltner", "Keltner middle"], ["Donchian", "Donchian middle"], ["TrendCore", "Trend Core"]]
      .map(([tok, name]) => [`${B}MeetOvl${tok}`, ovl(name)]),
  });

  // Overlay Indicator Behaviour (direction mirror).
  groups.push({
    head: "Overlay Indicator Behaviour (direction mirror - overlay line behaviour)",
    rows: [["Hma", "HMA"], ["Tenkan", "Ichimoku Tenkan-sen"], ["Kijun", "Ichimoku Kijun-sen"], ["SenkouA", "Ichimoku Senkou A"], ["Keltner", "Keltner middle"], ["Donchian", "Donchian middle"], ["TrendCore", "Trend Core"]]
      .map(([tok, name]) => [`${B}Obr${tok}`, obr(name)]),
  });

  // Straight Line Indicators (+ the Volume Line row).
  groups.push({
    head: "Straight Line Indicators",
    rows: [
      ["ElliottWave", "Elliott wave line"], ["SupplyDemand", "Structure Based Supply Demand"], ["PriceAction", "Price Action Trend line"],
      ["ZigZag", "ZigZag Trendline"], ["ComboMaster", "Combo Master line"], ["PaneConsensus", "Pane Consensus Signal line"],
      ["AutoTrendline", "Auto Trendline"], ["Pitchfork", "Pitchfork median line"], ["TrendProjection", "Trend Projection line"],
      ["GannFan", "Gann Fan line"], ["FibFan", "Fibonacci Fan line"], ["Srema", "S/R EMA Reversal line"],
      ["StraightEma1", "ZigZag EMA 1"], ["StraightEma5", "ZigZag EMA 5"], ["StraightEma9", "ZigZag EMA 9"],
      ["StraightEma30", "ZigZag EMA 30"], ["StraightEma50", "ZigZag EMA 50"],
      ["StraightEma100", "ZigZag EMA 100"], ["StZigZag", "Supertrend ZigZag line"],
      ["BbPctSl", "BB%b Straight Line"],
    ].map(([tok, name]) => [`${B}Sl${tok}`, sl(name)]).concat([
      [`${B}Vl`, sl("Volume line")],
      [`${B}SlConsensus`, sl("Straight Line Consensus line")],
      [`${B}SlSupport`, sl("Support Trendline")],
      [`${B}SlResistance`, sl("Resistance Trendline")],
      [`${B}SlOverallConsensusPrice`, sl("Overall Straight Line Consensus (Price)")],
      [`${B}SlPriceActionDiv`, sl("Price Action Diversionless (Price)")],
    ]),
  });

  // Visible Straight Line Filter: bullish/bearish TREND of each straight-line
  // indicator, identical to the "Straight Line Indicators" rows above (the old
  // visible drawn line "rising / falling" slope read was removed on operator
  // request). A rising line is bullish, a falling line is bearish; a still
  // warming-up / un-drawn line is non-blocking.
  groups.push({
    head: "Visible Straight Line Filter",
    rows: [
      ["ElliottWave", "Elliott wave line"], ["SupplyDemand", "Supply Demand line"], ["PriceAction", "Price Action Trend line"],
      ["ZigZag", "ZigZag Trendline"], ["ComboMaster", "Combo Master line"], ["PaneConsensus", "Pane Consensus Signal line"],
      ["AutoTrendline", "Auto Trendline"], ["Pitchfork", "Pitchfork median line"], ["TrendProjection", "Trend Projection line"],
      ["GannFan", "Gann Fan line"], ["FibFan", "Fibonacci Fan line"], ["Srema", "S/R EMA Reversal line"],
      ["StraightEma", "ZigZag EMA"],
      ["BbPctSl", "BB%b Straight Line"],
    ].map(([tok, name]) => [`${B}Vsl${tok}`, vsl(name)]).concat([
      [`${B}VslVl`, vsl("Volume line")],
      [`${B}VslConsensus`, vsl("Straight Line Consensus line")],
      [`${B}VslSupport`, vsl("Support Trendline")],
      [`${B}VslResistance`, vsl("Resistance Trendline")],
      [`${B}VslOverallConsensusPrice`, vsl("Overall Straight Line Consensus (Price)")],
      [`${B}VslPriceActionDiv`, vsl("Price Action Diversionless (Price)")],
    ]),
  });

  // Straight Line Indicator Color Detection: one filter per straight-line
  // indicator that fires purely on the LINE'S COLOR on the LAST CLOSED candle -
  // the line closing GREEN (rising) on the bullish side, RED (falling) on the
  // bearish side. The live forming bar is deliberately ignored, so the colour is
  // confirmed only once the candle closes. Server aliases SlColor* to the same
  // line-direction maths as the Sl* rows, read one bar back (last closed).
  // Bullish list => green detects; Bearish list => red detects.
  groups.push({
    head: "Straight Line Indicator Color Detection (" + (isBull ? "GREEN" : "RED") + " line on last CLOSED candle, scanned every 1s -> " + (isBull ? "bullish CE" : "bearish PE") + ")",
    rows: [
      ["ElliottWave", "Elliott wave line"], ["SupplyDemand", "Supply Demand line"], ["PriceAction", "Price Action Trend line"],
      ["ZigZag", "ZigZag Trendline"], ["ComboMaster", "Combo Master line"], ["PaneConsensus", "Pane Consensus Signal line"],
      ["AutoTrendline", "Auto Trendline"], ["Pitchfork", "Pitchfork median line"], ["TrendProjection", "Trend Projection line"],
      ["GannFan", "Gann Fan line"], ["FibFan", "Fibonacci Fan line"], ["Srema", "S/R EMA Reversal line"],
      ["StraightEma", "ZigZag EMA"],
      ["BbPctSl", "BB%b Straight Line"],
    ].map(([tok, name]) => [`${B}SlColor${tok}`, `${name} - ${isBull ? "GREEN" : "RED"} on closed candle`]).concat([
      [`${B}SlColorVl`, `Volume line - ${isBull ? "GREEN" : "RED"} on closed candle`],
      [`${B}SlColorConsensus`, `Straight Line Consensus line - ${isBull ? "GREEN" : "RED"} on closed candle`],
      [`${B}SlColorSupport`, `Support Trendline - ${isBull ? "GREEN" : "RED"} on closed candle`],
      [`${B}SlColorResistance`, `Resistance Trendline - ${isBull ? "GREEN" : "RED"} on closed candle`],
    ]),
  });

  // Arrow detection: trade the instant a fresh up/down arrow prints on the
  // selected indicator line. Bull side detects the bullish up arrow, Bear side
  // the bearish down arrow, using the exact same direction as the trend filters.
  groups.push({
    head: "Arrow Detection (fresh up/down arrow -> trade)",
    rows: [
      ["ElliottWave", "Elliott wave line arrow"], ["SupplyDemand", "Supply Demand line arrow"],
      ["PriceAction", "Price Action Trend line arrow"], ["ZigZag", "ZigZag Trendline arrow"],
      ["ComboMaster", "Combo Master line arrow"], ["PaneConsensus", "Pane Consensus Signal line arrow"],
      ["AutoTrendline", "Auto Trendline arrow"], ["Pitchfork", "Pitchfork median line arrow"],
      ["TrendProjection", "Trend Projection line arrow"], ["GannFan", "Gann Fan line arrow"],
      ["FibFan", "Fibonacci Fan line arrow"], ["Srema", "S/R EMA Reversal line arrow"],
      ["StraightEma", "Straightline EMA arrow"],
      ["BbPctSl", "BB%b Straight Line arrow"],
      ["TrendCore", "Trend Core line arrow"], ["Oit", "OI Trend arrow"], ["Vl", "Volume line arrow"],
      ["Consensus", "Straight Line Consensus line arrow"],
      ["Support", "Support Trendline arrow"], ["Resistance", "Resistance Trendline arrow"],
    ].map(([tok, name]) => [`${B}Arrow${tok}`, name]),
  });

  // Research stream scoping.
  groups.push({
    head: "Research stream (run only this group on this side)",
    rows: [
      [`${B}Candle`, "Candlestick patterns"],
      [`${B}Elliott`, "Elliott Wave"],
      [`${B}Indicator`, "Indicators"],
      [`${B}Pane`, "Pane indicators"],
      [`${B}Symmetry`, "Symmetry"],
      [`${B}Structure`, "Chart structure"],
      [`${B}Atr`, "ATR / Volatility"],
    ],
  });

  return groups;
}

const FILTER_LAYOUT_BULL = _filterLayout(true);
const FILTER_LAYOUT_BEAR = _filterLayout(false);
const FILTER_BULL = FILTER_LAYOUT_BULL.flatMap((s) => s.rows);
const FILTER_BEAR = FILTER_LAYOUT_BEAR.flatMap((s) => s.rows);

// Human label for an indicator-filter key (`BullMeetEma9_21` -> its row text),
// used by the PRIMARY "Entry indicator filter" picker/chips so the operator sees
// the same wording as the secondary filter list.
const FILTER_LABEL = {};
FILTER_BULL.concat(FILTER_BEAR).forEach((row) => { FILTER_LABEL[row[0]] = row[1]; });
function filterKeyLabel(key) {
  return FILTER_LABEL[key] || key;
}

// 1:1 key map between the Bullish and Bearish filter catalogues. Both layouts
// are produced by the same builder in the same row order, so index i in one list
// mirrors index i in the other. This also covers the first rows whose keys carry
// no Bull/Bear prefix (IncUp <-> IncDown, CrossUp <-> CrossDown, GtUp <-> GtDown,
// ...), which a naive "Bull" -> "Bear" string swap missed - making the Mirror
// button silently do nothing for exactly those rows.
const FILTER_MIRROR_TO_BEAR = {};
const FILTER_MIRROR_TO_BULL = {};
FILTER_BULL.forEach((row, i) => {
  const bear = FILTER_BEAR[i];
  if (!bear) return;
  FILTER_MIRROR_TO_BEAR[row[0]] = bear[0];
  FILTER_MIRROR_TO_BULL[bear[0]] = row[0];
});

// NIFTY / Index / Commodity Trend-Following confirmations now accept ANY
// indicator filter, read as that filter's HELD side: the bullish state assigns
// the CE leg, the bearish state the PE leg, and the side flips the moment the
// opposite crossover prints. Crossover rows collapse to their level form so the
// trend follows the side the cross moved to, not a one-bar event. Rows with no
// two-sided price-direction state (arrows, research scopes, volatility-only
// rows) and the straight-line family already offered as chart ids are skipped.
const TREND_PAIR_SKIP = /^(Candle|Elliott|Indicator|Pane|Symmetry|Structure|Atr|PbrElderforce|BbwInc|PbrBbwUp|PbrAtrUp|PbrVoloscUp|GtUp|GtDown|LtUp|LtDown)$/;
function _trendPairKey(key) {
  const base = String(key || "").replace(/^(Bull|Bear)/, "");
  if (!base || TREND_PAIR_SKIP.test(base) || /^Arrow/.test(base)) return null;
  if (/^(Sl|Vsl)/.test(base)) return null;
  if (/^(IncUp|IncDown)$/.test(base)) return "Inc";
  if (/^(IncUpAll|IncDownAll)$/.test(base)) return "IncAll";
  if (/^(GapUp|GapDown)$/.test(base)) return "Gap";
  if (/^(CrossUp|CrossDown)$/.test(base)) return "CloseVsEma9";
  if (/^(PaneCrossUp|PaneCrossDown)$/.test(base)) return "MeetPaneCross";
  if (/^(PaneIncUpAll|PaneIncDownAll)$/.test(base)) return "PaneIncAll";
  if (/^St10_/.test(base)) return "Meet" + base;
  if (/^St1CloseCross(Above|Below)$/.test(base)) return "MeetCloseSt";
  if (/^VwapCloseCross(Above|Below)$/.test(base)) return "MeetCloseVwap";
  if (/^BbCross(Above|Below)$/.test(base)) return "MeetCloseBb";
  if (/^PcCross(Above|Below)$/.test(base)) return "MeetClosePc";
  if (/^Ema\d+_\d+$/.test(base)) return "Meet" + base;
  return base;
}
const TREND_PAIR_NEUTRAL = {
  IncDown: "IncUp", IncDownAll: "IncUpAll", GapDown: "GapUp",
  CrossDown: "CrossUp", PaneCrossDown: "PaneCrossUp", PaneIncDownAll: "PaneIncUpAll",
};
const TREND_LABEL_OVERRIDES = {
  Inc: "Close rising / falling",
  IncAll: "EMA 9 / 21 / 35 stack",
  Gap: "Gap up / gap down",
  CloseVsEma9: "Close / EMA 1 vs EMA 9 (held side)",
  PaneIncAll: "Pane lines all rising / falling",
};
const TREND_FILTER_INDICATORS = (() => {
  const byKey = new Map();
  FILTER_BULL.forEach(([key, label]) => {
    const base = String(key).replace(/^Bull/, "");
    const canon = _trendPairKey(key);
    if (!canon) return;
    // Prefer the level ("Meet ...") wording: the trend follows the held side,
    // not a fresh cross event.
    const level = /^Meet/.test(base) ? 2 : 1;
    const prev = byKey.get(canon);
    if (prev && prev.level >= level) return;
    const id = TREND_PAIR_NEUTRAL[base] || key;
    let name = String(label || key).replace(/\s*\((?:level,\s*)?overall trend vote:[^)]*\)/i, "");
    if (TREND_LABEL_OVERRIDES[canon]) name = TREND_LABEL_OVERRIDES[canon];
    else {
      const m = /^MeetEma1_(\d+)$/.exec(canon);
      if (m) name = `EMA 1 / EMA ${m[1]} (held side)`;
    }
    byKey.set(canon, { v: id, level, i: { id, name } });
  });
  return Array.from(byKey.values()).map((r) => ({ v: r.v, i: r.i, g: "Indicator filter (flips on opposite cross)" }));
})();
const TREND_FILTER_NAME = {};
TREND_FILTER_INDICATORS.forEach((r) => { TREND_FILTER_NAME[r.v] = r.i.name; });

function filterSectionHTML(side) {
  const bull = side === "Bull";
  const layout = bull ? FILTER_LAYOUT_BULL : FILTER_LAYOUT_BEAR;
  const color = bull ? "#00d4aa" : "#ef5350";
  const border = bull ? "#1f4a35" : "#4a1f1f";
  const bg = bull ? "#0e231a" : "#230e0e";
  const body = layout
    .map((sec) => {
      const head = sec.head
        ? `<div style="border-top:1px solid #2d2d50;margin-top:2px;padding-top:3px;font-size:8px;color:#ffd700">${esc(sec.head)}</div>`
        : "";
      const rows = sec.rows
        .map((f) => {
          let gear = "";
          if (/(Sl|SlColor|Vsl)Support$/.test(f[0])) {
            gear = `<span class="pt-gear" data-pt="support" title="Support Trendline settings" style="cursor:pointer;color:#26a69a;font-size:11px;line-height:1">&#9881;</span>`;
          } else if (/(Sl|SlColor|Vsl)Resistance$/.test(f[0])) {
            gear = `<span class="pt-gear" data-pt="resistance" title="Resistance Trendline settings" style="cursor:pointer;color:#ef5350;font-size:11px;line-height:1">&#9881;</span>`;
          }
          return `<div style="display:flex;align-items:center;gap:3px"><label style="font-size:9px;color:#ccc;display:flex;align-items:center;gap:3px;flex:1"><input type="checkbox" data-filter="${f[0]}"> ${esc(f[1])}</label>${gear}</div>`;
        })
        .join("");
      return head + rows;
    })
    .join("");
  return `<div id="rtFilterSection${side}" style="border:1px solid ${border};border-radius:4px;padding:4px 8px;margin-right:10px;background:${bg};min-width:230px;max-width:340px">
    <label style="font-size:9px;color:${color};display:flex;align-items:center;gap:3px;font-weight:700">
      <input type="checkbox" data-master="${side}" style="accent-color:${color}"> ${bull ? "Bullish" : "Bearish"} <span style="font-weight:400;color:#666;font-size:8px">(select all)</span></label>
    <div style="margin-top:4px;display:flex;flex-direction:column;gap:3px;max-height:260px;overflow-y:auto">
      ${body}
    </div>
  </div>`;
}

function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}
function fmtMoney(n) {
  if (n == null || isNaN(n)) return "--";
  return "\u20b9" + Number(n).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
function isTruthy(v) {
  return v === true || v === 1 || v === "1" || v === "true";
}
function el(id) {
  return document.getElementById(id);
}

// In-page dialogs. Native `confirm()`/`alert()` are silently suppressed when
// the pane runs inside a sandboxed preview frame (the call just returns false),
// which made every guarded action look dead. These render a real modal so the
// confirm/alert gates work identically in the standalone page, the RT tab and
// the Paper Trade frame.
function ensureDialog() {
  let back = el("rtDialogBackdrop");
  if (back) return back;
  back = document.createElement("div");
  back.id = "rtDialogBackdrop";
  back.innerHTML =
    '<div id="rtDialog" role="dialog" aria-modal="true">' +
    '<div class="rt-dialog-title" id="rtDialogTitle"></div>' +
    '<div class="rt-dialog-msg" id="rtDialogMsg"></div>' +
    '<div class="rt-dialog-actions">' +
    '<button class="btn-action" id="rtDialogCancel">Cancel</button>' +
    '<button class="btn-action" id="rtDialogOk">OK</button>' +
    "</div></div>";
  document.body.appendChild(back);
  return back;
}

function uiDialog(msg, opts) {
  opts = opts || {};
  return new Promise((resolve) => {
    const back = ensureDialog();
    el("rtDialogTitle").textContent = opts.title || (opts.alertOnly ? "Notice" : "Confirm");
    el("rtDialogMsg").textContent = msg;
    const cancel = el("rtDialogCancel");
    const ok = el("rtDialogOk");
    cancel.style.display = opts.alertOnly ? "none" : "";
    ok.classList.toggle("danger", !!opts.danger);
    ok.textContent = opts.okText || (opts.alertOnly ? "OK" : "Confirm");
    const done = (v) => {
      back.classList.remove("show");
      back.onclick = null;
      ok.onclick = null;
      cancel.onclick = null;
      document.removeEventListener("keydown", onKey);
      resolve(v);
    };
    const onKey = (e) => {
      if (e.key === "Escape") done(false);
      else if (e.key === "Enter") done(true);
    };
    back.onclick = (e) => { if (e.target === back) done(false); };
    cancel.onclick = () => done(false);
    ok.onclick = () => done(true);
    document.addEventListener("keydown", onKey);
    back.classList.add("show");
    setTimeout(() => { try { ok.focus(); } catch (_) {} }, 30);
  });
}
function uiConfirm(msg, opts) {
  return uiDialog(msg, opts);
}
function uiAlert(msg) {
  return uiDialog(msg, { alertOnly: true });
}

// Single working "Square Off All Trades" action shared by both tabs (shared
// module: the Paper frame rewrites /api/rt -> /api/paper, so the same call
// squares off the simulated book there). It closes every running engine trade at
// market and, on the real tab, also flattens any broker manual/residual qty.
async function squareOffAllTrades() {
  const pos = (STATE && STATE.positions) || [];
  const n = pos.length;
  const msg = PAPER
    ? "Square Off ALL running paper trades at market?\n\nSimulated exits - koi real Dhan order nahi jayega." +
      (n ? "\n\n" + n + " running trade(s) close honge." : "\n\nAbhi koi running trade nahi hai.")
    : "Square Off ALL running trades at market?\n\nExits WILL be sent to Dhan." +
      (n ? "\n\n" + n + " running trade(s) close honge." : "");
  if (!(await uiConfirm(msg, { danger: true }))) return;
  try {
    await API.squareOff();
  } catch (e) {
    await uiAlert("Square Off All Trades failed: " + (e && e.message ? e.message : e));
    return;
  }
  if (typeof refresh === "function") refresh();
}

// Per-filter settings for the Support / Resistance Trendline indicator filters.
// These are the geometry inputs the engine's SlSupport / SlResistance filter rows
// use to fit the line (Pivot strength / ATR period / Min tol % / Tol ATR mult /
// Pivots to scan / Forward bars / Full span). Colours / line width are kept for
// parity with the chart indicator's own settings panel. Values live in settings +
// hidden [data-set] inputs so a generic settings save never erases them.
const PT_DEFAULTS = {
  strength: 5,
  atrPeriod: 14,
  minPct: 0.05,
  tolMult: 0.5,
  look: 12,
  fwd: 10,
  fullSpan: false,
  upColor: "#26a69a",
  downColor: "#ef5350",
  lineWidth: 2,
};

function openPivotTrendSettings(kind) {
  const sup = kind === "support";
  const p = sup ? "sup" : "res";
  const cap = sup ? "Support" : "Resistance";
  const s = (STATE && STATE.settings) || {};
  const key = (f) => p + f.charAt(0).toUpperCase() + f.slice(1);
  const v = (f) => (s[key(f)] == null || s[key(f)] === "" ? PT_DEFAULTS[f] : s[key(f)]);
  const backId = "rtPtBackdrop";
  let back = document.getElementById(backId);
  if (back) back.remove();
  back = document.createElement("div");
  back.id = backId;
  back.style.cssText = "position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:100000;display:flex;align-items:center;justify-content:center";
  back.innerHTML =
    '<div style="background:#12122a;border:1px solid #3d3d6b;border-radius:8px;padding:14px 16px;min-width:310px;color:#eaeaf5">' +
    '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px"><span style="font-weight:700;color:' + (sup ? "#26a69a" : "#ef5350") + '">' + cap + ' Trendline Settings</span><span id="ptClose" style="cursor:pointer;color:#888;font-size:14px;line-height:1">&#10005;</span></div>' +
    '<div style="display:grid;grid-template-columns:1fr 100px;gap:6px;align-items:center;font-size:11px">' +
    '<span>Pivot strength</span><input type="number" id="ptStrength" min="2" max="50" step="1" value="' + v("strength") + '">' +
    '<span>ATR period</span><input type="number" id="ptAtrPeriod" min="2" max="200" step="1" value="' + v("atrPeriod") + '">' +
    '<span>Min tol %</span><input type="number" id="ptMinPct" min="0" max="5" step="0.05" value="' + v("minPct") + '">' +
    '<span>Tol ATR mult</span><input type="number" id="ptTolMult" min="0" max="10" step="0.1" value="' + v("tolMult") + '">' +
    '<span>Pivots to scan</span><input type="number" id="ptLook" min="3" max="400" step="1" value="' + v("look") + '">' +
    '<span>Forward bars</span><input type="number" id="ptFwd" min="0" max="200" step="1" value="' + v("fwd") + '">' +
    '<span>Full span</span><input type="checkbox" id="ptFullSpan"' + (v("fullSpan") ? " checked" : "") + ">" +
    '<span>Rising color</span><input type="color" id="ptUpColor" value="' + v("upColor") + '">' +
    '<span>Falling color</span><input type="color" id="ptDownColor" value="' + v("downColor") + '">' +
    '<span>Line width</span><input type="number" id="ptLineWidth" min="1" max="5" step="1" value="' + v("lineWidth") + '">' +
    "</div>" +
    '<div style="display:flex;gap:8px;justify-content:flex-end;margin-top:10px">' +
    '<button class="btn-action" id="ptReset">Reset</button>' +
    '<button class="btn-action" id="ptCancel">Cancel</button>' +
    '<button class="btn-action" id="ptSave" style="background:#00d4aa;color:#0a0a18;font-weight:700">Save</button>' +
    "</div></div>";
  document.body.appendChild(back);
  const close = () => back.remove();
  const g = (id) => document.getElementById(id);
  const setFields = (src) => {
    g("ptStrength").value = src.strength;
    g("ptAtrPeriod").value = src.atrPeriod;
    g("ptMinPct").value = src.minPct;
    g("ptTolMult").value = src.tolMult;
    g("ptLook").value = src.look;
    g("ptFwd").value = src.fwd;
    g("ptFullSpan").checked = !!src.fullSpan;
    g("ptUpColor").value = src.upColor;
    g("ptDownColor").value = src.downColor;
    g("ptLineWidth").value = src.lineWidth;
  };
  back.onclick = (e) => { if (e.target === back) close(); };
  g("ptClose").onclick = close;
  g("ptCancel").onclick = close;
  g("ptReset").onclick = () => setFields(PT_DEFAULTS);
  g("ptSave").onclick = () => {
    const fields = {
      [key("strength")]: num(g("ptStrength").value),
      [key("atrPeriod")]: num(g("ptAtrPeriod").value),
      [key("minPct")]: num(g("ptMinPct").value),
      [key("tolMult")]: num(g("ptTolMult").value),
      [key("look")]: num(g("ptLook").value),
      [key("fwd")]: num(g("ptFwd").value),
      [key("fullSpan")]: g("ptFullSpan").checked,
      [key("upColor")]: g("ptUpColor").value,
      [key("downColor")]: g("ptDownColor").value,
      [key("lineWidth")]: num(g("ptLineWidth").value),
    };
    Object.keys(fields).forEach((k) => {
      const hi = document.querySelector('#tab-realtime [data-set="' + k + '"]');
      if (hi) {
        if (hi.type === "checkbox") hi.checked = !!fields[k];
        else hi.value = fields[k];
      }
    });
    close();
    API.settings(Object.assign({}, (STATE && STATE.settings) || {}, fields)).then(refresh);
  };
}

// Advanced thresholds for the "Ignore fake breakouts / reversals" filter. The
// master checkbox lives in every trend row; these knobs tune exactly how strict
// the fake-move test is. Values persist as normal camelCase settings so the
// backend `anti_fake_read` reads them straight off the saved Settings.
const ANTI_FAKE_DEFAULTS = {
  antiFakeBars: 2,
  antiFakeAtrMult: 0.25,
  antiFakeVolMult: 0.5,
  antiFakeAdxMin: 18,
  antiFakeChopMax: 61.8,
  antiFakeCooldownBars: 2,
};

function openAntiFakeSettings() {
  const s = (STATE && STATE.settings) || {};
  const v = (k) => (s[k] == null || s[k] === "" ? ANTI_FAKE_DEFAULTS[k] : s[k]);
  const backId = "rtAntiFakeBackdrop";
  let back = document.getElementById(backId);
  if (back) back.remove();
  back = document.createElement("div");
  back.id = backId;
  back.style.cssText = "position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:100000;display:flex;align-items:center;justify-content:center";
  back.innerHTML =
    '<div style="background:#12122a;border:1px solid #3d3d6b;border-radius:8px;padding:14px 16px;min-width:320px;color:#eaeaf5">' +
    '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px"><span style="font-weight:700;color:#4fc3f7">Fake-move filter settings</span><span id="afClose" style="cursor:pointer;color:#888;font-size:14px;line-height:1">&#10005;</span></div>' +
    '<div style="display:grid;grid-template-columns:1fr 100px;gap:6px;align-items:center;font-size:11px">' +
    '<span>Persistence bars (same-side closes)</span><input type="number" id="afBars" min="1" max="20" step="1" value="' + v("antiFakeBars") + '">' +
    '<span>Min move (ATR multiple)</span><input type="number" id="afAtrMult" min="0" max="5" step="0.05" value="' + v("antiFakeAtrMult") + '">' +
    '<span>Min volume (fraction of avg)</span><input type="number" id="afVolMult" min="0" max="5" step="0.1" value="' + v("antiFakeVolMult") + '">' +
    '<span>Min ADX (trend strength)</span><input type="number" id="afAdxMin" min="0" max="100" step="1" value="' + v("antiFakeAdxMin") + '">' +
    '<span>Max Choppiness</span><input type="number" id="afChopMax" min="0" max="100" step="0.1" value="' + v("antiFakeChopMax") + '">' +
    '<span>Cooldown bars after flip</span><input type="number" id="afCooldown" min="0" max="50" step="1" value="' + v("antiFakeCooldownBars") + '">' +
    "</div>" +
    '<div style="font-size:9px;color:#888;margin-top:8px">Stricter = zyada fake entries block, par kuch real moves late/miss ho sakte hain. Paper me test karke tuning karo.</div>' +
    '<div style="display:flex;gap:8px;justify-content:flex-end;margin-top:10px">' +
    '<button class="btn-action" id="afReset">Reset</button>' +
    '<button class="btn-action" id="afCancel">Cancel</button>' +
    '<button class="btn-action" id="afSave" style="background:#4fc3f7;color:#0a0a18;font-weight:700">Save</button>' +
    "</div></div>";
  document.body.appendChild(back);
  const close = () => back.remove();
  const g = (id) => document.getElementById(id);
  back.onclick = (e) => { if (e.target === back) close(); };
  g("afClose").onclick = close;
  g("afCancel").onclick = close;
  g("afReset").onclick = () => {
    g("afBars").value = ANTI_FAKE_DEFAULTS.antiFakeBars;
    g("afAtrMult").value = ANTI_FAKE_DEFAULTS.antiFakeAtrMult;
    g("afVolMult").value = ANTI_FAKE_DEFAULTS.antiFakeVolMult;
    g("afAdxMin").value = ANTI_FAKE_DEFAULTS.antiFakeAdxMin;
    g("afChopMax").value = ANTI_FAKE_DEFAULTS.antiFakeChopMax;
    g("afCooldown").value = ANTI_FAKE_DEFAULTS.antiFakeCooldownBars;
  };
  g("afSave").onclick = () => {
    const fields = {
      antiFakeBars: Math.max(1, Math.min(20, Math.round(num(g("afBars").value) || 2))),
      antiFakeAtrMult: Math.max(0, Math.min(5, num(g("afAtrMult").value))),
      antiFakeVolMult: Math.max(0, Math.min(5, num(g("afVolMult").value))),
      antiFakeAdxMin: Math.max(0, Math.min(100, num(g("afAdxMin").value))),
      antiFakeChopMax: Math.max(0, Math.min(100, num(g("afChopMax").value))),
      antiFakeCooldownBars: Math.max(0, Math.min(50, Math.round(num(g("afCooldown").value) || 0))),
    };
    close();
    API.settings(Object.assign({}, (STATE && STATE.settings) || {}, fields)).then(refresh);
  };
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------
function style() {
  const css = `
  #tab-realtime { overflow-y: auto; }
  #tab-realtime.active { display: block; }
  #tab-realtime .account-section { padding: 8px; }
  #tab-realtime .rt-title { font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 1px; margin: 0; }
  #tab-realtime .rt-strip { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; padding: 6px 8px; background: #12122a; border: 1px solid var(--border2, #2d2d50); border-radius: 4px; margin: 0 8px 6px; font-size: 11px; }
  #tab-realtime .rt-pill { font-size: 11px; padding: 3px 9px; border-radius: 10px; background: #22224a; }
  #tab-realtime .rt-pill.on { background: #124a2a; color: #7CFFB2; }
  #tab-realtime .rt-pill.off { background: #4a1212; color: #ff9d9d; }
  #tab-realtime .rt-pill.armed { background: #5a3a00; color: #ffd479; }
  #tab-realtime .account-table { width: 100%; border-collapse: collapse; font-size: 10.5px; }
  #tab-realtime .account-table th { text-align: left; color: #8888b8; font-weight: 500; padding: 4px 5px; border-bottom: 1px solid #2a2a4a; position: sticky; top: 0; background: #111127; }
  #tab-realtime .account-table td { padding: 4px 5px; border-bottom: 1px solid #1c1c38; white-space: nowrap; }
  #tab-realtime .rt-scroll { max-height: 230px; overflow-y: scroll; border: 1px solid var(--border, #1e1e40); border-radius: 3px; scrollbar-width: thin; scrollbar-color: #4a4a80 #12122a; }
  #tab-realtime .rt-scroll.rt-scroll-open { max-height: none; overflow-y: visible; }
  #tab-realtime .rt-scan-fade { display: inline-flex; align-items: center; gap: 10px; flex-wrap: wrap; opacity: .26; transition: opacity .35s ease; pointer-events: none; }
  #tab-realtime .rt-scan-fade.on { opacity: 1; pointer-events: auto; }
  #tab-realtime .rt-scan-fade input:disabled, #tab-realtime .rt-scan-fade select:disabled { opacity: .5; }
  #tab-realtime .rt-pos { color: #7CFFB2; } #tab-realtime .rt-neg { color: #ff8888; }
  #tab-realtime .rtom-chip { user-select: none; }
  #tab-realtime .rtom-chip:hover { border-color: #b39ddb !important; }
  #tab-realtime .rtom-f { display: flex; align-items: center; gap: 5px; font-size: 11px; color: #b8b8c8; }
  #tab-realtime .rtom-f > span:first-child { min-width: 88px; }
  #tab-realtime input:not([type=checkbox]), #tab-realtime select { background: #1a1a35; border: 1px solid #35356a; color: #eaeaf5; border-radius: 3px; padding: 4px 6px; font-size: 12px; }
  #tab-realtime input[type=checkbox] { accent-color: #b39ddb; width: 15px; height: 15px; }
  #tab-realtime button { cursor: pointer; }
  #tab-realtime .rt-engine-row { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; padding: 6px 8px; border: 1px solid #1e1e40; border-radius: 4px; margin: 4px 0; background: #0d0d1e; }
  #tab-realtime .rt-engine-row b.rt-cap { color: #ffd700; font-size: 10px; white-space: nowrap; }
  #tab-realtime .rt-cond { display: grid; grid-template-columns: 1.4fr .8fr .8fr .6fr auto; gap: 4px; margin-bottom: 4px; }
  #tab-realtime .rt-log { font-family: monospace; font-size: 10px; line-height: 1.45; color: #bfbfd8; padding: 4px 6px; }
  #tab-realtime .rt-badge { font-size: 9px; padding: 1px 5px; border-radius: 8px; background: #333; }
  #rtDialogBackdrop { position: fixed; inset: 0; background: rgba(0,0,0,.6); display: none; align-items: center; justify-content: center; z-index: 9999; }
  #rtDialogBackdrop.show { display: flex; }
  #rtDialog { width: min(430px, 92vw); background: #14142c; border: 1px solid #3a3a60; border-radius: 6px; padding: 14px 16px; box-shadow: 0 8px 40px rgba(0,0,0,.6); }
  #rtDialog .rt-dialog-title { font-size: 12px; font-weight: 700; color: #e6e6ff; margin-bottom: 6px; }
  #rtDialog .rt-dialog-msg { font-size: 12px; color: #c0c0d8; white-space: pre-wrap; line-height: 1.5; }
  #rtDialog .rt-dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 14px; }
  #rtDialog .btn-action { width: auto; padding: 5px 16px; margin: 0; font-size: 11px; }
  #rtDialog #rtDialogOk.danger { background: #5e2d2d; border-color: #7e3d3d; }
  `;
  const e = document.createElement("style");
  e.textContent = css;
  document.head.appendChild(e);
}

// ---------------------------------------------------------------------------
// Shell (mirrors the old app's Realtime Trading Engine layout)
// ---------------------------------------------------------------------------
function shell() {
  const rt = document.getElementById("tab-realtime");
  if (!rt) return;
  rt.className = "tab-content";
  rt.innerHTML = `
    <div class="rt-strip">
      <span class="rt-title">Realtime Trading Engine</span>
      <span class="rt-pill" id="rtConn">checking…</span>
      <span class="rt-pill" id="rtEnginePill">Engine: OFF</span>
      <span class="rt-pill" id="rtArmPill">DISARMED</span>
      <span class="rt-pill" id="rtFundsPill">Funds: --</span>
      <span style="flex:1"></span>
      <button class="btn-action" id="rtRefreshAccount" style="width:auto;padding:3px 10px;margin:0 4px 0 0;font-size:10px">Refresh Account</button>
      <button class="btn-action" id="rtSquareOff" style="width:auto;padding:3px 10px;margin:0;font-size:10px;background:#8a1f1f;border-color:#b03030;font-weight:700;color:#fff">Square Off All Trades</button>
    </div>

    <div class="monitor-toolbar" id="rtAstTplRunSection" style="border:1px solid #4a2d7e;border-radius:4px;margin:4px 0;padding:6px 8px;background:#0d0d1e">
      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
        <b style="color:#b39ddb;font-size:10px">AST saved templates quick run:</b>
        <select id="rtAstTplRun" style="min-width:250px"><option value="">-- no saved templates --</option></select>
        <button class="btn-action" id="rtAstTplRunBtn" style="width:auto;padding:5px 14px;margin:0;background:#b39ddb;color:#0a0a18;font-weight:700;font-size:10px">Run saved template</button>
        <span style="font-size:8px;color:#666">Dropdown me se saved template chuno aur Run dabao - AI Smart Trading engine khud us template ki saved settings (SL/trail/TP, strikes, filters, symbols) apply karke uske saved mode (strategies / Indicator-filters / AI auto-pick) me trades start kar dega.</span>
      </div>
      <div style="margin-top:4px"><span id="rtAstTplRunStatus" style="font-size:9px;color:#888"></span></div>
    </div>

    <div class="monitor-toolbar" id="rtUniversalTplSection" style="border:1px solid #26a69a;border-radius:4px;margin:4px 0;padding:6px 8px;background:#071a17">
      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
        <b style="color:#26a69a;font-size:10px;white-space:nowrap">Universal Template Pool:</b>
        <label class="rtom-f">Name <input type="text" id="rtTplName" placeholder="Template name" style="width:130px"></label>
        <select id="rtTplMode" style="width:110px"><option value="bullish">Bullish</option><option value="bearish">Bearish</option><option value="sideways">Sideways</option></select>
        <button class="btn-action" id="rtTplSave" style="width:auto;padding:3px 10px;margin:0;background:#b39ddb;color:#0a0a18">Save Template</button>
        <span style="font-size:9px;color:#888">Apply in any engine:</span>
        <select id="rtTplOpen" style="min-width:200px"><option value="">-- none --</option></select>
        <button class="btn-action" id="rtTplApply" style="width:auto;padding:3px 10px;margin:0;background:#26a69a;color:#04120f;font-weight:700" title="Apply this saved template's settings to the current engine tab">Apply</button>
        <button class="btn-action warn" id="rtTplDelete" style="width:auto;padding:3px 10px;margin:0" title="Delete from the universal pool (removes it from every engine tab)">Delete</button>
        <span id="rtTplInfo" style="font-size:9px;color:#888"></span>
      </div>
      <div style="margin-top:3px;font-size:8px;color:#666">Saved once, available in every engine tab. Origin tab is stored automatically.</div>
    </div>

    <div class="account-section" style="border-top:1px solid #1e1e40">
      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
        <h3 class="rt-title">Running Strategies &amp; Trades</h3>
        <button class="btn-action" style="width:auto;padding:3px 10px;margin:0;font-size:10px" data-act="runref">Refresh</button>
        <button class="btn-action warn" style="width:auto;padding:3px 10px;margin:0;font-size:10px" data-act="closeall">Square Off All Trades</button>
      </div>
      <div id="rtMarginBar" style="display:none;align-items:center;gap:12px;flex-wrap:wrap;margin-top:4px;padding:4px 8px;background:#0d0d1e;border:1px solid #1e1e40;border-radius:3px;font-size:11px"></div>
      <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:4px">
        <div style="flex:1;min-width:280px">
          <div style="font-size:10px;color:#66ccff;font-weight:700;margin:4px 0 2px">Running Strategies (Indicator filter based)</div>
          <div id="rtRunStrategies" style="max-height:220px;overflow-y:auto"></div>
        </div>
        <div style="flex:1;min-width:280px">
          <div style="font-size:10px;color:#66ccff;font-weight:700;margin:4px 0 2px">Picked Strikes (Premium - trade execute)</div>
          <div id="rtRunStrikes" style="max-height:220px;overflow-y:auto"></div>
        </div>
        <div style="flex:1;min-width:280px">
          <div style="font-size:10px;color:#66ccff;font-weight:700;margin:4px 0 2px">Running Trades (Dhan open positions)</div>
          <div id="rtRunTrades" style="max-height:220px;overflow-y:auto"></div>
        </div>
      </div>
    </div>

    <h3 id="rtAccountSection" style="font-size:11px;color:#888;text-transform:uppercase;margin:6px 0 4px">Running Trades <span style="text-transform:none;color:#666;font-weight:normal">(open positions)</span></h3>
    <div class="rt-scroll"><table class="account-table" id="rtPos"><thead><tr>
      <th>Option</th><th>Qty</th><th>Entry</th><th>LTP</th><th>LTP chg</th><th>P&amp;L (gross)</th><th>SL (pts / %)</th><th>Guard</th><th></th>
    </tr></thead><tbody></tbody></table></div>

    <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:6px 0 4px">
      <h3 style="font-size:11px;color:#888;text-transform:uppercase;margin:0">Closed Trades <span id="rtClosedCount" style="text-transform:none;color:#666;font-weight:normal">(0)</span></h3>
      <button class="btn-action" id="rtClosedExpand" style="width:auto;padding:3px 10px;margin:0;font-size:10px" title="Closed Trades ki poori list ek saath dikhao (scroll box ki 230px limit hatao)">Show all</button>
    </div>
    <div class="rt-scroll" id="rtClosedWrap"><table class="account-table" id="rtClosed"><thead><tr>
      <th>Option</th><th>Qty</th><th>Entry → Exit</th><th>SL (pts / %)</th><th>LTP chg (entry)</th><th>P&amp;L (net)</th><th>Charges</th><th>Reason</th><th>Entry → Exit time</th>
    </tr></thead><tbody></tbody></table></div>

    <div class="account-section" style="border-top:1px solid #1e1e40;margin-top:6px">
      <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:4px 0">
        <h3 class="rt-title">AI Smart Trading Engine</h3>
        <button class="btn-action" id="rtEngineToggle" style="width:auto;padding:5px 12px;margin:0">AI Smart Trading: OFF</button>
        <label class="rtom-f" id="rtAutoTradeWrap" style="color:#00e676;font-weight:700;border:1px solid #1f6f4a;background:#0f2419;border-radius:4px;padding:3px 9px" title="ON: ek click me poori trend-following setup apply hoti hai - NIFTY trend direction + confirmation indicators, Top Movers universe, indicator-filter gate, ATR stop-loss, profit-armed trail SL aur Target Lab auto-cut. Engine master ON hote hi entries khud chalti hain. OFF: koi nayi entry nahi, par chal rahi trades apne SL / trail / target par protected rehti hain.">
          <input type="checkbox" id="rtAutoTradeCb" data-set="autoTradeOn" style="accent-color:#00e676"> Auto Trading (trend + SL + trail + target)
        </label>
        <button class="btn-action" data-act="ticknow" id="rtTickNow" style="width:auto;padding:5px 12px;margin:0;background:#66ccff;color:#0a0a18">Run / Tick Now</button>
        <span id="rtDisarmWarn" style="display:none;font-size:10px;font-weight:700;color:#ffd479;background:#2a220d;border:1px solid #7e6a2d;border-radius:4px;padding:3px 8px">Master ON hai par aaj auto square-off ho chuka hai, isliye naye entries agle IST din tak band hain; agle din armed khud ho jayega.</span>
        <button class="btn-action warn" data-act="resetpnl" style="width:auto;padding:5px 12px;margin:0" title="Clear every closed trade, log and realized total. Starts the next trade from a clean ₹0 slate.">Reset P&amp;L</button>
      </div>

      <div id="rtSummary" style="display:flex;gap:6px;margin-top:8px;flex-wrap:wrap"></div>

      <!-- ORDER PLACEMENT METHOD (old realtimeorders.js block) -->
      <div id="rtOrderMethods" class="account-section" style="border:1px solid #4a2d7e;border-radius:5px;margin:6px 0;padding:8px 10px;background:#0d0d1e">
        <div id="rtOrderMethodsHead" style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
          <h3 style="font-size:14px;color:#c4a9ff;text-transform:uppercase;margin:0">Order Placement Method</h3>
          <span style="font-size:11px;color:#888">Sirf ek method enable hota hai. Uski saari options niche Risk &amp; Quantity row me hain; engine khud order place karega (koi Place Order button nahi).</span>
          <span style="font-size:12px;color:#bbb;margin-left:auto">Dhan margin available: <b id="rtomMarginAvail" style="color:#ffd700">--</b></span>
        </div>
        <div id="rtomRow" style="display:flex;gap:10px;flex-wrap:wrap;margin-top:8px"></div>
        <div id="rtBrokerRow" style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-top:8px;border-top:1px dashed #4a4a80;padding-top:8px">
          <span id="rtBrokerLabel" style="font-size:12px;color:#c4a9ff;font-weight:700;white-space:nowrap">Send orders to:</span>
          <span id="rtBrokerChips" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center"></span>
          <span id="rtBrokerNote" style="font-size:10px;color:#888"></span>
        </div>
        <div id="rtEngineExtra" style="display:flex;flex-wrap:wrap;align-items:center;gap:10px;margin-top:8px;border-top:1px dashed #4a4a80;padding-top:8px;font-size:12px">
          <span style="color:#c4a9ff;font-weight:700;font-size:12px;white-space:nowrap">Engine controls:</span>
          <span class="rtom-f" title="Live available balance (paper me virtual wallet, real me Dhan funds). Ye read-only hai.">Available balance <b id="rtEngAvailBal" style="color:#00d4aa">--</b></span>
          <label class="rtom-f" title="ON: agar required margin available se zyada hai (ya margin cap / funds nahi), to signal turant SKIP ho jayega - wo tarde late/retry par KABHI nahi khulegi. OFF: purana behaviour (margin free hone par wahi signal baad me fill ho sakta hai)."><input type="checkbox" data-set="skipOnMarginBlock" style="accent-color:#00d4aa"> Skip if no margin (no late entry)</label>
          <label class="rtom-f"><input type="checkbox" data-set="slAuto"> SL auto (ATR-hunting-aware)</label>
          <label class="rtom-f"><input type="checkbox" data-set="tf1min"> 1 min</label>
          <label class="rtom-f"><input type="checkbox" data-set="tf5min"> 5 min</label>
          <label class="rtom-f"><input type="checkbox" data-set="mtf"> Multi-TF confirm</label>
          <label class="rtom-f"><input type="checkbox" data-set="useOwnSettings"> Use AST settings (own SL/trail/timeframe)</label>
          <label class="rtom-f"><input type="checkbox" data-set="aiSl"> AI Stop-Loss (save capital)</label>
          <label class="rtom-f"><input type="checkbox" data-set="aiTpPct"> AI TP % (decide run)</label>
         </div>
       </div>

      <!-- CHART TYPE: the series every indicator / filter / entry is evaluated on -->
      <div class="rt-engine-row" id="rtChartRow" style="border-color:#4a2d7e">
        <b class="rt-cap" style="color:#66ccff">Chart Type:</b>
        <label class="rtom-f" style="color:#66ccff" title="ON: indicator filters, strategies aur entry/exit DISPLAY chart (Heikin Ashi / Line / Renko / Candlestick) par run karenge - bilkul wahi series jo chart tab par dikhti hai. OFF (default): engine purane tarike se raw candle data par chalega, chart type sirf display rahega.">
          <input type="checkbox" data-set="filterOnDisplayChart" id="rtFilterOnDisplay" style="accent-color:#66ccff"> Run filters on display chart
        </label>
        <span id="rtChartKindWrap" style="display:inline-flex;align-items:center;gap:10px;flex-wrap:wrap">
        <label class="rtom-f" title="Algo isi chart par saare indicator, filter aur entry/exit evaluate karega - bilkul wahi series jo chart tab par dikhti hai. (Sirf jab upar wala switch ON ho.)">Evaluate on
          <select data-set="chartKind" id="rtChartKind" style="min-width:150px">
            <option value="candles">Candlestick</option>
            <option value="heikin_ashi">Heikin Ashi</option>
            <option value="line">Line</option>
            <option value="renko">Renko</option>
          </select>
        </label>
        <span id="rtRenkoCtl" style="display:none;align-items:center;gap:10px;flex-wrap:wrap">
          <label class="rtom-f">Brick mode
            <select data-set="renkoMode" id="rtRenkoMode" style="min-width:120px">
              <option value="traditional">Traditional</option>
              <option value="atr">ATR</option>
              <option value="percentage">Percentage</option>
            </select>
          </label>
          <label class="rtom-f" id="rtRenkoBoxRow">Box size <input type="number" data-set="renkoBoxSize" min="0.01" step="0.05" style="width:80px"></label>
          <label class="rtom-f" id="rtRenkoAtrRow" style="display:none">ATR length <input type="number" data-set="renkoAtrLength" min="1" max="200" step="1" style="width:70px"></label>
          <label class="rtom-f" id="rtRenkoPctRow" style="display:none">Percentage % <input type="number" data-set="renkoPercentage" min="0.01" step="0.05" style="width:70px"></label>
          <label class="rtom-f">Source
            <select data-set="renkoSource" id="rtRenkoSource" style="min-width:110px">
              <option value="close">Close</option>
              <option value="highlow">High-Low</option>
            </select>
          </label>
          <label class="rtom-f"><input type="checkbox" data-set="renkoWicks"> Wicks</label>
          <label class="rtom-f" style="color:#66ccff" title="Non-repaint Renko: bricks sirf CLOSED candles se banenge (forming candle ignore ho jayegi), to har brick bar-close par commit hota hai aur kabhi mid-bar aakar gayab/repaint nahi hota. Color har brick ka apne open/close se fix rehta hai. OFF = default (legacy Renko, forming candle bhi use hoti hai)."><input type="checkbox" data-set="renkoNonRepaint" id="rtRenkoNonRepaint" style="accent-color:#66ccff"> Non-repaint</label>
        </span>
        </span>
        <span id="rtChartKindHint" style="font-size:9px;color:#888">Switch ON karo to saare indicator filter isi DISPLAY chart par chalenge. OFF = default (raw candle data).</span>
      </div>

      <!-- ENGINE RISK & QUANTITY: SL / Trail SL / Lot / Auto-Lot -->
      <div class="rt-engine-row" id="rtEngineRisk" style="border-color:#4a2d7e;border-width:1px">
        <b class="rt-cap" style="color:#c4a9ff">Risk &amp; Quantity:</b>
        <label class="rtom-f" style="color:#ef5350"><input type="checkbox" id="rtEngSl"> SL <input type="number" id="rtEngSlPct" step="0.5" min="0" style="width:56px" placeholder="%"> %</label>
        <label class="rtom-f" style="color:#ffb74d"><input type="checkbox" id="rtEngPointTrailSl"> Trail SL Pts <input type="number" id="rtEngPointTrailSlPts" step="0.05" min="0" style="width:56px" placeholder="pts"></label>
        <label class="rtom-f" id="rtEngPtsFromPctWrap" style="color:#80cbc4;display:none" title="ON: Point Trail 'Pts' ko Manual Trail SL % se har strike ke premium par auto calculate kiya jayega (points = entry premium x % / 100, minimum 1 tick). Isse % hi single source ban jata hai."><input type="checkbox" id="rtEngPtsFromPct"> Pts = % (auto)</label>
        <label class="rtom-f" style="color:#ffb74d"><input type="checkbox" id="rtEngTrailSl"> Trail SL <input type="number" id="rtEngTrailSlPct" step="0.5" min="0" style="width:56px" placeholder="%"> %</label>
        <label class="rtom-f" style="color:#66ccff" title="Net Take-Profit: charges aur slippage ke baad ka NET profit track hota hai. Jab net profit is minimum rupee amount ko cross karega tab feature arm hoga; uske baad peak net se 'Giveback %' (default 25%) neeche aate hi exit. Paper aur real dono engines par kaam karta hai."><input type="checkbox" id="rtEngNetTp"> Net TP <input type="number" id="rtEngNetTpMin" step="50" min="0" style="width:72px" placeholder="min ₹"> ₹</label>
        <label class="rtom-f" style="color:#66ccff" title="Peak net profit ka kitna % giveback hone par exit karna hai. Blank ya 0 = auto 25%. Example: peak ₹4000, giveback 25% -> exit ₹3000.">Giveback <input type="number" id="rtEngNetTpGiveback" step="1" min="0" max="99" style="width:56px" placeholder="25 auto"> %</label>
        <label class="rtom-f" title="Lot size (per-lot qty). Khaali chhodne par engine har contract ka actual lot size Dhan scrip-master se auto fetch karta hai.">Lot size <input type="number" id="rtEngLot" step="1" min="0" style="width:64px" placeholder="auto"></label>
        <label class="rtom-f">Lots <input type="number" id="rtEngLots" step="1" min="1" style="width:56px"></label>
        <label class="rtom-f" style="color:#ffd700"><input type="checkbox" id="rtEngAutoLots"> Auto-Lot qty (OI + volume + margin)</label>
        <label class="rtom-f" style="color:#ffd700" title="Auto-Lot volume cap: traded volume ka itna % se zyada lots kabhi nahi">Vol % <input type="number" id="rtEngAutoLotVolPct" data-set="autoLotVolumePct" step="0.5" min="0" style="width:52px" placeholder="1"></label>
        <label class="rtom-f" style="color:#ffd700" title="Auto-Lot OI cap: open interest ka itna % se zyada lots kabhi nahi. Engine dono caps me se jo kam lots dega wahi buy karega.">OI % <input type="number" id="rtEngAutoLotOiPct" data-set="autoLotOiPct" step="0.5" min="0" style="width:52px" placeholder="1"></label>
        <span id="rtEngAutoLotsInfo" style="font-size:9px;color:#666"></span>
        <span id="rtEngSliceWrap" style="display:none;flex-basis:100%;flex-wrap:wrap;align-items:center;gap:10px;border-top:1px dashed #4a4a80;padding-top:7px">
          <label class="rtom-f" style="color:#ffd700" title="Iceberg slicing: bade order ko chhote child slices me todkar bhejta hai. ON karne par, jab total lots 'Lot slice limit' tak pahunche, entry 'Slice qty' lots ke slices me jayegi (jaise 20 lots, limit 20, slice 5 => 5-5 lots ke 4 slices). Paper tab me ye simulated child fills hote hain (Running/Closed me 'N slices' badge dikhega); Realtime tab me har slice ke liye asli Dhan child order jata hai. Off = single entry."><input type="checkbox" id="rtEngSliceOn" data-set="sliceOn" style="accent-color:#ffd700"> Auto Order Slicing (super ko iceberg jaisa; off = slice band)</label>
          <label class="rtom-f" style="color:#ffd700" title="Kitne total lots par slicing shuru ho. 0 = har entry par slice.">Lot slice limit <input type="number" id="rtEngSliceLotLimit" data-set="sliceLotLimit" min="0" step="1" style="width:64px" placeholder="20"></label>
          <label class="rtom-f" style="color:#ffd700" title="Har slice me kitne lots (0 = slicing band). Isko total lots se chhota rakhna zaroori hai, warna split nahi hoga.">Slice qty (lots) <input type="number" id="rtEngSliceLots" data-set="sliceLots" min="0" step="1" style="width:64px" placeholder="5"></label>
          <span id="rtEngSliceInfo" style="font-size:9px;color:#666"></span>
        </span>
        <span style="font-size:9px;color:#666">SL / Trail SL % yahin set karo - AI Smart engine har entry par inhe lagayega, phir khud SL/trail hit par exit karega. Take Profit ke liye AI risk management me RR / AI TP% use karo.</span>
        <span style="flex-basis:100%;height:0"></span>
        <span id="rtEngMethodWrap" style="display:flex;flex-wrap:wrap;align-items:center;gap:10px;flex-basis:100%;border-top:1px dashed #4a4a80;padding-top:7px">
          <span style="font-size:11px;color:#c4a9ff;font-weight:700;white-space:nowrap">Selected method:</span>
          <b id="rtEngMethodName" style="font-size:11px;color:#fff;white-space:nowrap">Super Order</b>
          <span id="rtEngMethodOpts" style="display:flex;flex-wrap:wrap;align-items:center;gap:10px"></span>
        </span>
        <span style="font-size:9px;color:#666">Ye sab options selected method par auto-apply hote hain; order place karne ki zarurat nahi - condition meet hote hi engine khud place karega. Req: <b id="rtEngReq" style="color:#66ccff">--</b> - Freeze: <b id="rtEngFreeze" style="color:#b39ddb">--</b></span>
      </div>

      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:8px;border:1px solid #1e1e40;border-radius:3px;padding:4px 8px">
        <label style="font-size:9px;color:#00d4aa;display:flex;align-items:center;gap:3px;cursor:pointer;font-weight:700" title="Live Data Pool: shows the shared realtime candle (open/high/low/close/volume), indicator values and the entry-condition PASS/FAIL for every resolved symbol. The strategies read the exact same pool, so what you see is what they trade.">
          <input type="checkbox" id="rtDataPool" data-set="data_pool" style="accent-color:#00d4aa"> Data Pool (live candle + indicator + filter readout)</label>
        <span id="rtDataPoolInfo" style="font-size:9px;color:#666">OFF - shared pool feeds strategies only</span>
        <button id="rtDataPoolRefresh" style="font-size:9px;color:#fff;background:#2d2d5e;border:1px solid #3d3d7e;border-radius:3px;padding:2px 10px;cursor:pointer;font-weight:700" title="Re-resolve the selected universe and re-render the pool readout now">Refresh</button>
      </div>
      <div id="rtDataPoolBody" style="display:none;margin-top:6px;max-height:240px;overflow-y:scroll;border:1px solid #1e1e40;border-radius:3px;scrollbar-width:thin;scrollbar-color:#4a4a80 #12122a;font-size:10px"></div>

      <div class="rt-engine-row">
        <b class="rt-cap">AI risk management:</b>
        <label class="rtom-f"><input type="checkbox" data-set="rrEnabled"> Set TP by Risk:Reward <input type="number" data-set="rrValue" step="0.1" style="width:56px"></label>
        <span id="rtRrStatus" style="font-size:9px;color:#00d4aa;font-weight:700"></span>
        <span style="font-size:9px;color:#666">Overall SL / Trail SL "Risk &amp; Quantity" row me hain (upar), AI Stop-Loss / AI TP% "Engine controls" me. Take Profit ya to Risk:Reward ya AI TP% se lagta hai.</span>
      </div>

      <div class="rt-engine-row" id="rtBrainRiskRow" style="border-color:#26a69a">
        <b class="rt-cap" style="color:#26a69a">Brain Risk &amp; Protection:</b>
        <label class="rtom-f" style="color:#ef5350" title="Daily loss cap (rupees). Din ka NET realised loss is amount tak pahunchte hi brain agle IST din tak koi nayi entry nahi kholta. 0 = off. Ye sirf nayi entries rokte hain - chal rahi trades apne SL / trail / target par protected rehti hain.">Daily loss cap <input type="number" data-set="riskDailyLossCap" step="500" min="0" style="width:80px" placeholder="0"> &#8377;</label>
        <label class="rtom-f" style="color:#ffb74d" title="Per-trade risk budget (rupees). ON hone par position ka size MARGIN se nahi, STOP ki doori se tay hota hai - tight stop = bade lots, wide stop = chhote lots, par rupee risk hamesha same. 0 = off (purana lots/margin path)." >Risk / trade <input type="number" data-set="riskPerTrade" step="100" min="0" style="width:80px" placeholder="0"> &#8377;</label>
        <label class="rtom-f" style="color:#ffd54f" title="Total open risk cap (rupees): sabhi open positions ke stop-distance x qty ka jod. Cap hit hone par nayi entry block hoti hai. 0 = off.">Max open risk <input type="number" data-set="riskMaxOpen" step="500" min="0" style="width:80px" placeholder="0"> &#8377;</label>
        <label class="rtom-f" style="color:#4fc3f7" title="Opening noise guard: session open ke baad itne minute tak koi nayi entry nahi (spreads wide, pehli candles repaint hoti hain). 0 = off.">No trade first <input type="number" data-set="entryNoTradeBeforeMin" step="1" min="0" style="width:64px" placeholder="0"> min</label>
        <label class="rtom-f" style="color:#80cbc4" title="Volatility band: ATR% ko instrument ki apni recent distribution se compare karta hai. Quiet percentile se neeche = costs dominate; Wild percentile se upar = noise se stops katte hain. Dono extreme par brain NO-TRADE kar deta hai. 0 = band off.">Vol band quiet <input type="number" data-set="volQuietPct" step="1" min="0" max="100" style="width:56px" placeholder="20"> / wild <input type="number" data-set="volWildPct" step="1" min="0" max="100" style="width:56px" placeholder="90"> %</label>
        <label class="rtom-f" style="color:#ffd54f" title="Option premium elasticity: underlying ke 1% move par PREMIUM kitne % move karta hai. Near-ATM weekly index option ~6-10x hota hai. Brain isi se cost ko R me convert karta hai (risk premium ke %, cost turnover ke %). Apne broker experience ke hisaab se set karo; 0 = 1.0 (futures jaisa).">Premium x <input type="number" data-set="brainOptionLeverage" step="1" min="0" style="width:56px" placeholder="8"> &times;</label>
        <label class="rtom-f" style="color:#ffd54f" title="Slippage override (basis points). 0 = built-in conservative default. Cost gate yahi slippage use karta hai.">Slippage <input type="number" data-set="riskSlippageBps" step="1" min="0" style="width:56px" placeholder="auto"> bps</label>
        <span style="flex-basis:100%;height:0"></span>
        <label class="rtom-f" style="color:#4db6ac;font-weight:700" title="Brain exit suite master. ON: breakeven ratchet, partial profit, time stop aur regime-flip exit sab active. OFF: purane exits bilkul waisे hi (SL / trail / target) - kuch nahi badalta."><input type="checkbox" data-set="exitSuiteOn" style="accent-color:#4db6ac"> Brain exit suite (breakeven + partial + time stop)</label>
        <label class="rtom-f" style="color:#80cbc4" title="Kitne R profit par stop entry par (breakeven) shift ho. 0 = off.">Breakeven at <input type="number" data-set="exitBreakevenR" step="0.1" min="0" style="width:56px" placeholder="1.0">R</label>
        <label class="rtom-f" style="color:#80cbc4" title="Partial profit: itne R par position ka diya fraction book karo. 0 = off.">Partial <input type="number" data-set="exitPartialFrac" step="0.1" min="0" max="1" style="width:56px" placeholder="0.5"> @ <input type="number" data-set="exitPartialR" step="0.1" min="0" style="width:56px" placeholder="2.0">R</label>
        <label class="rtom-f" style="color:#80cbc4" title="Time stop: itne minute baad trade force exit (theta bleed / dead capital se bachne ke liye). 0 = off.">Time stop <input type="number" data-set="exitTimeStopMin" step="5" min="0" style="width:64px" placeholder="0"> min</label>
        <label class="rtom-f" style="color:#80cbc4" title="Regime flip par exit: entry ke waqt ka regime (trend/range) badal jaye to trade band kar do."><input type="checkbox" data-set="exitOnRegimeFlip" style="accent-color:#4db6ac"> Exit on regime flip</label>
        <label class="rtom-f" style="color:#80cbc4" title="Filter flip par exit: jis same-side indicator filters ne entry di thi, wo hold karna band kar dein to trade band kar do. Entry ke waqt wahi filters position par freeze hote hain, isliye baad me filters badalne se purani trade par asar nahi padta. Feed cold ho to exit force nahi hota. Note: ye har position par ek candle read karta hai, isliye default OFF."><input type="checkbox" data-set="exitOnFilterFlip" style="accent-color:#4db6ac"> Exit on filter flip</label>
        <span style="font-size:9px;color:#666;flex-basis:100%">Ye sab SIRF Auto Trading ON par lagte hain. Har limit 0 = off, isliye purana behaviour safe rehta hai.</span>
      </div>

      <div class="rt-engine-row">
        <b class="rt-cap">Trades per strategy:</b>
        <label class="rtom-f"><input type="checkbox" data-set="tradeLimit"> Max trades</label>
        <input type="number" data-set="tradeLimitCount" step="1" style="width:56px">
        <label class="rtom-f"><input type="checkbox" data-set="aiTrades"> AI auto trades</label>
        <span id="rtTradesStatus" style="font-size:9px;color:#666;font-weight:700"></span>
      </div>

      <div class="rt-engine-row">
        <b class="rt-cap">Engine scan:</b>
        <label class="rtom-f" title="ON: engine apni entry conditions ko throttled cadence par scan karega (niche set kiya gaya). OFF: normal ultrafast ~100ms tick. Exits (SL/Trail/TP) hamesha fast rehte hain.">
          <input type="checkbox" id="rtEngineScanCb" data-set="scanIntervalOn" style="accent-color:#00d4aa"> Engine Scan
        </label>
        <span id="rtScanIntervalBox" class="rt-scan-fade" title="Engine ko kitne interval par scan karna hai. Minutes + Seconds + Milliseconds milakar ek total time banta hai (minimum 50ms).">
          <label class="rtom-f">Minutes <input type="number" id="rtScanMin" min="0" max="1440" step="1" style="width:70px" placeholder="0"></label>
          <label class="rtom-f">Seconds <input type="number" id="rtScanSec" min="0" max="59" step="1" style="width:64px" placeholder="0"></label>
          <label class="rtom-f">Milliseconds <input type="number" id="rtScanMs" min="0" max="999" step="10" style="width:82px" placeholder="100"></label>
        </span>
        <span id="rtScanBarCloseChk" class="rt-scan-fade" title="ON: fixed interval ke bajaye har bar close par (nayi candle shuru hote hi) engine scan karega.">
          <label class="rtom-f"><input type="checkbox" id="rtScanBarCloseCb" data-set="scanBarClose" style="accent-color:#ffd700"> Countdown to Bar Close</label>
        </span>
        <span id="rtScanBarCloseBox" class="rt-scan-fade" title="Jis timeframe ka bar close hone par scan karna hai.">
          <select id="rtScanBarCloseTf" data-set="scanBarCloseTf" style="min-width:210px">
            <option value="1min">1 minute countdown to bar close</option>
            <option value="5min">5 minute countdown to bar close</option>
          </select>
        </span>
        <input type="number" id="rtScanTotalMs" data-set="scanIntervalMs" min="50" max="86400000" step="10" style="display:none">
        <span id="rtScanStatus" style="font-size:9px;color:#666;font-weight:700"></span>
      </div>

      <div class="rt-engine-row">
        <b class="rt-cap">Trade times:</b>
        <label class="rtom-f"><input type="checkbox" data-set="startAfterEnabled"> Start trading after</label>
        <label class="rtom-f"><input type="time" data-set="startAfter" style="width:90px"></label>
        <label class="rtom-f"><input type="checkbox" data-set="noTradeAfterEnabled"> No trade after</label>
        <label class="rtom-f"><input type="time" data-set="noTradeAfter" style="width:90px"></label>
        <label class="rtom-f"><input type="checkbox" data-set="autoSquareOffEnabled"> Auto square off at</label>
        <label class="rtom-f"><input type="time" data-set="autoSquareOffTime" style="width:90px"></label>
        <span id="rtTimeStatus" style="font-size:9px;color:#666;font-weight:700"></span>
      </div>

      <div class="rt-engine-row" id="rtFnoExpiryRow" style="border-color:#4a3a0a">
        <b class="rt-cap">F&amp;O stock expiry day:</b>
        <label class="rtom-f" title="ON: jis din F&O stocks ki option expiry hai, us din neeche set time par saare F&O-stock option positions khud square off ho jayenge, aur us time ke baad koi nayi F&O-stock entry execute nahi hogi. Index aur commodity par koi asar nahi hota.">
          <input type="checkbox" data-set="fnoExpirySquareOffEnabled" style="accent-color:#ffd700"> Auto square off + no trade after
        </label>
        <label class="rtom-f"><input type="time" data-set="fnoExpirySquareOffTime" style="width:90px"></label>
        <label class="rtom-f" title="ON: jis din F&O stocks ki option expiry hai, us poore din koi bhi nayi F&O-stock entry execute NAHI hogi (open se hi band, time ka koi lena-dena nahi). Index aur commodity par koi asar nahi hota. Exits normal chalte rehte hain.">
          <input type="checkbox" data-set="fnoExpiryNoTradeDayEnabled" style="accent-color:#ef5350"> No trade on expiry day (all day)
        </label>
        <span style="font-size:8px;color:#888">Sirf F&amp;O stocks ki expiry wale din lagu (index / commodity par nahi).</span>
        <span id="rtFnoExpiryStatus" style="font-size:9px;color:#666;font-weight:700"></span>
      </div>

      <div class="rt-engine-row">
        <b class="rt-cap">Trading sessions:</b>
        <span style="font-size:8px;color:#666">Add multiple intraday windows (IST). Entries sirf enabled session ke andar fire honge; exits kabhi gated nahi.</span>
        <label class="rtom-f">Start <input type="text" id="rtSessionStart" value="09:30 AM" list="rtSessTimes" style="width:96px" autocomplete="off"></label>
        <label class="rtom-f">End <input type="text" id="rtSessionEnd" value="10:30 AM" list="rtSessTimes" style="width:96px" autocomplete="off"></label>
        <datalist id="rtSessTimes">
          <option value="09:15 AM"></option><option value="09:30 AM"></option><option value="10:00 AM"></option>
          <option value="10:30 AM"></option><option value="11:00 AM"></option><option value="12:00 PM"></option>
          <option value="01:00 PM"></option><option value="01:30 PM"></option><option value="02:00 PM"></option>
          <option value="02:30 PM"></option><option value="03:00 PM"></option><option value="03:14 PM"></option>
          <option value="03:15 PM"></option><option value="03:30 PM"></option>
        </datalist>
        <button type="button" class="btn-action" id="rtSessionAdd" style="width:auto;padding:4px 12px;margin:0 4px;font-size:10px">+ Add session</button>
        <span id="rtSessionStatus" style="font-size:9px;color:#666;font-weight:700"></span>
        <div id="rtTradeSessionsList" style="flex-basis:100%;display:flex;flex-wrap:wrap;gap:6px;align-items:center;margin-top:3px"></div>
      </div>

      <div class="rt-engine-row">
        <b class="rt-cap">NIFTY:</b>
        <span id="rtNiftyStatus" style="font-size:11px;color:#ffd700"></span>
        <span style="font-size:9px;color:#666">Straight-line confirm indicators drive the ensemble trend.</span>
      </div>

      <div class="rt-engine-row">
        <b class="rt-cap">Strike:</b>
        <label class="rtom-f">Option Type
          <select data-set="optionSide" style="width:120px"><option value="both">Both CE &amp; PE</option><option value="CE">Call (CE)</option><option value="PE">Put (PE)</option></select></label>
        <label class="rtom-f">Execute Trade In
          <select data-set="strikeMode" style="width:190px">
            <option value="above">Above ATM</option>
            <option value="below">Below ATM</option>
            <option value="both_atm">Above and below ATM</option>
            <option value="above_atm">Above including ATM</option>
            <option value="below_atm">Below including ATM</option>
            <option value="both_atm_inc">Above and below including ATM</option>
            <option value="atm">Only ATM</option>
          </select></label>
        <label class="rtom-f">Number of Strikes <input type="number" data-set="strikeCount" min="0" step="1" style="width:56px"></label>
        <label class="rtom-f"><input type="checkbox" data-set="onlyPositive"> Only +green premium strikes</label>
        <label class="rtom-f" title="Only +green ke sath: agar Execute Trade In ki chuni hui side (Above/Below ATM) ki saari strikes minus ho, to ATM ke opposite side (Below/Above) me plus-sign wali strike dhoondh kar usi leg (CE/PE) me trade karta hai. Single-side dropdown par hi lagu."><input type="checkbox" data-set="fallbackOppositeSide" style="accent-color:#00d4aa"> Opposite side fallback (if all minus)</label>
        <label class="rtom-f" title="ON hone par Execute Trade In ki chuni hui side ke plus-sign (rising) premiums me se sabse jyada LTP change % wala strike pick hota hai (tie: jyada LTP change). +green aur Opposite side fallback ke sath mil kar chalta hai. OFF par +green ka sabse kareeb wala rising strike."><input type="checkbox" data-set="fastestRising" style="accent-color:#00d4aa"> Pick fastest positive rising LTP</label>
        <label class="rtom-f">Fastest-Rising Strikes <input type="number" data-set="fastestCount" min="1" step="1" style="width:56px"></label>
      </div>

      <div class="rt-engine-row" id="rtManualStrikeRow" style="align-items:flex-start;border-color:#4a3a0a">
        <label class="rtom-f" style="color:#ffd700;font-weight:bold" title="Testing only: ON hone par engine ATM / Execute-Trade-In / Number of Strikes / +green / fastest-rising ki jagah sirf neeche add kiye strikes par option leg resolve karega.">
          <input type="checkbox" data-set="manualStrikesEnabled" id="rtManualStrikeCb"> Manual Strike Select (Testing only)
        </label>
        <span id="rtManualStrikeHint" style="font-size:8px;color:#888">ON karo, phir instrument + strike pick karke Add dabao. Ye real market me order place + trail SL test karne ke liye hai.</span>
        <div id="rtManualStrikeBox" style="flex-basis:100%;display:none;flex-wrap:wrap;gap:8px;align-items:center;margin-top:4px">
          <label class="rtom-f">Instrument <select id="rtManualInstrument" style="min-width:280px"><option value="">-- loading --</option></select></label>
          <label class="rtom-f">Expiry <select id="rtManualExpiry" style="width:130px"></select></label>
          <label class="rtom-f">Strike <select id="rtManualStrikePick" style="min-width:230px"></select></label>
          <label class="rtom-f">Side <select id="rtManualSide" style="width:130px"><option value="both">Both CE &amp; PE</option><option value="CE">CE only</option><option value="PE">PE only</option></select></label>
          <button class="btn-action" id="rtManualStrikeAdd" style="width:auto;padding:3px 12px;margin:0">+ Add Strike</button>
          <span id="rtManualStrikeStatus" style="font-size:9px;color:#888"></span>
          <div id="rtManualStrikeList" style="flex-basis:100%;display:flex;flex-wrap:wrap;gap:6px;align-items:center;font-size:9px;color:#ccc;margin-top:2px"></div>
        </div>
      </div>

      <div class="rt-engine-row">
        <label class="rtom-f" style="color:#ffd700;font-weight:bold"><input type="checkbox" data-set="premiumOnly"> Premium chart only (run + trade)</label>
        <span style="font-size:9px;color:#888">Strategies run AND trades execute on the option premium chart for all instruments.</span>
      </div>

      <div class="rt-engine-row" id="rtRunInRow">
        <b class="rt-cap">Strategy should be run in:</b>
        <label class="rtom-f">Indices <select data-set="runIndex" style="width:120px"><option value="spot">Spot chart</option><option value="premium">Option premium</option><option value="both">Both</option></select></label>
        <label class="rtom-f">F&amp;O stocks <select data-set="runFno" style="width:120px"><option value="spot">Spot chart</option><option value="premium">Option premium</option><option value="both">Both</option></select></label>
        <label class="rtom-f">Commodities <select data-set="runComm" style="width:130px"><option value="spot">Spot chart</option><option value="futures">Futures</option><option value="premium">Option premium</option><option value="both">Both</option></select></label>
        <label class="rtom-f"><input type="checkbox" data-set="runInDefault"> Make this default setting</label>
      </div>

      <div class="rt-engine-row" id="rtTradeInRow">
        <b class="rt-cap">Trade should be executed in:</b>
        <label class="rtom-f">Indices <select data-set="tradeInIndex" style="width:160px"><option value="spot">Spot chart</option><option value="premium">Selected strike option premium chart</option><option value="both">Both</option></select></label>
        <label class="rtom-f">F&amp;O stocks <select data-set="tradeInFno" style="width:180px" title="F&amp;O trades are always executed on the selected-strike option premium chart (engine-enforced)."><option value="premium">Selected strike option premium chart</option></select></label>
        <label class="rtom-f">Commodities <select data-set="tradeInComm" style="width:160px"><option value="spot">Futures contract</option><option value="premium">Option premium chart</option></select></label>
        <label class="rtom-f"><input type="checkbox" data-set="tradeInDefault"> Make this default setting</label>
        <span id="rtRoutingStatus" style="font-size:9px;color:#666;font-weight:700;flex-basis:100%"></span>
      </div>

      <div class="rt-engine-row">
        <b class="rt-cap">Top Gainers / Losers + Indices:</b>
        <button class="btn-action" data-toggle="moversOn" id="rtMoversToggle" style="width:auto;padding:3px 10px;margin:0;font-size:10px">Top Movers: OFF</button>
        <label class="rtom-f">Top gainers <input type="number" data-set="moversGainers" min="0" step="1" style="width:56px"></label>
        <label class="rtom-f">Top losers <input type="number" data-set="moversLosers" min="0" step="1" style="width:56px"></label>
        <label class="rtom-f">Indices (set CE/PE leg)
          <select id="rtMoversIndicesSelect" style="min-width:150px"><option value="">-- pick index --</option></select>
        </label>
        <button class="btn-action" id="rtMoversIndicesAdd" style="width:auto;padding:3px 10px;margin:0">Add more</button>
        <span id="rtMoversIndicesList" style="font-size:9px;color:#ccc;display:flex;flex-wrap:wrap;gap:4px;align-items:center"></span>
        <span style="font-size:8px;color:#666;flex-basis:100%">Each index trades only its selected CE/PE leg. No leg = skipped (index trading needs an explicit trend call).</span>
        <input type="hidden" data-list="moversIndices">
      </div>
      <div id="rtMoversList" style="display:none;margin-top:4px;font-size:9px;color:#ccc;background:#12122a;border:1px solid #2d2d50;border-radius:4px;padding:6px 8px"></div>

      <div class="rt-engine-row" id="rtLiquidRow">
        <b class="rt-cap" style="color:#4fc3f7">Liquid F&amp;O Stocks:</b>
        <button class="btn-action" data-toggle="liquidOn" id="rtLiquidToggle" style="width:auto;padding:3px 10px;margin:0;font-size:10px">Liquid F&amp;O: OFF</button>
        <!-- Equity Indicator Based Trade only: daily full-universe liquidity scan
             controls. Hidden (display:none) on every other engine by the
             EQUITY_ONLY/paper gating block, so the F&O engines never show them. -->
        <label class="rtom-f rt-eq-only" id="rtEqTradeCountRow" style="display:none">Stocks to trade <input type="number" data-set="liquidTradeCount" min="1" max="500" step="1" style="width:60px"></label>
        <label class="rtom-f rt-eq-only" id="rtEqScanTimeRow" style="display:none">Daily scan at <input type="time" data-set="liquidScanTime" style="width:100px"></label>
        <label class="rtom-f rt-eq-only" id="rtEqMinTurnoverRow" style="display:none" title="Minimum turnover (₹ crore) a stock must show at scan time to be eligible. The ranking is by turnover × growth, so this only keeps the very illiquid names out.">Min turnover <input type="number" data-set="liquidScanMinTurnoverCr" min="0" step="1" style="width:60px"> cr</label>
        <button class="btn-action rt-eq-only" id="rtEqScanNow" style="display:none;width:auto;padding:3px 10px;margin:0">Scan now</button>
        <label class="rtom-f rt-eq-only" id="rtEqScanPickRow" style="display:none">Scanned stocks
          <select id="rtEqScanSelect" style="min-width:220px"><option value="">-- run scan --</option></select>
        </label>
        <span class="rt-eq-only" id="rtEqScanInfo" style="display:none;font-size:9px;color:#8ab4f8;flex-basis:100%"></span>
        <label class="rtom-f">Top gainers <input type="number" data-set="liquidGainers" min="0" step="1" style="width:56px"></label>
        <label class="rtom-f">Top losers <input type="number" data-set="liquidLosers" min="0" step="1" style="width:56px"></label>
        <label class="rtom-f">Top gainer
          <select id="rtLiquidGainerSelect" style="min-width:150px"><option value="">-- pick stock --</option></select>
        </label>
        <label class="rtom-f">Top loser
          <select id="rtLiquidLoserSelect" style="min-width:150px"><option value="">-- pick stock --</option></select>
        </label>
        <label class="rtom-f">Add stock
          <select id="rtLiquidAddSelect" style="min-width:150px"><option value="">-- pick stock --</option></select>
        </label>
        <button class="btn-action" id="rtLiquidAddBtn" style="width:auto;padding:3px 10px;margin:0">+ Add</button>
        <span style="font-size:8px;color:#666;flex-basis:100%">Only the liquid index basket (NIFTY 50 / BANK NIFTY / SENSEX / FINNIFTY / MIDCAP NIFTY). Top gainers trade CE, top losers trade PE; a hand-added stock builds both legs and the indicator filters pick the side. Remove (x) any stock to keep the engine off it.</span>
        <div id="rtLiquidChips" style="flex-basis:100%;display:flex;flex-wrap:wrap;gap:4px;align-items:center;font-size:9px;color:#ccc"></div>
        <input type="hidden" data-list="liquidList">
        <!-- Liquid-only lot policy: Auto-Lot + iceberg slicing that apply ONLY to
             legs armed by this Liquid F&O scanner. Kept separate from the global
             Risk & Quantity Auto-Lot / Slicing so the liquid basket can size and
             slice differently from Top Movers / indices / commodities. Shown on
             Realtime Trading Engine and Paper Trade 1 only. -->
        <span id="rtLiquidLotWrap" style="flex-basis:100%;display:flex;flex-wrap:wrap;align-items:center;gap:10px;border-top:1px dashed #4a4a80;padding-top:7px;margin-top:5px">
          <b class="rt-cap" style="color:#4fc3f7;flex-basis:100%">Liquid F&amp;O lot policy (applies to Liquid F&amp;O picks only):</b>
          <label class="rtom-f" style="color:#ffd700" title="Liquid-only Auto-Lot: sirf Liquid F&amp;O scanner se aaye legs ka quantity isi policy se decide hoga - min(volume %, OI %, margin). Top Movers / Indices / Commodities par koi asar nahi. OFF = liquid stocks Top Movers jaisa hi upar wala global Auto-Lot (Vol%/OI%) use karenge."><input type="checkbox" id="rtLiquidAutoLots" data-set="liquidAutoLotOn" style="accent-color:#ffd700"> Liquid Auto-Lot qty (OI + volume + margin)</label>
          <label class="rtom-f" style="color:#ffd700" title="Liquid Auto-Lot volume cap: liquid contract ke traded volume ka itna % se zyada lots kabhi nahi.">Vol % <input type="number" id="rtLiquidAutoLotVolPct" data-set="liquidAutoLotVolumePct" step="0.5" min="0" style="width:52px" placeholder="1"></label>
          <label class="rtom-f" style="color:#ffd700" title="Liquid Auto-Lot OI cap: liquid contract ke open interest ka itna % se zyada lots kabhi nahi. Engine volume, OI aur margin me se jo sabse kam lots de wahi lega.">OI % <input type="number" id="rtLiquidAutoLotOiPct" data-set="liquidAutoLotOiPct" step="0.5" min="0" style="width:52px" placeholder="1"></label>
          <span id="rtLiquidAutoLotsInfo" style="font-size:9px;color:#666"></span>
          <span id="rtLiquidSliceWrap" style="flex-basis:100%;display:flex;flex-wrap:wrap;align-items:center;gap:10px;border-top:1px dashed #4a4a80;padding-top:7px">
            <label class="rtom-f" style="color:#ffd700" title="Liquid-only iceberg slicing: Liquid F&amp;O legs ke bade order ko chhote child slices me todta hai. Jab total lots 'Lot slice limit' tak pahunche, entry 'Slice qty' lots ke slices me jayegi. Paper Trade 1 me simulated child fills, Realtime me har slice ka asli Dhan child order. Global slicing se alag hai. OFF = liquid stocks Top Movers jaisa hi upar wala global Slicing use karenge (aur slicing band ho to normal Lots qty)."><input type="checkbox" id="rtLiquidSliceOn" data-set="liquidSliceOn" style="accent-color:#ffd700"> Liquid Auto Order Slicing (iceberg)</label>
            <label class="rtom-f" style="color:#ffd700" title="Kitne total lots par liquid slicing shuru ho. 0 = har liquid entry par slice.">Lot slice limit <input type="number" id="rtLiquidSliceLotLimit" data-set="liquidSliceLotLimit" min="0" step="1" style="width:64px" placeholder="20"></label>
            <label class="rtom-f" style="color:#ffd700" title="Har liquid slice me kitne lots (0 = liquid slicing band). Total lots se chhota rakhna zaroori hai.">Slice qty (lots) <input type="number" id="rtLiquidSliceLots" data-set="liquidSliceLots" min="0" step="1" style="width:64px" placeholder="5"></label>
            <span id="rtLiquidSliceInfo" style="font-size:9px;color:#666"></span>
          </span>
        </span>
      </div>
      <div id="rtLiquidList" style="display:none;margin-top:4px;font-size:9px;color:#ccc;background:#12122a;border:1px solid #2d2d50;border-radius:4px;padding:6px 8px"></div>

      <div class="rt-engine-row">
        <b class="rt-cap" style="color:#00d4aa">NIFTY Trend Following:</b>
        <button class="btn-action" data-toggle="niftyTrendOn" id="rtNiftyTrendToggle" style="width:auto;padding:3px 10px;margin:0;font-size:10px">Trend Follow: OFF</button>
        <label class="rtom-f" style="color:#ff8a65;font-weight:bold" title="ON: jab market sideways ho (weak ADX + high choppiness + flat EMA slope + tight range) to koi naya trend-follow entry nahi hoga - NIFTY, Indices aur Commodities teeno par. Running trades apne SL/target/trail/net-TP par chalti rahengi. Regime bars unavailable = no new trade (fail-closed).">
          <input type="checkbox" data-set="noTradeSideways" id="rtNiftySidewaysCb" class="rt-sideways-cb" style="accent-color:#ff8a65"> No trade on sideways trend
        </label>
        <label class="rtom-f">Confirm indicators
          <select id="rtNiftyTrendConfIndSelect" style="min-width:160px"><option value="">-- pick indicator --</option></select>
        </label>
        <button class="btn-action" id="rtNiftyTrendConfIndAdd" style="width:auto;padding:3px 10px;margin:0">Add</button>
        <span id="rtNiftyTrendConfIndList" style="font-size:9px;color:#ccc;display:flex;flex-wrap:wrap;gap:4px;align-items:center"></span>
        <input type="hidden" data-list="niftyTrendConfInds">
        <label class="rtom-f" title="ON: NIFTY ka bullish/bearish side in confirmation indicators ke LINE COLOUR se decide hoga (green = rising = bullish, red = falling = bearish), last closed candle par (forming bar ignore), aur algo har 1 second me indicators ko scan karega. OFF: har confirmation indicator ki VISIBLE drawn straight line ki latest segment direction se (newest plotted point vs pichhla) - wahi line jo chart par dikh rahi hai.">
          <input type="checkbox" data-set="niftyTrendColor" id="rtNiftyTrendColorCb" style="accent-color:#00d4aa"> Color based trend detection (green/red, 5s scan)
        </label>
        <label class="rtom-f" style="color:#ffd700;font-weight:bold" title="Net-direction strict gate: jab ON ho, Top Gainers (CE) legs sirf tab banenge jab NIFTY ke assigned straight-line confirmation indicators ka NET direction bullish ho, aur Top Losers (PE) legs sirf tab jab net bearish ho. Mismatch ya tie (barabar) = us leg ko strictly block (majority / AI Brain bhi nahi bacha sakta). Yahi gate NIFTY-trend picks par bhi lagta hai. OFF = purana behaviour.">
          <input type="checkbox" data-set="niftyNetStrict" id="rtNiftyNetStrictCb" style="accent-color:#ffd700"> Net direction strict gate (CE=bull, PE=bear)
        </label>
        <label class="rtom-f" style="color:#ffd700;font-weight:bold" title="All-lines-agree strict gate: jab ON ho, Top Gainers (CE) legs sirf tab banenge jab NIFTY ke assigned straight-line confirmation indicators me HAR line bullish (green) ho, aur Top Losers (PE) legs sirf tab jab HAR line bearish (red) ho. Ek bhi line ulti ya mixed = us leg ko strictly block. Yahi gate NIFTY-trend picks par bhi lagta hai. OFF = purana behaviour.">
          <input type="checkbox" data-set="niftyAllAgreeStrict" id="rtNiftyAllAgreeStrictCb" style="accent-color:#ffd700"> All-lines-agree strict gate (every line must confirm)
        </label>
        <label class="rtom-f" style="color:#4fc3f7;font-weight:bold" title="ON: chhote fake breakout / fake reversal / temporary flip ko ignore karke sirf actual trend par nayi entry legi. Entry se pehle underlying candles check hoti hain - persistent same-side closes, structure (HH/HL ya LH/LL), ATR se bada move, decent volume, ADX upar + chop neeche, aur rejection wick. Koi bhi check fail = us fake move par trade block. Running trades par koi asar nahi - sirf nayi entries rukti hain. OFF = purana behaviour.">
          <input type="checkbox" data-set="antiFakeOn" id="rtNiftyAntiFakeCb" class="rt-antifake-cb" style="accent-color:#4fc3f7"> Ignore fake breakouts/reversals (trade real trend only)
        </label>
        <button class="btn-action" id="rtAntiFakeSettingsBtn" style="width:auto;padding:3px 8px;margin:0;font-size:10px" title="Fake-move filter ke thresholds: persistence bars, ATR multiple, volume multiple, ADX min, chop max, cooldown bars.">&#9881; Fake filter</button>
      </div>
      <div id="rtNiftyTrendList" style="display:none;margin-top:4px;font-size:9px;color:#ccc;background:#12122a;border:1px solid #2d2d50;border-radius:4px;padding:6px 8px"></div>

      <div class="rt-engine-row" id="rtIndexTrendRow" style="display:none">
        <b class="rt-cap" style="color:#00d4aa">Indices Trend Following:</b>
        <button class="btn-action" data-toggle="indexTrendOn" id="rtIndexTrendToggle" style="width:auto;padding:3px 10px;margin:0;font-size:10px">Trend Follow: OFF</button>
        <label class="rtom-f" style="color:#ff8a65;font-weight:bold" title="ON: sideways regime (weak ADX + high choppiness + flat slope + tight range) me us index par koi naya trend-follow entry nahi hoga. Running trades SL/target/trail par chalti rahengi.">
          <input type="checkbox" data-set="noTradeSideways" id="rtIndexSidewaysCb" class="rt-sideways-cb" style="accent-color:#ff8a65"> No trade on sideways trend
        </label>
        <label class="rtom-f">Confirm indicators
          <select id="rtIndexTrendConfIndSelect" style="min-width:160px"><option value="">-- pick indicator --</option></select>
        </label>
        <button class="btn-action" id="rtIndexTrendConfIndAdd" style="width:auto;padding:3px 10px;margin:0">Add</button>
        <span id="rtIndexTrendConfIndList" style="font-size:9px;color:#ccc;display:flex;flex-wrap:wrap;gap:4px;align-items:center"></span>
        <input type="hidden" data-list="indexTrendConfInds">
        <span style="font-size:8px;color:#666;flex-basis:100%">Har added index apne chart ke in confirmation indicators se trend tay karega: bullish = us index ka CE call leg, bearish = PE put leg.</span>
        <label class="rtom-f" style="color:#4fc3f7;font-weight:bold" title="ON: chhote fake breakout / fake reversal / temporary flip ko ignore karke sirf actual trend par nayi entry legi. Entry se pehle underlying candles check hoti hain - persistent same-side closes, structure, ATR se bada move, volume, ADX/chop aur rejection wick. Running trades par koi asar nahi - sirf nayi entries rukti hain. OFF = purana behaviour.">
          <input type="checkbox" data-set="antiFakeOn" id="rtIndexAntiFakeCb" class="rt-antifake-cb" style="accent-color:#4fc3f7"> Ignore fake breakouts/reversals (real trend only)
        </label>
        <div id="rtIndexTrendStatus" style="flex-basis:100%;display:none;font-size:9px;color:#ccc;background:#12122a;border:1px solid #2d2d50;border-radius:4px;padding:4px 6px;margin-top:4px"></div>
      </div>

      <div class="rt-engine-row" id="rtCommTrendRow" style="display:none">
        <b class="rt-cap" style="color:#ffb74d">Commodities Trend Following:</b>
        <button class="btn-action" data-toggle="commTrendOn" id="rtCommTrendToggle" style="width:auto;padding:3px 10px;margin:0;font-size:10px">Trend Follow: OFF</button>
        <label class="rtom-f" style="color:#ff8a65;font-weight:bold" title="ON: sideways regime (weak ADX + high choppiness + flat slope + tight range) me us commodity par koi naya trend-follow entry nahi hoga. Running trades SL/target/trail par chalti rahengi.">
          <input type="checkbox" data-set="noTradeSideways" id="rtCommSidewaysCb" class="rt-sideways-cb" style="accent-color:#ff8a65"> No trade on sideways trend
        </label>
        <label class="rtom-f">Confirm indicators
          <select id="rtCommTrendConfIndSelect" style="min-width:160px"><option value="">-- pick indicator --</option></select>
        </label>
        <button class="btn-action" id="rtCommTrendConfIndAdd" style="width:auto;padding:3px 10px;margin:0">Add</button>
        <span id="rtCommTrendConfIndList" style="font-size:9px;color:#ccc;display:flex;flex-wrap:wrap;gap:4px;align-items:center"></span>
        <input type="hidden" data-list="commTrendConfInds">
        <span style="font-size:8px;color:#666;flex-basis:100%">Har added commodity apne chart ke in confirmation indicators se trend tay karega: bullish = CE call leg, bearish = PE put leg.</span>
        <label class="rtom-f" style="color:#4fc3f7;font-weight:bold" title="ON: chhote fake breakout / fake reversal / temporary flip ko ignore karke sirf actual trend par nayi entry legi. Entry se pehle underlying candles check hoti hain - persistent same-side closes, structure, ATR se bada move, volume, ADX/chop aur rejection wick. Running trades par koi asar nahi - sirf nayi entries rukti hain. OFF = purana behaviour.">
          <input type="checkbox" data-set="antiFakeOn" id="rtCommAntiFakeCb" class="rt-antifake-cb" style="accent-color:#4fc3f7"> Ignore fake breakouts/reversals (real trend only)
        </label>
        <div id="rtCommTrendStatus" style="flex-basis:100%;display:none;font-size:9px;color:#ccc;background:#12122a;border:1px solid #2d2d50;border-radius:4px;padding:4px 6px;margin-top:4px"></div>
      </div>

      <div class="rt-engine-row" id="rtMoversTplRow" style="border-color:#4a3a0a;align-items:flex-start">
        <b class="rt-cap" style="color:#b39ddb">Assign AST Template to direction:</b>
        <div style="flex-basis:100%;display:flex;align-items:center;gap:6px;flex-wrap:wrap">
          <label class="rtom-f" style="color:#00d4aa">Top gainers (bullish) &rarr; template
            <select id="rtTplMoverBull" style="min-width:190px"></select>
          </label>
          <button class="btn-action" id="rtAssignMoverBull" style="width:auto;padding:3px 10px;margin:0;font-size:10px">Assign</button>
          <span id="rtMoverBullTplList" style="font-size:9px;color:#ccc;display:inline-flex;flex-wrap:wrap;gap:4px;align-items:center"></span>
        </div>
        <div style="flex-basis:100%;display:flex;align-items:center;gap:6px;flex-wrap:wrap">
          <label class="rtom-f" style="color:#ef5350">Top losers (bearish) &rarr; template
            <select id="rtTplMoverBear" style="min-width:190px"></select>
          </label>
          <button class="btn-action" id="rtAssignMoverBear" style="width:auto;padding:3px 10px;margin:0;font-size:10px">Assign</button>
          <span id="rtMoverBearTplList" style="font-size:9px;color:#ccc;display:inline-flex;flex-wrap:wrap;gap:4px;align-items:center"></span>
        </div>
      </div>

      <div class="rt-engine-row">
        <b class="rt-cap">Commodities (MCX):</b>
        <button class="btn-action" data-toggle="commodityOn" id="rtCommodityToggle" style="width:auto;padding:3px 10px;margin:0;font-size:10px">Commodities: OFF</button>
        <label class="rtom-f">Add <select id="rtCommodityAdd" style="min-width:150px"><option value="">-- pick commodity --</option></select></label>
        <button class="btn-action" id="rtCommodityAddBtn" style="width:auto;padding:3px 10px;margin:0">+ Add</button>
        <button class="btn-action warn" id="rtCommodityClearBtn" style="width:auto;padding:3px 8px;margin:0">Clear</button>
        <span id="rtCommodityStatus" style="font-size:9px;color:#888">Add MCX commodity futures to trade them directly.</span>
        <div id="rtCommodityChips" style="flex-basis:100%;display:flex;flex-wrap:wrap;gap:4px;align-items:center;font-size:9px;color:#ccc"></div>
        <span style="font-size:8px;color:#666;flex-basis:100%">Each commodity trades only its selected CE/PE leg; that leg's indicator filters apply automatically (CE = bullish, PE = bearish). No leg = follow the global side.</span>
        <input type="hidden" data-list="commodityList">
      </div>

      <div class="rt-engine-row" id="rtScannerRemovedRow" style="display:none">
        <b class="rt-cap" style="color:#ef5350">Removed picks:</b>
        <span style="font-size:8px;color:#666">Stocks removed from Top Movers / Top Losers / NIFTY Trend - the engine will not pick or trade them until restored.</span>
        <div id="rtScannerRemovedList" style="flex-basis:100%;display:flex;flex-wrap:wrap;gap:4px;align-items:center;font-size:9px;color:#ccc;margin-top:3px"></div>
        <input type="hidden" data-list="scannerExclude" id="rtScannerExcludeList">
      </div>

      <div class="rt-engine-row" id="rtStrikesSection">
        <b class="rt-cap">Picked Strikes:</b>
        <span id="rtStrikesNote" style="font-size:8px;color:#666">Option strikes the engine picked to execute (auto-shown while Top Movers / NIFTY Trend Follow is ON)</span>
      </div>
      <div id="rtStrikesList" style="display:none;margin-top:2px;font-size:9px;color:#ccc;background:#0e1626;border:1px solid #2d2d50;border-radius:4px;padding:6px 8px"></div>

      <div class="rt-engine-row" id="rtFilterRow" style="align-items:flex-start">
        <b class="rt-cap">Indicator filters:</b>
        <label class="rtom-f" style="flex-basis:100%;color:#ffd54f;font-weight:bold" title="Slope from N bars: ON karne par Indicator filters list (Bullish + Bearish) ki SAARI straight-line filters apna trend aakhri N closed bars se padhti hain - line ki newest value vs N bar pehle ki value. Chhota N (jaise 1) = turant response par zyada whipsaw; bada N = smooth slope par thoda slow. OFF (default) par purana 5-point noise-floor window chalta hai (sub-noise flicker ignore). Yeh gateway dono side ke sabhi indicator filters par ek saath lagu hota hai."><input type="checkbox" data-set="indFilterSlopeOn" id="rtIndSlopeOn" style="accent-color:#ffd54f"> Slope from <input type="number" data-set="indFilterSlopeBars" id="rtIndSlopeBars" min="1" max="200" step="1" style="width:56px" placeholder="1"> bars (all indicator filters)</label>
        <div id="rtEntryPrimaryWrap" style="flex-basis:100%;display:flex;flex-wrap:wrap;gap:10px;align-items:center;border:1px dashed #ffd700;border-radius:4px;padding:5px 8px;background:#1a1608">
          <label class="rtom-f" style="color:#ffd700;font-weight:bold" title="Entry indicator filter (PRIMARY): ON karne par neeche dropdown se add kiye gaye indicator filter PRIMARY entry condition ban jaate hain. Gate SABSE PEHLE inhi primary conditions ko check karta hai - saari primary condition meet honi chahiye, tabhi aage badhta hai. Primary fail = entry turant BLOCK (secondary filters isko override nahi kar sakte). Primary pass hone ke baad hi baaki normal (secondary) indicator filters ka rule - strict AND / AI Brain / majority - chalta hai, yani final entry ke liye PRIMARY + SECONDARY dono meet hone chahiye. OFF = purana behaviour (sirf normal filters).">
            <input type="checkbox" data-set="entryPrimaryOn" id="rtEntryPrimaryCb" style="accent-color:#ffd700"> Entry indicator filter (PRIMARY)
          </label>
          <label class="rtom-f" style="color:#ffd700">Add filter
            <select id="rtEntryPrimarySelect" style="min-width:220px"><option value="">-- pick indicator filter --</option></select>
          </label>
          <button class="btn-action" id="rtEntryPrimaryAdd" style="width:auto;padding:3px 10px;margin:0">Add</button>
          <span id="rtEntryPrimaryList" style="font-size:9px;color:#ccc;display:flex;flex-wrap:wrap;gap:4px;align-items:center"></span>
          <input type="hidden" data-list="entryPrimaryFilters">
          <span id="rtEntryPrimaryInfo" style="font-size:9px;color:#8a7a2a;flex-basis:100%"></span>
        </div>
        <div style="flex-basis:100%;display:flex;flex-wrap:wrap;gap:10px;align-items:center">
          <label class="rtom-f" style="color:#26a69a;font-weight:bold" title="Detect indicator crossover on forming bar: crossover filters (CrossUp/Down, EMA pairs, Supertrend pairs, VWAP, BB/PC, Pane, Straight-Line EMA) live FORMING candle par padhe jayenge. Cross usi tick par milta hai, lekin forming bar repaint hone par cross wapas gayab ho sakta hai. Execution ke exact waqt bhi yahi read dobara check hota hai."><input type="checkbox" data-set="indCrossOnForming" id="rtIndCrossForming" style="accent-color:#26a69a"> Detect indicator crossover on forming bar</label>
          <label class="rtom-f" style="color:#ff8a65;font-weight:bold" title="Detect indicator crossover on closed bar: crossover filters aakhri CLOSED (band ho chuki) candle par padhe jayenge. Isse forming candle ka repaint cross ko gayab nahi kar sakta - cross closed bar par confirm hota hai. Forming checkbox ke saath mutually exclusive hai: ek active hote hi doosra fade + inactive ho jata hai, aur active checkbox ko uncheck karne par dono inactive ho jate hain. Execution ke exact waqt bhi yahi read dobara check hota hai."><input type="checkbox" data-set="indCrossOnClosed" id="rtIndCrossClosed" style="accent-color:#ff8a65"> Detect indicator crossover on closed bar</label>
          <label class="rtom-f" style="color:#ffd54f;font-weight:bold" title="Crossover confirmation bars (dono checkboxes par ek saath lagu): 0 = instant - forming mode me current tick vs pichhla tick, closed mode me aakhri closed bar vs usse pichhli bar. N = cross read bar se N bar pehle hua ho aur tab se lagatar hold kar raha ho; jaise 2 + closed = 2 closed bars pehle cross hua aur ab tak crossed hai. Zyada N = slow confirm par kam whipsaw. Har chart type (Candles / Heikin Ashi / Line / Renko) par same rule.">Crossover <input type="number" data-set="indCrossBars" id="rtIndCrossBars" min="0" max="200" step="1" style="width:56px" placeholder="0"> bars (all crossover filters)</label>
        </div>
        <div style="flex-basis:100%;display:flex;flex-wrap:wrap;gap:10px;align-items:center">
          <label class="rtom-f"><input type="checkbox" data-set="allInOne"> All together (strict AND)</label>
          <label class="rtom-f" style="color:#ff8a65;font-weight:bold" title="Require condition: ON hone par leg sirf tab enter karegi jab uski condition (ticked indicator filters / mode) actual me meet ho. Koi same-side filter tick hi na ho (khali rule) to entry BLOCK (khali = meet nahi). Jo filters tick hain aur unme se condition meet nahi ho rahi to bhi entry BLOCK. Yeh sabhi tabs (Realtime, Realtime 2, Paper Trade 1/2/3) par lagu hota hai. Default OFF."><input type="checkbox" data-set="requireCondition" style="accent-color:#ff8a65"> Require condition (no filter / not met = no trade)</label>
          <label class="rtom-f"><input type="checkbox" data-set="overallDir"> Overall Bullish/Bearish idea</label>
          <label class="rtom-f">AI Brain
            <select data-set="brainMode" style="width:170px"><option value="off">OFF</option><option value="auto">Auto (score)</option></select></label>
          <label class="rtom-f">threshold <input type="number" data-set="brainThreshold" min="5" max="100" step="1" style="width:56px"> %</label>
           <label class="rtom-f" style="color:#26a69a;font-weight:bold" title="Spot candle colour gate: ON karne par spot chart par chalne wali trade (strategy mode ya Indicator-filters mode) sirf tab execute hogi jab spot chart ki live FORMING candle trade ki direction me ho - Top Gainer (bullish / CE) ke liye GREEN candle, Top Loser (bearish / PE) ke liye RED candle. Galat colour par signal skip / held rehta hai (consume nahi hota), aur candle colour badalne par condition meet hone par trade execute ho jati hai. Doji (close = open) par bhi rukta hai. Execution ke exact waqt bhi dobara check hota hai. Manual orders par lagu nahi. Default OFF."><input type="checkbox" data-set="spotCandleGate" style="accent-color:#26a69a"> Spot candle gate (Gainer = green, Loser = red)</label>
           <label class="rtom-f" style="color:#ce93d8;font-weight:bold" title="Trend check before entry: ON karne par indicator-filter condition meet hone ke BAAD, trade place karne se pehle underlying (spot) chart ka CURRENT trend check hota hai. CE leg ke liye bullish trend, PE leg ke liye bearish trend zaroori hai - trend opposite ya sideways ho to entry SKIP (signal held rehta hai, consume nahi hota). Trend read teen independent votes ka composite hai (aapke chune hue trend confirmation indicators ka net + EMA20 slope + price vs EMA20) aur range-regime veto (ADX / choppiness / squeeze) ke saath - yani flat market me koi side enter nahi hoti. Trend-following master switch ON ho ya OFF, yeh pure pre-order filter hamesha chalta hai. Execution ke exact waqt bhi dobara check hota hai. Manual orders par lagu nahi. Default OFF."><input type="checkbox" data-set="entryTrendCheckOn" style="accent-color:#ce93d8"> Trend check before entry (opposite/sideways = skip)</label>
          <label class="rtom-f" style="color:#4fc3f7;font-weight:bold" title="Formed candle only (Closed Bar Compute): ON karne par indicator filters live FORMING candle ki jagah aakhri CLOSED (band ho chuki) candle par padhe jaate hain. Isse forming candle ke andar filter value badalne se aane wala repaint khatam ho jata hai - yani jo filter abhi pass ho raha tha wo next tick par gayab ho kar trade ko galat entry nahi dega. Har chart type (Candles / Heikin Ashi / Line / Renko) par lagu. OFF (default) par pehle jaisa hi live forming bar padha jata hai. Execution ke exact waqt bhi wahi closed/forming read dobara check hota hai. Chart tab me iska display counterpart 'Formed candle only' checkbox hai."><input type="checkbox" data-set="closedBarCompute" style="accent-color:#4fc3f7"> Formed candle only (closed bar)</label>
          <label class="rtom-f" style="color:#ffd54f;font-weight:bold" title="Flip Trigger: ON karne par normal indicator-filter gate (All together strict AND / AI Brain / majority) band ho jata hai aur entry SIRF isi vote se hoti hai. Vote saari straight-line lines (Straight Line Indicators + Visible Straight Line Filter) ke saath OI Trend aur Trend Core ki direction par hota hai - jitni lines is leg ki direction me ho rahi hain (bullish => CE leg, bearish => PE leg) unka % jab diye threshold tak pahunche to entry. Flat ya abhi warm-up line vote me count nahi hoti (na numerator na denominator). Forming candle aur 'Formed candle only' dono mode, aur har chart type (Candles / Heikin Ashi / Line / Renko) par chalta hai. Execution ke exact waqt bhi dobara check hota hai. Default OFF."><input type="checkbox" data-set="flipTriggerOn" style="accent-color:#ffd54f"> Flip Trigger (line flip vote)</label>
          <label class="rtom-f" style="color:#ffd54f;font-weight:bold" title="Flip Trigger threshold: is % ya usse zyada lines jab leg ki direction me ho jaayein tabhi entry hoti hai. Example: 10 lines compute ho rahi hain aur 50 set hai to 5 lines on-side hote hi entry. 0-100. Default 50.">Flip vote <input type="number" data-set="flipTriggerPct" min="0" max="100" step="5" style="width:56px"> %</label>
        </div>
        <div id="rtBrainSummary" style="flex-basis:100%;font-size:9px;color:#b39ddb"></div>
        <div style="flex-basis:100%;display:flex;flex-wrap:wrap;gap:10px;align-items:flex-start">
          ${filterSectionHTML("Bull")}
          <div style="display:flex;flex-direction:column;justify-content:center;align-items:center;gap:4px;padding-top:4px">
            <button type="button" id="rtMirrorFilters" title="Mirror opposite: copy the currently-selected side's filters to the other side" style="background:#1a2332;color:#ffd700;border:1px solid #3d3d6b;border-radius:4px;font-size:11px;font-weight:700;padding:8px 6px;cursor:pointer">&#8646;<span style="display:block;font-size:8px;font-weight:400;color:#aaa;margin-top:2px">Mirror</span></button>
          </div>
          ${filterSectionHTML("Bear")}
        </div>
        <div style="display:none">
          <input type="number" data-set="scMinAgree"><input type="number" data-set="scConfirm"><input type="number" data-set="scStrength">
          <input type="text" data-set="scUpColor"><input type="text" data-set="scDownColor"><input type="text" data-set="scFlatColor"><input type="number" data-set="scLineWidth">
          <input type="number" data-set="supStrength"><input type="number" data-set="supAtrPeriod"><input type="number" data-set="supMinPct"><input type="number" data-set="supTolMult"><input type="number" data-set="supLook"><input type="number" data-set="supFwd"><input type="checkbox" data-set="supFullSpan"><input type="text" data-set="supUpColor"><input type="text" data-set="supDownColor"><input type="number" data-set="supLineWidth">
          <input type="number" data-set="resStrength"><input type="number" data-set="resAtrPeriod"><input type="number" data-set="resMinPct"><input type="number" data-set="resTolMult"><input type="number" data-set="resLook"><input type="number" data-set="resFwd"><input type="checkbox" data-set="resFullSpan"><input type="text" data-set="resUpColor"><input type="text" data-set="resDownColor"><input type="number" data-set="resLineWidth">
        </div>
      </div>

      <div class="rt-engine-row" id="rtMultiPosRow" style="border-color:#ffa726">
        <b class="rt-cap" style="color:#ffa726">Multi Position:</b>
        <label class="rtom-f" style="color:#00d4aa" title="Nayi position har naye signal/bar par: condition jis bar (1min/5min) me meet hoti hai, usi bar turant ek position khulti hai. Condition agle bar tak true rahe to agli bar par nayi position (har bar ek). Ek hi bar me multiple nahi. Same strike par bhi stack hoti rahegi; concurrent positions unlimited."><input type="checkbox" data-set="multiFreshOn" id="rtMultiFreshCb"> Fresh signal par position</label>
        <label class="rtom-f" style="color:#ef5350" title="Condition true rahne tak har scan cycle (~100ms) par nayi position khulti rahegi. Unlimited concurrent positions; orders/sec cap fir bhi lagu rehta hai."><input type="checkbox" data-set="multiAlwaysOn" id="rtMultiAlwaysCb"> Condition true rahne tak baar-baar position</label>
        <span style="font-size:8px;color:#888;flex-basis:100%">Dono OFF = purana rule (ek strategy me ek hi open position). Jo mode active hoga, algo usi ke hisaab se trade karega.</span>
      </div>

      <div class="rt-engine-row" id="rtEntryBrakeRow" style="border-color:#26c6da">
        <b class="rt-cap" style="color:#26c6da">Repeat Entry Brake:</b>
        <label class="rtom-f" style="color:#00d4aa" title="Ek signal = ek entry (latch). Ek entry ke baad usi strategy par nayi entry tabhi hogi jab condition pehle OFF ho jaaye aur latch dobara arm ho. Jab tak signal true rahe, koi repeat entry nahi - isse baar-baar wahi trade khulne se rokti hai."><input type="checkbox" data-set="entryLatchOn" id="rtLatchCb"> Repeat entry latch (ek signal = ek entry)</label>
        <label class="rtom-f" style="color:#888" title="Latch re-arm delay: signal OFF hone ke baad itne second tak lagatar OFF rahe, tabhi naya signal arm hota hai. 0 = pehle OFF scan par hi re-arm.">Re-arm (sec) <input type="number" data-set="entryLatchSecs" id="rtLatchSecs" min="0" step="1" style="width:64px" placeholder="60"></label>
        <label class="rtom-f" style="color:#ef5350" title="Position band hone ke baad usi strategy par itne second tak nayi entry block rehti hai (churn aur charges kam karne ke liye)."><input type="checkbox" data-set="entryCooldownOn" id="rtCooldownCb"> Re-entry cooldown</label>
        <label class="rtom-f" style="color:#888" title="Cooldown length (seconds): position band hone ke baad itni der tak nayi entry nahi. 0 = koi wait nahi.">Cooldown (sec) <input type="number" data-set="entryCooldownSecs" id="rtCooldownSecs" min="0" step="1" style="width:64px" placeholder="60"></label>
        <span style="font-size:8px;color:#888;flex-basis:100%">Latch: ek signal sirf ek baar trade kare jab tak condition dobara OFF se ON na ho. Cooldown: trade band hone ke baad itni der tak nayi entry nahi. Dono default ON (60 sec).</span>
      </div>

      <div class="rt-engine-row" id="rtRunModeRow">
        <label class="rtom-f" style="color:#b39ddb" title="Indicator-filters trading mode: trades the scanner universe when the ticked filters agree.">
          <input type="checkbox" id="rtFilterModeCb" checked> Indicator-filters mode
        </label>
        <button class="btn-action" id="rtFilterPaperBtn" style="width:auto;padding:6px 16px;margin:0;background:#b39ddb;color:#0a0a18;font-weight:700">Place trades based on Indicator filters</button>
        <span style="font-size:9px;color:#888;flex-basis:100%">Indicator-filters mode trades the scanner universe (Top Movers / NIFTY trend / Commodities) when the Indicator filters agree by majority; tick "All together (strict AND)" to require ALL selected filters, or enable AI Brain (score) for a confluence threshold. Paper trades are simulated (no real Dhan orders).</span>
      </div>

      <div class="account-section" id="rtBlockerSection" style="overflow-y:auto;border-top:1px solid #ff5252;margin-top:8px;padding-top:6px">
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:2px 0">
          <h3 style="font-size:11px;color:#ff8a80;text-transform:uppercase;margin:0">Why no trades? (live blockers)</h3>
          <span style="font-size:9px;color:#888">Exact reason the engine is not placing an entry right now - every gate that dropped a signal, newest first, with a count. Margin, latch, indicator gate, premium, rate limit and order errors all show here.</span>
          <span id="rtBlockerCount" style="font-size:8px;color:#888;margin-left:auto">0 reason(s)</span>
          <button class="btn-action" id="rtBlockerRefresh" style="width:auto;padding:1px 8px;margin:0;font-size:8px">Refresh</button>
          <button class="btn-action" id="rtBlockerClear" style="width:auto;padding:1px 8px;margin:0;font-size:8px">Clear</button>
          <button class="btn-action" id="rtDiagCapture" style="width:auto;padding:1px 8px;margin:0;font-size:8px;background:#6a4c93;color:#fff" title="Is waqt ka poora diagnostic snapshot (engine/scan heartbeat, feed, margin, blockers, server log tail) disk par save karo. Installed app me bug hone par yahi file bhejni hai.">Capture now</button>
          <button class="btn-action" id="rtDiagBundle" style="width:auto;padding:1px 8px;margin:0;font-size:8px;background:#8e3b46;color:#fff" title="Ek support bundle file banao: snapshot + startup self-check + latest captures + server log tail. Secrets kabhi include nahi hote (sirf present/absent).">Support bundle</button>
          <span id="rtDiagNote" style="font-size:8px;color:#7ec8ff;flex-basis:100%;word-break:break-all"></span>
        </div>
        <div id="rtBlockerHeadline" style="font-size:9px;color:#ffd54f;padding:3px 6px;background:#1a1205;border:1px solid #4a3a10;border-radius:4px;margin:2px 0 4px">Waiting for engine state...</div>
        <div id="rtBlockerBody" style="max-height:170px;overflow-y:auto;font-size:9px;line-height:1.5;background:#0a0a18;border:1px solid #1e1e40;border-radius:4px;padding:4px 6px;color:#aaa"></div>
      </div>

      <div class="account-section" id="rtCondLogSection" style="overflow-y:auto;border-top:1px solid #ffd700;margin-top:8px;padding-top:6px">
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:2px 0">
          <h3 style="font-size:11px;color:#ffd700;text-transform:uppercase;margin:0">Condition Log</h3>
          <span style="font-size:9px;color:#888">Engine skip/condition events - no option contracts, no tradeable instruments, filter gates and order errors. Newest first.</span>
          <span id="rtCondLogCount" style="font-size:8px;color:#888;margin-left:auto">0 line(s)</span>
          <label style="font-size:9px;color:#888;display:flex;align-items:center;gap:3px">Level
            <select id="rtCondLogLevel" style="width:auto"><option value="all">All</option><option value="warn">Warnings + errors</option><option value="error">Errors only</option></select>
          </label>
          <label style="font-size:9px;color:#888;display:flex;align-items:center;gap:3px"><input type="checkbox" id="rtCondLogPause"> Pause</label>
          <button class="btn-action" id="rtCondLogRefresh" style="width:auto;padding:1px 8px;margin:0;font-size:8px">Refresh</button>
          <button class="btn-action" id="rtCondLogClear" style="width:auto;padding:1px 8px;margin:0;font-size:8px">Clear</button>
        </div>
        <div id="rtCondLogBody" style="max-height:190px;overflow-y:auto;font-size:9px;line-height:1.5;background:#0a0a18;border:1px solid #1e1e40;border-radius:4px;padding:4px 6px;color:#aaa"></div>
      </div>

      <div id="rtEtPanel" style="max-height:170px;overflow-y:auto;font-size:9px;border-top:1px solid #1e1e40;margin-top:6px;padding-top:4px">
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
          <b style="color:#66ccff;font-size:10px">ENTRY TIMING DIAGNOSTICS</b>
          <span style="font-size:8px;color:#666">when the full Bullish/Bearish filter set first held vs when the entry actually placed - delay = late-entry lag. One row per alignment episode.</span>
          <span id="rtEtCount" style="font-size:8px;color:#888;margin-left:auto">0 event(s)</span>
          <button class="btn-action" id="rtEtClear" style="width:auto;padding:1px 8px;margin:0;font-size:8px">Clear</button>
        </div>
        <div id="rtEtBody" style="padding:2px 0;color:#888;line-height:1.5"></div>
      </div>

      <h3 style="font-size:11px;color:#888;text-transform:uppercase;margin:8px 0 4px">Holdings <span style="text-transform:none;color:#666;font-weight:normal">(Dhan demat, T+1 delivery)</span></h3>
      <div class="rt-scroll"><table class="account-table" id="rtHoldings"><thead><tr>
        <th>Symbol</th><th>Qty</th><th>Avg</th><th>LTP</th><th>P&amp;L</th>
      </tr></thead><tbody></tbody></table></div>
    </div>`;

  // Paper trading never places Dhan orders, so the Dhan Order Placement Method
  // cards (Normal / Super / Forever / Slice) are dead UI here. Hide the whole
  // method picker and keep only the universal engine controls + Risk & Quantity
  // row, which drive the simulated engine.
  if (PAPER) {
    const om = rt.querySelector("#rtOrderMethods");
    const head = rt.querySelector("#rtOrderMethodsHead");
    const cards = rt.querySelector("#rtomRow");
    const methodWrap = rt.querySelector("#rtEngMethodWrap");
    if (head) head.style.display = "none";
    if (cards) cards.style.display = "none";
    if (methodWrap) methodWrap.style.display = "none";
    if (om) {
      om.style.border = "none";
      om.style.background = "transparent";
      om.style.padding = "0";
      om.style.margin = "0";
    }
  }

  // Iceberg slicing controls (checkbox + lot-slice-limit + slice qty). These are
  // engine settings, not broker method options, so every engine tab shows them
  // (the commodities-only Paper Trade 3 runs the same indicator-filter gate as
  // Paper Trade 2).
  {
    const sliceWrap = rt.querySelector("#rtEngSliceWrap");
    if (sliceWrap) sliceWrap.style.display = "flex";
  }

  // "Points trail auto from %" is available on every engine, Paper Trade 3
  // included: the derived point trail follows the percentage Trail SL.
  {
    const linkWrap = rt.querySelector("#rtEngPtsFromPctWrap");
    if (linkWrap) linkWrap.style.display = "flex";
  }

  // Paper Trade 2 / Paper Trade 3 / Realtime Trading Engine 2/3 have no stock
  // scanner: hide the whole Top Movers stock system, the NIFTY Trend Following
  // section, the F&O-stock routing selects and the NIFTY BB%b (Pane). Hidden
  // (not removed) so the shared refresh/render code that reads these nodes keeps
  // working; the backend engine also refuses to arm any of them.
  //
  //   INDICES_ONLY      (Paper Trade 2) : indices picker + Indices Trend Follow
  //   COMMODITIES_ONLY  (Paper Trade 3) : commodity picker + Commodities Trend
  //   neither           (Realtime 2/3)  : combined Indices + Commodities row
  if (PAPER2) {
    const hide = (el) => { if (el) el.style.display = "none"; };
    const show = (el) => { if (el) el.style.display = ""; };
    // F&O-stock expiry-day guard belongs to the Realtime + Paper Trade 1 tabs only.
    hide(rt.querySelector("#rtFnoExpiryRow"));
    // Liquid F&O scanner (and its lot policy) exists only where the stock
    // scanner does: Realtime Trading Engine 1 and Paper Trade 1. Realtime 2/3
    // and Paper Trade 2/3 have no Liquid F&O universe at all, so the whole row,
    // its readout and the lot policy are hidden here (the backend also refuses
    // to arm the scanner or its lot policy on those engines).
    hide(rt.querySelector("#rtLiquidRow"));
    hide(rt.querySelector("#rtLiquidList"));
    hide(rt.querySelector("#rtLiquidLotWrap"));
    const hideRowOf = (id) => {
      const el = rt.querySelector("#" + id);
      if (el) hide(el.closest(".rt-engine-row"));
    };
    const hideSet = (key) => {
      const inp = rt.querySelector('[data-set="' + key + '"]');
      if (inp) hide(inp.closest("label"));
    };
    // Top Movers stock controls: gainer/loser counts + the stock list, the AST
    // template assignment row and the removed-picks row.
    hideSet("moversGainers");
    hideSet("moversLosers");
    hide(rt.querySelector("#rtMoversList"));
    hide(rt.querySelector("#rtMoversTplRow"));
    hide(rt.querySelector("#rtScannerRemovedRow"));
    // NIFTY Trend Following + its picked-strikes list.
    hideRowOf("rtNiftyTrendToggle");
    hide(rt.querySelector("#rtNiftyTrendList"));
    // F&O-stock routing selects (strategy run-in / trade-in).
    hideSet("runFno");
    hideSet("tradeInFno");
    const moversRow = rt.querySelector("#rtMoversToggle")?.closest(".rt-engine-row");
    const commRow = rt.querySelector("#rtCommodityToggle")?.closest(".rt-engine-row");
    const indexTrendRow = rt.querySelector("#rtIndexTrendRow");
    const commTrendRow = rt.querySelector("#rtCommTrendRow");
    if (INDICES_ONLY) {
      // Paper Trade 2: indices only. The commodity row and the Commodities
      // Trend Following section are removed outright (hidden, so the shared
      // picker/chip code and the DOM settings write keep working).
      if (moversRow) {
        const cap = moversRow.querySelector("b.rt-cap");
        if (cap) cap.textContent = "Indices:";
      }
      if (commRow) hide(commRow);
      if (commTrendRow) hide(commTrendRow);
      show(indexTrendRow);
      hideSet("runComm");
      hideSet("tradeInComm");
    } else if (COMMODITIES_ONLY) {
      // Paper Trade 3: commodities only. Every index / stock section is removed.
      if (moversRow) hide(moversRow);
      if (indexTrendRow) hide(indexTrendRow);
      hideSet("runIndex");
      hideSet("tradeInIndex");
      show(commRow);
      show(commTrendRow);
    } else {
      // Realtime Trading Engine 2/3: one combined Indices & Commodities row
      // driven by the single movers master toggle.
      if (moversRow) {
        const cap = moversRow.querySelector("b.rt-cap");
        if (cap) cap.textContent = "Indices & Commodities:";
        Array.from(moversRow.querySelectorAll("span")).forEach((sp) => {
          if (/Each index trades only/.test(sp.textContent || "")) {
            sp.textContent =
              "Har added index/commodity ke liye CE/PE leg dropdown se leg chuno - trade usi leg me execute hoga (CE = bullish filters, PE = bearish filters). No leg = global side follow.";
          }
        });
        if (commRow) {
          const sep = document.createElement("b");
          sep.className = "rt-cap";
          sep.textContent = "Commodities:";
          const nodes = [sep];
          const addLabel = commRow.querySelector("#rtCommodityAdd")?.closest("label");
          if (addLabel) nodes.push(addLabel);
          const addBtn = commRow.querySelector("#rtCommodityAddBtn");
          if (addBtn) nodes.push(addBtn);
          const clearBtn = commRow.querySelector("#rtCommodityClearBtn");
          if (clearBtn) nodes.push(clearBtn);
          const chips = commRow.querySelector("#rtCommodityChips");
          if (chips) {
            chips.style.flexBasis = "100%";
            nodes.push(chips);
          }
          const listInput = commRow.querySelector('[data-list="commodityList"]');
          if (listInput) nodes.push(listInput);
          nodes.forEach((n) => moversRow.appendChild(n));
          hide(commRow);
        }
      }
      show(indexTrendRow);
      show(commTrendRow);
    }
  }

  // Realtime Trading Engine + Paper Trade 1: the Indices (Top-Movers) picker and
  // the whole Commodities (MCX) system are removed, and the run-in / trade-in
  // sections keep only the F&O-stocks routing. The controls are hidden (not
  // deleted) so the shared picker/chip/render routines keep working; the backend
  // also strips these two engines, so a saved file or a settings POST can never
  // arm them.
  if (!PAPER2 && !PAPER3) {
    const hide = (el) => { if (el) el.style.display = "none"; };
    const hideSet = (key) => {
      const inp = rt.querySelector('[data-set="' + key + '"]');
      if (inp) hide(inp.closest("label"));
    };
    // Commodities (MCX): hide the whole row.
    hide(rt.querySelector("#rtCommodityToggle")?.closest(".rt-engine-row"));
    // Top Movers: hide the per-index CE/PE leg picker + its hint/list; keep the
    // gainers/losers counts and the Top Movers master. Fix the row caption.
    ["rtMoversIndicesSelect", "rtMoversIndicesAdd", "rtMoversIndicesList"].forEach((id) => {
      hide(rt.querySelector("#" + id));
    });
    const miHidden = rt.querySelector('input[data-list="moversIndices"]');
    if (miHidden) {
      hide(miHidden.previousElementSibling);
      hide(miHidden);
    }
    const moversRow = rt.querySelector("#rtMoversToggle")?.closest(".rt-engine-row");
    if (moversRow) {
      const cap = moversRow.querySelector("b.rt-cap");
      if (cap) cap.textContent = "Top Gainers / Losers:";
    }
    // Run-in / trade-in keep only the F&O-stocks select.
    hideSet("runIndex");
    hideSet("runComm");
    hideSet("tradeInIndex");
    hideSet("tradeInComm");
  }

  // Equity Indicator Based Trade (Paper Trade 4): a cash-equity-only book. Hide
  // every option-specific control (the whole Strike row, manual strike select,
  // premium chart / premium-chart-only, run-in / trade-in routing, the Picked
  // Strikes readout) plus the Top Movers / NIFTY trend / indices / commodities
  // sections and the Bearish filter column. Show the liquid cash-equity universe
  // and keep ONLY the Bullish indicator filters armed. Everything is hidden (not
  // removed) so shared refresh/render code that reads these nodes keeps working;
  // the backend also strips them, so a saved file or POST can never re-arm them.
  if (EQUITY_ONLY) {
    const hide = (el) => { if (el) el.style.display = "none"; };
    const hideSet = (key) => {
      const inp = rt.querySelector('[data-set="' + key + '"]');
      if (inp) hide(inp.closest("label"));
    };
    const hideRow = (id) => {
      const el = rt.querySelector("#" + id);
      if (el) hide(el.closest(".rt-engine-row"));
    };
    // Whole option Strike row, manual-strike row and premium-only row.
    const optRow = rt.querySelector('[data-set="optionSide"]')?.closest(".rt-engine-row");
    if (optRow) hide(optRow);
    ["optionSide", "strikeMode", "strikeCount", "onlyPositive", "fallbackOppositeSide", "fastestRising", "fastestCount"].forEach(hideSet);
    hideRow("rtManualStrikeRow");
    hideSet("manualStrikesEnabled");
    hideSet("premiumOnly");
    // Run-in / trade-in routing rows are meaningless for a single spot chart.
    hideRow("rtRunInRow");
    hideRow("rtTradeInRow");
    // Picked Strikes (premium) column in the Running panel.
    hide(rt.querySelector("#rtRunStrikes")?.closest("div[style*='flex']"));
    // Top Movers / NIFTY trend / indices / commodities sections.
    hide(rt.querySelector("#rtMoversToggle")?.closest(".rt-engine-row"));
    hide(rt.querySelector("#rtMoversList"));
    hide(rt.querySelector("#rtScannerRemovedRow"));
    hideRow("rtNiftyTrendToggle");
    hide(rt.querySelector("#rtNiftyTrendList"));
    hide(rt.querySelector("#rtIndexTrendRow"));
    hide(rt.querySelector("#rtCommTrendRow"));
    hideRow("rtFnoExpiryRow");
    hideRow("rtStrikesSection");
    // Bearish filter column: this book is bullish-only.
    hide(rt.querySelector("#rtFilterSectionBear"));
    const mirror = rt.querySelector("#rtMirrorFilters");
    if (mirror) hide(mirror);
    // Relabel and keep the liquid row: it is this book's cash-equity universe.
    const liquidRow = rt.querySelector("#rtLiquidRow");
    if (liquidRow) {
      const cap = liquidRow.querySelector("b.rt-cap");
      if (cap) cap.textContent = "Scanned Liquid Equity Stocks:";
      const tog = rt.querySelector("#rtLiquidToggle");
      if (tog) tog.textContent = tog.textContent.replace(/Liquid F&O/, "Liquid Equity");
      const lot = rt.querySelector("#rtLiquidLotWrap");
      if (lot) hide(lot);
      // Equity scan controls: show the daily-scan trade count, scan time,
      // turnover floor, "Scan now" and the scanned-stocks dropdown.
      ["rtEqTradeCountRow", "rtEqScanTimeRow", "rtEqMinTurnoverRow", "rtEqScanNow", "rtEqScanPickRow", "rtEqScanInfo"].forEach((id) => {
        const el = rt.querySelector("#" + id);
        if (el) el.style.display = "";
      });
      // Top gainers / Top losers do NOT exist for cash equity: hide those inputs.
      ["liquidGainers", "liquidLosers"].forEach(hideSet);
      Array.from(liquidRow.querySelectorAll("span")).forEach((sp) => {
        if (/Only the liquid index basket/.test(sp.textContent || "")) {
          sp.textContent =
            "Daily liquidity scan of the full NSE cash-equity universe (turnover x growth), ranked; the algo trades only the top N most-liquid names you set above. Sirf BULLISH indicator filters entry gate karte hain; har pick spot/delivery BUY hota hai - koi strike, option type ya premium nahi. Remove (x) any stock to keep the engine off it.";
        }
      });
      const gl = rt.querySelector("#rtLiquidGainerSelect")?.closest("label");
      const ll = rt.querySelector("#rtLiquidLoserSelect")?.closest("label");
      [gl, ll].forEach((l) => hide(l));
    }
    const title = rt.querySelector(".rt-strip .rt-title");
    if (title) title.textContent = "Equity Indicator Based Trade";
  }


  // Auto Experiment: the "Auto Trading (trend + SL + trail + target)" preset is
  // removed from that tab only. Hide the control (the shared refresh/render code
  // that reads the node keeps working) and force the engine's stored value off
  // so a leftover saved file can never arm it. Every other engine is untouched.
  if (AUTO_TRADE_REMOVED) {
    const wrap = rt.querySelector("#rtAutoTradeWrap");
    if (wrap) wrap.style.display = "none";
  }
  // AI Trader tabs: hide the Auto Trading toggle (the brain is always on in the
  // backend) and show a small note explaining the engine is autonomous.
  if (AI_TRADER) {
    const wrap = rt.querySelector("#rtAutoTradeWrap");
    if (wrap) {
      wrap.style.display = "none";
      wrap.insertAdjacentHTML(
        "afterend",
        '<span class="rt-pill" style="color:#7c4dff;border:1px solid #4527a0;background:#160f2e;border-radius:4px;padding:3px 9px;font-weight:700" ' +
          'title="AI Trader autonomous engine: multi-factor brain har cycle me regime + filter ranking + volatility gate recompute karta hai aur khud entry/exit leta hai. Auto Trading preset hamesha ON rehta hai.">' +
          "AI Brain: always ON · autonomous</span>"
      );
    }
  }

  // NIFTY BB%b (Pane): shown on EVERY engine tab (Realtime Trading Engine,
  // Paper Trade, Paper Trade 3) right below the NIFTY Trend Following
  // section. There is no candlestick chart here, so the normal BB%b
  // (Pane) indicator is drawn on a small canvas fed by the engine on NIFTY spot
  // at the selected 1min/5min timeframe. The series is fetched from
  // `/api/rt/nifty-pane`, which each paper frame routes to its own
  // `/api/paper*` engine, so every tab shows its own saved zones/mode. Paper
  // Trade 2 removes this section entirely, so it is never mounted there.
  if (!PAPER2 && !EQUITY_ONLY) mountNiftyBbPctPane();

  // Shared margin pool: one total margin for the whole app, with a per-tab
  // allotment (hard cap). Mounted at the top of every engine pane so the same
  // control drives this tab's own engine id.
  mountSharedMargin(rt, realtimeEngineId());

  buildMethodCards();
  wire();
}

// ---------------------------------------------------------------------------
// Order Placement Method cards
// ---------------------------------------------------------------------------
function methodMeta(key) {
  return METHODS.find((m) => m.key === key) || METHODS[0];
}

// One option control for the active method's `order_cfg` (rendered inside the
// engine Risk & Quantity row, never duplicated on the method card).
function optField(x) {
  const f = `data-mfield="${x.k}"`;
  if (x.type === "select") {
    return `<label class="rtom-f"><span>${esc(x.label)}</span>
      <select class="rtEng-opt" ${f}>${x.opts.map((o) => `<option value="${o[0]}">${o[1]}</option>`).join("")}</select></label>`;
  }
  if (x.type === "check") {
    return `<label class="rtom-f" style="color:#e0e0e0"><input type="checkbox" class="rtEng-opt" ${f}> ${esc(x.label)}
      <span style="font-size:10px;color:#888">(super ko iceberg jaisa; off = slice band)</span></label>`;
  }
  return `<label class="rtom-f"><span>${esc(x.label)}</span>
    <input type="number" class="rtEng-opt" ${f} min="0" step="0.05" style="width:78px"></label>`;
}

// Method cards are NAME + enable checkbox only. Exactly one method is enabled
// (radio-like); every order option lives in the engine's single Risk & Quantity
// row and is written to the enabled method's `order_cfg`, which `open_entry` reads.
function methodChipHTML(m) {
  const isSel = m.key === activeMethod;
  return `<label class="rtom-chip${isSel ? " sel" : ""}" data-key="${m.key}" title="${esc(m.desc)} (${esc(m.sdk)})"
    style="display:flex;align-items:center;gap:7px;font-size:13px;font-weight:700;cursor:pointer;
    border-radius:5px;padding:6px 12px">
    <input type="checkbox" class="rtom-enable" data-key="${m.key}"${isSel ? " checked" : ""} style="accent-color:#b39ddb;width:16px;height:16px"> ${esc(m.name)}
    <span style="font-size:10px;color:#777;font-weight:400">${esc(m.sdk)}</span></label>`;
}

function buildMethodCards() {
  const row = document.getElementById("rtomRow");
  if (row) {
    row.innerHTML = METHODS.map(methodChipHTML).join("");
    row.querySelectorAll(".rtom-enable").forEach((cb) => {
      cb.addEventListener("change", () => {
        const k = cb.getAttribute("data-key");
        if (k) {
          activeMethod = k;
          API.method({ method: k });
        }
        buildMethodCards();
      });
    });
  }
  renderMethodOpts();
  applyChartPrice();
  syncEngineRisk();
  syncInterlocks();
  // The trail owner depends on the active method, so re-state it on every change.
  renderBrokerRow();
}

// Per-broker routing for THIS engine. The catalogue + which brokers are ON and
// fully configured come from the shared Broker Settings tab; the tick here is
// the engine's own "send orders to this broker" switch. Paper books never send a
// real order, so the row is hidden there.
//
// Trail ownership is stated up front: when every ticked broker runs a native
// trailing stop for the active method, the broker owns the trail and the engine
// holds only the fixed stop/target. A mixed selection falls back to the engine
// trail so one position never carries two ratchets.
function renderBrokerRow() {
  const row = document.getElementById("rtBrokerRow");
  if (!row) return;
  const chipsEl = document.getElementById("rtBrokerChips");
  const noteEl = document.getElementById("rtBrokerNote");
  const labelEl = document.getElementById("rtBrokerLabel");
  if (PAPER) {
    row.style.display = "none";
    return;
  }
  row.style.display = "flex";
  const b = (STATE && STATE.brokers) || {};
  const catalog = (b.catalog && b.catalog.brokers) || [];
  if (!catalog.length) {
    chipsEl.innerHTML = `<span style="font-size:11px;color:#888">Broker list load ho rahi hai...</span>`;
    noteEl.textContent = "";
    return;
  }
  const usable = new Set((b.usable || []).map((x) => String(x).toLowerCase()));
  const map = b.map || {};
  const touched = !!b.settingsTouched;
  chipsEl.innerHTML = catalog.map((br) => {
    const acct = br.account || {};
    const on = usable.has(br.id);
    // An untouched settings store means Dhan keeps its legacy route even though
    // the shared store has not been filled in yet.
    const legacyDhan = !touched && br.id === "dhan";
    const ticked = map[br.id] != null ? !!map[br.id] : legacyDhan;
    const configured = !!acct.configured;
    const enabled = !!acct.enabled;
    const ready = !!br.adapterReady;
    let why = "";
    if (!configured) why = "Broker Settings me API keys bharo";
    else if (!enabled) why = "Broker Settings me switch ON karo";
    else if (!ready) why = "Order adapter is build me pending hai";
    else if (!on) why = "Broker Settings me ON + configured nahi";
    const disabled = !on && !legacyDhan;
    const style = disabled
      ? "opacity:.45;filter:grayscale(.5)"
      : (ticked ? "border-color:#4a2d7e;background:#161028" : "");
    return `<label class="rtom-chip${ticked && !disabled ? " sel" : ""}" data-broker="${esc(br.id)}"
      title="${esc(why || br.note || "")}${br.nativeTrail ? " · native trail" : " · engine trail"}"
      style="display:flex;align-items:center;gap:7px;font-size:12px;font-weight:700;cursor:${disabled ? "not-allowed" : "pointer"};
      border-radius:5px;padding:5px 10px;${style}">
      <input type="checkbox" class="rtom-broker" data-broker="${esc(br.id)}"${ticked ? " checked" : ""}${disabled ? " disabled" : ""}
        style="accent-color:#66ccff;width:15px;height:15px"> ${esc(br.name)}
      <span style="font-size:9px;color:${br.nativeTrail ? "#00d4aa" : "#ffb74d"};font-weight:400">
        ${br.nativeTrail ? "native trail" : "engine trail"}</span>
    </label>`;
  }).join("");
  chipsEl.querySelectorAll(".rtom-broker").forEach((cb) => {
    cb.addEventListener("change", async () => {
      const id = cb.getAttribute("data-broker");
      if (!id) return;
      await API.brokersSet({ id, enabled: !!cb.checked });
      refresh();
    });
  });
  // State the trail owner for the CURRENT method so the operator can see which
  // side will ratchet the stop.
  const resolved = (b.resolved || []).map((x) => String(x).toLowerCase());
  const methodNative = (METHODS.find((m) => m.key === activeMethod) || {});
  // Mirrors `crates/server/src/brokers.rs`: only the Super Order method carries
  // a native broker-side trail. Forever/GTT has no trailing leg, so the engine
  // keeps the trail there.
  const nativeMethods = new Set(["super"]);
  // The broker can only own the trail when a jump is actually configured; with
  // no jump it would trail nothing, so the engine keeps its own trail (this
  // mirrors `brokers::broker_owns_trail` on the server).
  const u = (STATE && STATE.settings) || {};
  const jumpOn =
    (!!u.trailPointsFromPct && !!u.manualTrailSl && num(u.manualTrailSlPct) > 0) ||
    (!!u.manualPointTrailSl && num(u.manualPointTrailSlPoints) > 0);
  const brokerOwns = jumpOn && resolved.length > 0 && nativeMethods.has(activeMethod)
    && resolved.every((id) => {
      const br = catalog.find((c) => c.id === id);
      return br && br.nativeTrail;
    });
  if (labelEl) labelEl.textContent = "Send orders to:";
  if (!resolved.length) {
    noteEl.innerHTML = touched || map && Object.keys(map).length
      ? `<span style="color:#ffb0b0">Koi broker deliverable nahi - upar tick karo ya Broker Settings me keys bharo.</span>`
      : `<span style="color:#888">Dhan (default route)</span>`;
  } else {
    noteEl.innerHTML = brokerOwns
      ? `<span style="color:#00d4aa">Trail: broker-side (${esc(methodNative.name || activeMethod)})</span>`
      : `<span style="color:#ffb74d">Trail: engine-side (${esc(methodNative.name || activeMethod)})</span>`;
  }
}

// Render the active method's own options (order type, GTT leg, slice qty ...)
// into the one Risk & Quantity row.
function renderMethodOpts() {
  const box = document.getElementById("rtEngMethodOpts");
  if (!box) return;
  const m = methodMeta(activeMethod);
  const nameEl = document.getElementById("rtEngMethodName");
  if (nameEl) nameEl.textContent = m.name;
  box.innerHTML = m.extras.map(optField).join("");
  box.querySelectorAll(".rtEng-opt").forEach((e) => {
    e.addEventListener("input", () => onMethodOptInput(e));
    e.addEventListener("change", () => onMethodOptInput(e));
  });
  box.querySelectorAll(".rtEng-price").forEach((p) => p.addEventListener("input", () => onMethodOptInput(p)));
  syncMethodOpts();
}

// Auto-slice (Super) gates the slice-qty box, exactly like the old card.
function applySliceFade() {
  const box = document.getElementById("rtEngMethodOpts");
  if (!box) return;
  const chk = box.querySelector('[data-mfield="autoSlice"]');
  const qty = box.querySelector('[data-mfield="sliceQty"]');
  if (!qty) return;
  const on = !!(chk && chk.checked);
  qty.disabled = !on;
  const wrap = qty.closest("label");
  if (wrap) wrap.style.opacity = on ? "1" : ".35";
}

// order_cfg of the active method -> the Risk & Quantity method-option controls.
function syncMethodOpts() {
  const box = document.getElementById("rtEngMethodOpts");
  if (!box) return;
  const cfg = currentMethodCfg(activeMethod);
  const ae = document.activeElement;
  const p = box.querySelector(".rtEng-price");
  if (p && ae !== p && !num(p.value) && num(cfg.price) > 0) {
    p.value = cfg.price;
    priceAuto[activeMethod] = false;
  }
  box.querySelectorAll(".rtEng-opt").forEach((e) => {
    if (e === ae) return;
    const f = e.getAttribute("data-mfield");
    const v = cfg[f];
    if (v == null) return;
    if (e.type === "checkbox") e.checked = isTruthy(v);
    else e.value = v;
  });
  applySliceFade();
}

// Risk & Quantity method-option control -> active method's order_cfg.
function onMethodOptInput(e) {
  const key = activeMethod;
  const cfg = Object.assign({}, currentMethodCfg(key));
  if (e.classList.contains("rtEng-price")) {
    priceAuto[key] = false;
    cfg.price = num(e.value);
    updateMargin();
    API.method({ method: key, cfg }).then(refresh);
    return;
  }
  const f = e.getAttribute("data-mfield");
  if (!f) return;
  cfg[f] = e.type === "checkbox" ? !!e.checked : e.value;
  if (f === "autoSlice") applySliceFade();
  updateMargin();
  API.method({ method: key, cfg }).then(refresh);
}

// The active method's option controls live in the Risk & Quantity row, so a
// snapshot refresh just re-syncs them (+ chart price + engine risk).
function syncCards() {
  syncMethodOpts();
  applyChartPrice();
  syncEngineRisk();
}

// Engine-level Risk & Quantity row (SL / Trail SL / Lot size /
// Lots / Auto-Lot). Single source of truth is `settings` (+ top-level `autoLots`),
// exactly the fields `open_entry` reads, so what the operator sets here is what
// every engine entry gets.
function syncEngineRisk() {
  const u = (STATE && STATE.settings) || {};
  const ae = document.activeElement;
  const setCb = (id, v) => { const e = document.getElementById(id); if (e && ae !== e) e.checked = !!v; };
  const setNum = (id, v) => { const e = document.getElementById(id); if (e && ae !== e) e.value = num(v) > 0 ? v : ""; };
  setCb("rtEngSl", u.manualSl);
  setNum("rtEngSlPct", u.manualSlPct);
  setCb("rtEngTrailSl", u.manualTrailSl);
  setNum("rtEngTrailSlPct", u.manualTrailSlPct);
  setCb("rtEngNetTp", u.netTpOn);
  setNum("rtEngNetTpMin", u.netTpMin);
  setNum("rtEngNetTpGiveback", u.netTpGivebackPct);
  setCb("rtEngPointTrailSl", u.manualPointTrailSl);
  setNum("rtEngPointTrailSlPts", u.manualPointTrailSlPoints);
  setCb("rtEngPtsFromPct", u.trailPointsFromPct);
  // "Pts = % (auto)": when linked, the point trail is DERIVED per strike from the
  // Trail SL % (points = entry premium x % / 100) and rides the real order only.
  // Surface that: tick the point box and mark the value field "auto" so the typed
  // value is never mistaken for the active point trail.
  const linkCbEl = document.getElementById("rtEngPtsFromPct");
  const linkOn = !!(linkCbEl && linkCbEl.checked);
  const ptrCbEl = document.getElementById("rtEngPointTrailSl");
  if (linkOn && ptrCbEl && ae !== ptrCbEl) ptrCbEl.checked = true;
  const ptrPtsEl = document.getElementById("rtEngPointTrailSlPts");
  if (ptrPtsEl) ptrPtsEl.placeholder = linkOn ? "auto" : "pts";
  setNum("rtEngLot", u.lotSize);
  const auto = !!(STATE && STATE.autoLots);
  const lotsEl = document.getElementById("rtEngLots");
  if (lotsEl) {
    if (ae !== lotsEl) lotsEl.value = u.lots != null ? u.lots : 1;
    lotsEl.readOnly = auto;
    lotsEl.style.opacity = auto ? ".7" : "1";
    lotsEl.title = auto ? "Auto-Lot ON - engine min(volume %, OI %, margin) se lots decide karega" : "";
  }
  const al = document.getElementById("rtEngAutoLots");
  if (al) {
    if (ae !== al) al.checked = auto;
    al.parentElement && (al.parentElement.style.opacity = "1");
  }
  // Independent Auto-Lot liquidity caps (volume% and OI%). Only meaningful while
  // Auto-Lot is ON, so they fade + lock when it is off.
  const volPctEl = document.getElementById("rtEngAutoLotVolPct");
  if (volPctEl && ae !== volPctEl) volPctEl.value = u.autoLotVolumePct != null ? num(u.autoLotVolumePct) : 1;
  const oiPctEl = document.getElementById("rtEngAutoLotOiPct");
  if (oiPctEl && ae !== oiPctEl) oiPctEl.value = u.autoLotOiPct != null ? num(u.autoLotOiPct) : 1;
  [volPctEl, oiPctEl].forEach((e) => {
    if (!e) return;
    e.disabled = !auto;
    e.style.opacity = auto ? "1" : ".5";
  });
  const info = document.getElementById("rtEngAutoLotsInfo");
  if (info) {
    const vp = u.autoLotVolumePct != null ? num(u.autoLotVolumePct) : 1;
    const op = u.autoLotOiPct != null ? num(u.autoLotOiPct) : 1;
    info.textContent = auto
      ? `ON - min(volume ${vp}%, OI ${op}%, margin) se per-trade lots auto select`
      : "";
  }
  // Paper-only iceberg slicing (engine settings). Kept in step with the engine
  // on every snapshot; the controls are inert until the checkbox is ticked.
  const sliceOn = !!u.sliceOn;
  const sliceCb = document.getElementById("rtEngSliceOn");
  if (sliceCb && ae !== sliceCb) sliceCb.checked = sliceOn;
  const sliceLimEl = document.getElementById("rtEngSliceLotLimit");
  if (sliceLimEl && ae !== sliceLimEl) sliceLimEl.value = u.sliceLotLimit != null ? num(u.sliceLotLimit) : 0;
  const sliceQtyEl = document.getElementById("rtEngSliceLots");
  if (sliceQtyEl && ae !== sliceQtyEl) sliceQtyEl.value = u.sliceLots != null ? num(u.sliceLots) : 0;
  const sliceInfo = document.getElementById("rtEngSliceInfo");
  if (sliceInfo) {
    const lim = u.sliceLotLimit != null ? num(u.sliceLotLimit) : 0;
    const sq = u.sliceLots != null ? num(u.sliceLots) : 0;
    sliceInfo.textContent = !sliceOn
      ? ""
      : sq > 0
        ? `ON - ${lim > 0 ? lim + "+ lots par " : ""}${sq}-lot ke slices`
        : "Slice qty (lots) 0 hai - slicing band";
  }
  [sliceLimEl, sliceQtyEl].forEach((e) => {
    if (!e) return;
    e.disabled = !sliceOn;
    e.style.opacity = sliceOn ? "1" : ".5";
  });

  // Liquid F&O lot policy: a dedicated Auto-Lot + iceberg config that only this
  // scanner's legs read (`liquid_leg` in the engine). Same fade/lock rules as the
  // global block so an OFF switch visibly disables its inputs.
  const lqAuto = !!u.liquidAutoLotOn;
  const lqAutoCb = document.getElementById("rtLiquidAutoLots");
  if (lqAutoCb && ae !== lqAutoCb) lqAutoCb.checked = lqAuto;
  const lqVolEl = document.getElementById("rtLiquidAutoLotVolPct");
  if (lqVolEl && ae !== lqVolEl) lqVolEl.value = u.liquidAutoLotVolumePct != null ? num(u.liquidAutoLotVolumePct) : 1;
  const lqOiEl = document.getElementById("rtLiquidAutoLotOiPct");
  if (lqOiEl && ae !== lqOiEl) lqOiEl.value = u.liquidAutoLotOiPct != null ? num(u.liquidAutoLotOiPct) : 1;
  [lqVolEl, lqOiEl].forEach((e) => {
    if (!e) return;
    e.disabled = !lqAuto;
    e.style.opacity = lqAuto ? "1" : ".5";
  });
  const lqAutoInfo = document.getElementById("rtLiquidAutoLotsInfo");
  if (lqAutoInfo) {
    const vp = u.liquidAutoLotVolumePct != null ? num(u.liquidAutoLotVolumePct) : 1;
    const op = u.liquidAutoLotOiPct != null ? num(u.liquidAutoLotOiPct) : 1;
    lqAutoInfo.textContent = lqAuto
      ? `ON - min(volume ${vp}%, OI ${op}%, margin) se liquid legs ke per-trade lots auto select`
      : "OFF - liquid stocks global Auto-Lot (Vol%/OI%) use karenge, Top Movers jaisa";
  }
  const lqSliceOn = !!u.liquidSliceOn;
  const lqSliceCb = document.getElementById("rtLiquidSliceOn");
  if (lqSliceCb && ae !== lqSliceCb) lqSliceCb.checked = lqSliceOn;
  const lqLimEl = document.getElementById("rtLiquidSliceLotLimit");
  if (lqLimEl && ae !== lqLimEl) lqLimEl.value = u.liquidSliceLotLimit != null ? num(u.liquidSliceLotLimit) : 0;
  const lqQtyEl = document.getElementById("rtLiquidSliceLots");
  if (lqQtyEl && ae !== lqQtyEl) lqQtyEl.value = u.liquidSliceLots != null ? num(u.liquidSliceLots) : 0;
  const lqSliceInfo = document.getElementById("rtLiquidSliceInfo");
  if (lqSliceInfo) {
    const lim = u.liquidSliceLotLimit != null ? num(u.liquidSliceLotLimit) : 0;
    const sq = u.liquidSliceLots != null ? num(u.liquidSliceLots) : 0;
    lqSliceInfo.textContent = !lqSliceOn
      ? "OFF - liquid stocks global Slicing use karenge (band ho to normal Lots qty)"
      : sq > 0
        ? `ON - ${lim > 0 ? lim + "+ lots par " : ""}${sq}-lot ke liquid slices`
        : "Slice qty (lots) 0 hai - liquid slicing band";
  }
  [lqLimEl, lqQtyEl].forEach((e) => {
    if (!e) return;
    e.disabled = !lqSliceOn;
    e.style.opacity = lqSliceOn ? "1" : ".5";
  });
}

function currentMethodCfg(key) {
  const cfg = (STATE && STATE.orderCfg) || {};
  return cfg[key] || {};
}

function updateMargin() {
  const avail = availableMargin();
  const ha = document.getElementById("rtomMarginAvail");
  if (ha) ha.textContent = avail == null ? "--" : fmtMoney(avail);
  const u = (STATE && STATE.settings) || {};
  const lotEl = document.getElementById("rtEngLot");
  let lot = num(u.lotSize);
  if (!(lot > 0) && lotEl) lot = num(lotEl.value);
  if (!(lot > 0)) lot = chartLot;
  const lotsEl = document.getElementById("rtEngLots");
  const lots = num(lotsEl && lotsEl.value) || 1;
  const sel = chartSelection();
  const pEl = document.querySelector("#rtEngMethodOpts .rtEng-price");
  const price = num(pEl && pEl.value) || (sel && sel.ltp) || 0;
  const qty = lot * lots;
  const req = document.getElementById("rtEngReq");
  if (req && !req.dataset.live) req.textContent = qty <= 0 || price <= 0 ? "--" : fmtMoney(qty * price);
  scheduleMarginRequired();
}

// The chart's active instrument + its live quote, published by sidebar.js.
function chartSelection() {
  return window.__chartSelection || null;
}

// Fill the active method's Est. price from the live chart LTP (unless the user
// typed a manual price), so order entry is priced off live data.
function applyChartPrice() {
  const sel = chartSelection();
  const p = document.querySelector("#rtEngMethodOpts .rtEng-price");
  if (p && priceAuto[activeMethod] !== false && document.activeElement !== p && sel && sel.ltp > 0) {
    p.value = Number(sel.ltp).toFixed(2);
  }
  updateMargin();
}

function scheduleMarginRequired() {
  if (marginReqTimer) clearTimeout(marginReqTimer);
  marginReqTimer = setTimeout(refreshMarginRequired, 350);
}

// Pull the real Dhan margin for the chart symbol + configured qty via the engine
// (`/api/rt/instrument`), replacing the notional estimate with broker truth.
async function refreshMarginRequired() {
  const sel = chartSelection();
  if (!sel) return;
  const u = (STATE && STATE.settings) || {};
  const lotEl = document.getElementById("rtEngLot");
  const lotsEl = document.getElementById("rtEngLots");
  const lot = num(lotEl && lotEl.value) || num(u.lotSize) || chartLot;
  const lots = num(lotsEl && lotsEl.value) || 1;
  const pEl = document.querySelector("#rtEngMethodOpts .rtEng-price");
  const price = num(pEl && pEl.value) || sel.ltp || 0;
  const params = new URLSearchParams({
    securityId: String(sel.id || 0),
    exchangeSegment: sel.exch || "",
    symbol: sel.name || "",
    side: "BUY",
    productType: "INTRADAY",
    lots: String(lots),
    lotSize: String(lot || 0),
    price: String(price || 0),
  });
  try {
    const r = await fetch("/api/rt/instrument?" + params.toString());
    const d = await r.json();
    if (!d || d.ok !== true) return;
    const req = document.getElementById("rtEngReq");
    if (req) {
      if (d.marginRequired > 0) {
        req.dataset.live = "1";
        req.textContent = fmtMoney(d.marginRequired);
      } else {
        delete req.dataset.live;
      }
    }
    const av = d.availableBalance != null ? d.availableBalance : availableMargin();
    const ha = document.getElementById("rtomMarginAvail");
    if (ha && av != null) ha.textContent = fmtMoney(av);
    if (lotEl && d.lotSize > 0) {
      chartLot = d.lotSize;
      lotEl.placeholder = String(d.lotSize);
    }
    if (pEl && d.ltp > 0 && priceAuto[activeMethod] !== false && document.activeElement !== pEl) {
      pEl.value = Number(d.ltp).toFixed(2);
    }
    const fr = document.getElementById("rtEngFreeze");
    if (fr && num(sel.id) > 0) {
      try {
        const f = await API.freeze(sel.id, sel.name);
        fr.textContent = f && num(f.freezeQty) > 0 ? String(num(f.freezeQty)) : "--";
      } catch (e) {
        /* freeze qty optional */
      }
    }
  } catch (e) {
    /* broker offline - keep the estimate */
  }
}

function availableMargin() {
  const f = (STATE && STATE.funds) || {};
  const bal = f.availabelBalance != null ? f.availabelBalance : f.availableBalance;
  return bal != null ? num(bal) : null;
}

// Reflect the live available balance into the read-only Engine-controls readout
// and the top funds pill. Called on every snapshot and account refresh.
function updateBalanceReadouts() {
  const bal = availableMargin();
  const txt = bal != null ? fmtMoney(bal) : "--";
  const fp = document.getElementById("rtFundsPill");
  if (fp) fp.textContent = "Funds: " + txt;
  const ab = document.getElementById("rtEngAvailBal");
  if (ab) ab.textContent = txt;
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------
function settingsFromDom() {
  const s = {};
  document.querySelectorAll("#tab-realtime [data-set]").forEach((inp) => {
    const k = inp.getAttribute("data-set");
    s[k] = inp.type === "checkbox" ? inp.checked : inp.type === "number" ? num(inp.value) : inp.value;
  });
  const intLists = ["moversIndices", "commodityList", "scannerExclude", "liquidList"];
  // "No trade on sideways trend" is shown beside all three trend-following
  // engines but is a single persisted switch; read it from the NIFTY row (the
  // one that is always rendered) so a hidden duplicate can never win.
  const sidewaysCb = document.getElementById("rtNiftySidewaysCb");
  if (sidewaysCb) s.noTradeSideways = sidewaysCb.checked;
  // "Ignore fake breakouts/reversals" is shown beside all trend engines but is a
  // single persisted switch; read it from the NIFTY row (always rendered) so a
  // hidden duplicate in a stripped tab can never win.
  const antiFakeCb = document.getElementById("rtNiftyAntiFakeCb");
  if (antiFakeCb) s.antiFakeOn = antiFakeCb.checked;
  document.querySelectorAll("#tab-realtime [data-list]").forEach((inp) => {
    const k = inp.getAttribute("data-list");
    const raw = String(inp.value || "").split(",").map((x) => x.trim()).filter(Boolean);
    s[k] = intLists.indexOf(k) >= 0 ? raw.map((x) => num(x)) : raw;
  });
  const f = Object.assign({}, (STATE && STATE.settings && STATE.settings.filters) || {});
  document.querySelectorAll("#tab-realtime [data-filter]").forEach((cb) => {
    f[cb.getAttribute("data-filter")] = cb.checked;
  });
  s.filters = f;
  // Multi-session trade windows are edited by their own row below (not via
  // [data-set]), so carry them over from the last snapshot instead of letting a
  // generic settings save erase them.
  s.tradeSessions = (STATE && STATE.settings && STATE.settings.tradeSessions) || [];
  // Manual Strike Select list is edited by its own row (not via [data-set]), so
  // carry it over from the last snapshot instead of letting a generic save drop it.
  s.manualStrikes = (STATE && STATE.settings && STATE.settings.manualStrikes) || [];
  // Top Movers index legs are edited by each index chip's own CE/PE select (not
  // via [data-set]), so carry them over too.
  s.moversIndexLegs = (STATE && STATE.settings && STATE.settings.moversIndexLegs) || [];
  // Commodity legs are edited by each commodity chip's own CE/PE select (not via
  // [data-set]), so carry them over too.
  s.commodityLegs = (STATE && STATE.settings && STATE.settings.commodityLegs) || [];
  // Liquid F&O explicit gainer/loser picks are edited by their own dropdowns
  // (not via [data-set]). Read the DOM only when the picker is actually
  // populated; before the universe loads (or while a pick is not in it) the
  // existing value is carried over so an unrelated save can never erase it.
  const pick = (id, key) => {
    const sel = document.getElementById(id);
    const prev = num(((STATE && STATE.settings) || {})[key]);
    if (!sel || sel.options.length <= 1) { s[key] = prev; return; }
    s[key] = num(sel.value) || 0;
  };
  pick("rtLiquidGainerSelect", "liquidGainer");
  pick("rtLiquidLoserSelect", "liquidLoser");
  return s;
}

function pushSetting(partial) {
  const s = Object.assign({}, (STATE && STATE.settings) || {}, partial);
  API.settings(s).then(refresh);
}

// ---------------------------------------------------------------------------
// Engine Scan (testing): throttle the entry-condition scan either to a fixed
// interval (minutes + seconds + milliseconds) or to each bar close of a 1min /
// 5min timeframe. Exits are never throttled - the guardian task still runs at
// 50ms. The whole config area fades in/out with the toggles so the inactive
// controls are obviously not in play.
// ---------------------------------------------------------------------------
const SCAN_MIN_MS = 50;
const SCAN_MAX_MS = 86400000;

function scanTfNorm(v) {
  return String(v || "").toLowerCase().indexOf("5") === 0 ? "5min" : "1min";
}

// Combine the three unit inputs into one millisecond value, clamped to the same
// bounds the engine uses (so the UI and server never disagree).
function scanIntervalFromInputs() {
  const g = (id) => {
    const el = document.getElementById(id);
    return el ? Math.max(0, Math.floor(num(el.value) || 0)) : 0;
  };
  let total = g("rtScanMin") * 60000 + g("rtScanSec") * 1000 + g("rtScanMs");
  if (total < SCAN_MIN_MS) total = SCAN_MIN_MS;
  if (total > SCAN_MAX_MS) total = SCAN_MAX_MS;
  return total;
}

function humanScanMs(ms) {
  if (ms >= 60000 && ms % 60000 === 0) return ms / 60000 + (ms === 60000 ? " minute" : " minutes");
  if (ms >= 1000 && ms % 1000 === 0) return ms / 1000 + (ms === 1000 ? " second" : " seconds");
  return ms + " ms";
}

// Reflect the current checkbox/select state onto the DOM (fade + enable/disable
// + status text). Reads live DOM controls, so it can run before a server round
// trip for instant feedback.
function refreshEngineScanUi() {
  const master = document.getElementById("rtEngineScanCb");
  const barCb = document.getElementById("rtScanBarCloseCb");
  const on = !!(master && master.checked);
  const bc = on && !!(barCb && barCb.checked);
  const intervalActive = on && !bc;
  const barActive = on && bc;
  const box = (id) => document.getElementById(id);
  if (box("rtScanIntervalBox")) box("rtScanIntervalBox").classList.toggle("on", intervalActive);
  if (box("rtScanBarCloseChk")) box("rtScanBarCloseChk").classList.toggle("on", on);
  if (box("rtScanBarCloseBox")) box("rtScanBarCloseBox").classList.toggle("on", barActive);
  ["rtScanMin", "rtScanSec", "rtScanMs"].forEach((id) => {
    if (box(id)) box(id).disabled = !intervalActive;
  });
  if (box("rtScanBarCloseTf")) box("rtScanBarCloseTf").disabled = !barActive;
  if (barCb) barCb.disabled = !on;
  const status = box("rtScanStatus");
  if (status) {
    if (!on) {
      status.textContent = "Scan OFF (normal ~100ms tick)";
      status.style.color = "#666";
    } else if (bc) {
      const tf = scanTfNorm(box("rtScanBarCloseTf") && box("rtScanBarCloseTf").value);
      status.textContent = "Scan on each " + (tf === "5min" ? "5-minute" : "1-minute") + " bar close";
      status.style.color = "#00d4aa";
    } else {
      status.textContent = "Scanning every " + humanScanMs(scanIntervalFromInputs());
      status.style.color = "#00d4aa";
    }
  }
}

// Chart Type controls: reveal the Renko geometry row only when Renko is picked,
// and inside it only the fields the selected brick mode actually uses. Reads the
// live DOM so it can run before a server round-trip for instant feedback.
// Autonomous brain readout + read-only contract.
//
// While Auto Trading is ON the brain owns the indicator filters, the chart type
// and the risk multipliers, so those controls become read-only (Option A: the
// operator only watches). The summary line always states WHY - the regime math
// and the measured hit-rate of each chosen filter - so nothing is a black box.
function syncBrainUi(s) {
  s = s || (STATE && STATE.settings) || {};
  const autoOn = !!s.autoTradeOn;
  const regime = String(s.brainRegime || "");
  const note = String(s.brainNote || "");
  const bl = Number(s.brainSlAtr);
  const tr = Number(s.brainTrailAtr);
  const rr = Number(s.brainTpRr);

  const box = document.getElementById("rtBrainSummary");
  if (box) {
    if (!autoOn) {
      box.innerHTML = '<span style="color:#666">Brain idle - Auto Trading OFF. ON karte hi brain khud regime padh kar filters, chart, SL, trail aur target chunega (har choice ka reason yahan dikhega).</span>';
    } else if (!note) {
      box.innerHTML = '<span style="color:#ffd54f">Brain ON - pehla decision agle engine tick par aayega (market feed chahiye).</span>';
    } else {
      // A decision older than ~3 minutes cannot be trusted as "current": the
      // brain needs a live candle series and the feed may be cold. Show it as
      // STALE with its age instead of pretending it was just recomputed.
      const at = Number(s.brainNoteMs) || 0;
      const ageMs = at > 0 ? Date.now() - at : Infinity;
      const stale = ageMs > 180000;
      const ageTxt = at > 0
        ? (stale ? " (STALE, " + Math.round(ageMs / 60000) + " min purana)" : " (abhi)")
        : "";
      const chip = (label, val, color) =>
        '<span style="display:inline-block;background:#1a1030;border:1px solid ' + color + ';border-radius:4px;padding:1px 7px;margin-right:6px;color:' + color + ';font-weight:700">' +
        label + ": " + val + "</span>";
      box.innerHTML =
        chip("Regime", regime.toUpperCase() || "-", regime === "trend" ? "#00e676" : "#ffb74d") +
        chip("SL", (Number.isFinite(bl) ? bl.toFixed(1) : "-") + "x ATR", "#ef5350") +
        chip("Trail", (Number.isFinite(tr) ? tr.toFixed(1) : "-") + "x ATR", "#4fc3f7") +
        chip("Target", (Number.isFinite(rr) ? rr.toFixed(1) : "-") + "R", "#26a69a") +
        chip("Chart", String(s.chartKind || "candles"), "#ba68c8") +
        (s.brainNoTradeVol
          ? chip("Vol", "NO-TRADE", "#ff5252")
          : chip("Vol", String(s.brainVolZone || "ok"), "#8d8d8d")) +
        (Number.isFinite(Number(s.brainCostR))
          ? chip("Cost", (Number(s.brainCostR)).toFixed(2) + "R", "#78909c")
          : "") +
        (s.exitSuiteOn ? chip("Exits", "suite ON", "#4db6ac") : "") +
        (stale
          ? '<span style="display:inline-block;background:#2a220d;border:1px solid #7e6a2d;border-radius:4px;padding:1px 7px;color:#ffd479;font-weight:700">Stale' + esc(ageTxt) + "</span>"
          : '<span style="color:#7a7a9a;font-size:9px">' + esc(ageTxt) + "</span>") +
        '<div style="margin-top:4px;color:' + (stale ? "#8a7f5a" : "#b39ddb") + ';line-height:1.5">' + esc(note) + "</div>";
    }
  }

  // Read-only contract: the brain writes these fields, so a manual edit would
  // just be overwritten on the next tick. Keep them visible but locked.
  const lockTip = "Brain yeh value khud chun raha hai (Auto Trading ON). Audit ke liye summary line dekho. Manual control ke liye Auto Trading OFF karo.";
  const lock = (el, isLocked) => {
    if (!el) return;
    if (isLocked) {
      if (!el.dataset.brainLocked) {
        el.dataset.brainLocked = "1";
        el.dataset.prevDisabled = el.disabled ? "1" : "0";
      }
      el.disabled = true;
      el.title = lockTip;
    } else if (el.dataset.brainLocked) {
      el.disabled = el.dataset.prevDisabled === "1";
      el.title = "";
      delete el.dataset.brainLocked;
      delete el.dataset.prevDisabled;
    }
  };
  document.querySelectorAll("#tab-realtime [data-filter]").forEach((cb) => lock(cb, autoOn));
  document.querySelectorAll('#tab-realtime [data-master]').forEach((cb) => lock(cb, autoOn));
  const chartFlds = ["rtFilterOnDisplay", "rtChartKind", "rtRenkoMode", "rtRenkoSource"];
  chartFlds.forEach((id) => lock(document.getElementById(id), autoOn));
  const autoWrap = document.getElementById("rtAutoTradeWrap");
  if (autoWrap) {
    autoWrap.title = autoOn
      ? "Auto Trading ON: brain (real regime + filter hit-rate math) khud filters, chart, SL, trail aur target chun raha hai. Neeche summary me har choice ka reason dikhta hai. Master switch ON = automatic entries."
      : "ON: brain market regime padh kar khud best filters, chart, ATR stop, profit trail aur target chunta hai. Master switch ON karte hi entries khud chalti hain. OFF: koi nayi entry nahi, par chal rahi trades apne SL / trail / target par protected rehti hain.";
  }
  // The static Brain select/threshold remain the gate-scoring mode (separate
  // from Auto Trading), so they stay editable.
}

function refreshChartKindUi() {
  const val = (id) => {
    const el = document.getElementById(id);
    return el ? el.value : "";
  };
  const onEl = document.getElementById("rtFilterOnDisplay");
  const on = !!(onEl && onEl.checked);
  // Fade the chart-kind controls when the master switch is OFF: the engine is on
  // the raw candle series, so the chart type is display-only.
  const wrap = document.getElementById("rtChartKindWrap");
  if (wrap) wrap.style.opacity = on ? "1" : ".4";
  const hint = document.getElementById("rtChartKindHint");
  if (hint) hint.textContent = on
    ? "Filters isi DISPLAY chart par chal rahe hain."
    : "Switch ON karo to saare indicator filter DISPLAY chart par chalenge. OFF = default (raw candle data).";
  const kind = val("rtChartKind") || "candles";
  const renko = on && kind === "renko";
  const ctl = document.getElementById("rtRenkoCtl");
  if (ctl) ctl.style.display = renko ? "flex" : "none";
  if (!renko) return;
  const mode = val("rtRenkoMode") || "traditional";
  const show = (id, isOn) => {
    const el = document.getElementById(id);
    if (el) el.style.display = isOn ? "flex" : "none";
  };
  show("rtRenkoBoxRow", mode === "traditional");
  show("rtRenkoAtrRow", mode === "atr");
  show("rtRenkoPctRow", mode === "percentage");
}

// Sync the Engine Scan controls from a settings snapshot. Never clobbers the
// field the user is currently typing in.
function applyEngineScanUI(s) {
  s = s || {};
  const raw = Math.floor(num(s.scanIntervalMs) || 0);
  const total = raw < SCAN_MIN_MS ? 100 : Math.min(raw, SCAN_MAX_MS);
  const active = document.activeElement;
  const setVal = (id, v) => {
    const el = document.getElementById(id);
    if (el && el !== active) el.value = v;
  };
  setVal("rtScanMin", Math.floor(total / 60000));
  setVal("rtScanSec", Math.floor((total % 60000) / 1000));
  setVal("rtScanMs", total % 1000);
  const hidden = document.getElementById("rtScanTotalMs");
  if (hidden && hidden !== active) hidden.value = total;
  const sel = document.getElementById("rtScanBarCloseTf");
  if (sel && sel !== active) sel.value = scanTfNorm(s.scanBarCloseTf);
  refreshEngineScanUi();
}

// --- Run mode (old AST run-mode section) --------------------------------
// Indicator-filters mode is the only run mode: it trades the scanner universe
// (Top Movers / NIFTY trend / Commodities) with the ticked Bullish/Bearish
// indicator filters agreeing by majority (tick "All together (strict AND)" to
// require every one, or enable AI Brain for a score/veto gate). The old
// "Normal mode" (run the ticked strategies) and its "Run Paper Trading" button
// have been removed.
function applyRunModeUI() {
  const fCb = document.getElementById("rtFilterModeCb");
  if (fCb) fCb.checked = true;
  const fBtn = document.getElementById("rtFilterPaperBtn");
  if (fBtn) {
    fBtn.style.opacity = "1";
    fBtn.disabled = false;
  }
}

async function saveRunMode(partial) {
  const s = Object.assign({}, (STATE && STATE.settings) || {}, partial);
  await API.settings(s);
  await refresh();
  applyRunModeUI();
}

async function startRunEngine() {
  if (!(await uiConfirm(PAPER
    ? "Paper engine ON karein?\n\nMaster ON hote hi engine run + armed + auto tick ho jayega. Sab entries/exits SIMULATED honge (koi real Dhan order nahi)."
    : "Realtime engine ON karein?\n\nMaster ON hote hi engine run + ARMED + auto tick ho jayega aur LIVE Dhan orders enable honge.\nSirf OK dabao jab aap real trades chahte ho.", { danger: true }))) return false;
  const r = await API.engine({ on: true });
  if (!r || r.ok === false) {
    uiAlert((r && r.error) || "master switch ON failed");
    return false;
  }
  if (STATE) {
    STATE.engineOn = r.engineOn !== false;
    STATE.armed = !!r.armed;
    paintEnginePills(STATE.engineOn, STATE.armed);
  }
  return true;
}

/* "Place trades based on Indicator filters": the scanner universe is traded
   when the ticked Bullish/Bearish filters agree by majority (or satisfy the
   AI Brain threshold when enabled); tick "All together (strict AND)" to demand
   every filter. */
async function runFilterPaper() {
  await saveRunMode({ callManual: true, aiPick: false, filterMode: true });
  if (await startRunEngine()) refresh();
}

/* Indicator-filters is the only run mode, so its checkbox is permanently on:
   an attempt to untick it just re-arms the mode. */
async function onRunModeToggle() {
  await saveRunMode({ filterMode: true });
}

function wire() {
  const $ = (id) => document.getElementById(id);

  // Closed Trades "Show all" / "Collapse": lifts/restores the 230px inner-scroll
  // cap. The full ledger is already in the DOM, so this is a pure view toggle.
  const closedBtn = $("rtClosedExpand");
  if (closedBtn) closedBtn.onclick = () => {
    closedExpanded = !closedExpanded;
    try { localStorage.setItem("rtClosedExpand", closedExpanded ? "1" : "0"); } catch (e) { /* non-fatal */ }
    applyClosedExpand();
  };

  // Engine Scan: the interval inputs are not [data-set] (they combine into the
  // hidden scanIntervalMs), so wire them here; the checkboxes/select are normal
  // [data-set] controls saved by the generic binder further down.
  const scanMasterCb = $("rtEngineScanCb");
  const scanBarCb = $("rtScanBarCloseCb");
  [scanMasterCb, scanBarCb].forEach((cb) => {
    if (cb) cb.addEventListener("change", refreshEngineScanUi);
  });
  const scanTfSel = $("rtScanBarCloseTf");
  if (scanTfSel) scanTfSel.addEventListener("change", refreshEngineScanUi);
  let scanSaveTimer = null;
  const scheduleScanSave = () => {
    const hidden = $("rtScanTotalMs");
    if (hidden) hidden.value = scanIntervalFromInputs();
    if (scanSaveTimer) clearTimeout(scanSaveTimer);
    scanSaveTimer = setTimeout(() => {
      scanSaveTimer = null;
      API.settings(settingsFromDom()).then(refresh);
    }, 500);
  };
  ["rtScanMin", "rtScanSec", "rtScanMs"].forEach((id) => {
    const el = $(id);
    if (el) el.addEventListener("input", () => { refreshEngineScanUi(); scheduleScanSave(); });
  });
  refreshEngineScanUi();

  // Follow the chart's active instrument: keep the Order Placement cards priced
  // off the same live symbol the user is viewing.
  window.addEventListener("chartselection", () => {
    if (document.getElementById("rtomRow")) applyChartPrice();
  });

  $("rtEngineToggle").onclick = async () => {
    const on = !(STATE && STATE.engineOn);
    if (on) {
      if (!(await uiConfirm(PAPER
        ? "Paper engine ON karein?\n\nMaster ON hote hi engine run + armed + auto tick ho jayega. Sab entries/exits SIMULATED honge (koi real Dhan order nahi)."
        : "Realtime engine ON karein?\n\nMaster ON hote hi engine run + ARMED + auto tick ho jayega aur LIVE Dhan orders enable honge.\nSirf OK dabao jab aap real trades chahte ho.", { danger: true }))) return;
    }
    const r = await API.engine({ on });
    if (!r || r.ok === false) {
      uiAlert((r && r.error) || "master switch change failed");
      return;
    }
    if (STATE) {
      STATE.engineOn = r.engineOn !== false;
      STATE.armed = !!r.armed;
      paintEnginePills(STATE.engineOn, STATE.armed);
    }
    refresh();
  };

  // Run-mode section: Indicator-filters mode is the only mode.
  const filterBtn = $("rtFilterPaperBtn");
  if (filterBtn) filterBtn.onclick = () => runFilterPaper();
  const filterCb = $("rtFilterModeCb");
  if (filterCb) filterCb.onchange = () => onRunModeToggle();
  applyRunModeUI();

  // Engine Risk & Quantity row. SL / Trail SL default to a sane %
  // the moment they are ticked (a tick with 0% would disable the stop), and the
  // Auto-Lot toggle drives the same `autoLots` flag `open_entry` reads.
  const riskPct = (pctId, def) => num(($(pctId) || {}).value) || def;
  const slCb = $("rtEngSl");
  if (slCb) {
    slCb.onchange = () => {
      if (slCb.checked && !num(($("rtEngSlPct") || {}).value)) $("rtEngSlPct").value = 1.5;
      pushSetting({ manualSl: slCb.checked, manualSlPct: riskPct("rtEngSlPct", 1.5) });
    };
  }
  const slPct = $("rtEngSlPct");
  if (slPct) slPct.oninput = () => pushSetting({ manualSl: true, manualSlPct: num(slPct.value) });
  const trCb = $("rtEngTrailSl");
  if (trCb) {
    trCb.onchange = () => {
      if (trCb.checked && !num(($("rtEngTrailSlPct") || {}).value)) $("rtEngTrailSlPct").value = 1;
      pushSetting({ manualTrailSl: trCb.checked, manualTrailSlPct: riskPct("rtEngTrailSlPct", 1) });
    };
  }
  const trPct = $("rtEngTrailSlPct");
  // The Trail SL checkbox is the SINGLE on/off authority. Editing the % field
  // must only persist the number - it must never silently re-arm a trail the
  // operator has switched OFF (that was the "unchecked but trail still fires"
  // bug).
  if (trPct) trPct.oninput = () => pushSetting({ manualTrailSlPct: num(trPct.value) });
  const netTpCb = $("rtEngNetTp");
  const netTpMin = $("rtEngNetTpMin");
  const netTpGb = $("rtEngNetTpGiveback");
  if (netTpCb) {
    netTpCb.onchange = () => {
      if (netTpCb.checked && !num((netTpMin || {}).value)) netTpMin.value = 500;
      pushSetting({ netTpOn: netTpCb.checked, netTpMin: num((netTpMin || {}).value) || 500 });
    };
  }
  if (netTpMin) netTpMin.oninput = () => pushSetting({ netTpOn: true, netTpMin: num(netTpMin.value) });
  if (netTpGb) netTpGb.oninput = () => pushSetting({ netTpGivebackPct: num(netTpGb.value) });
  const ptrCb = $("rtEngPointTrailSl");
  if (ptrCb) {
    ptrCb.onchange = () => {
      if (ptrCb.checked && !num(($("rtEngPointTrailSlPts") || {}).value)) $("rtEngPointTrailSlPts").value = 1;
      pushSetting({ manualPointTrailSl: ptrCb.checked, manualPointTrailSlPoints: riskPct("rtEngPointTrailSlPts", 1) });
      syncInterlocks();
    };
  }
  const ptrPts = $("rtEngPointTrailSlPts");
  // Same rule as the % field: editing the points value persists the number but
  // never re-arms the point trail behind the operator's back.
  if (ptrPts) ptrPts.oninput = () => pushSetting({ manualPointTrailSlPoints: num(ptrPts.value) });
  const pfCb = $("rtEngPtsFromPct");
  if (pfCb)
    pfCb.onchange = () => {
      // When the link goes ON the point trail is derived per strike from the Trail
      // SL % (broker-only), so surface that: tick the point box and mark the field
      // "auto" instead of leaving stale controls looking active.
      if (pfCb.checked && ptrCb && document.activeElement !== ptrCb) ptrCb.checked = true;
      if (ptrPts) ptrPts.placeholder = pfCb.checked ? "auto" : "pts";
      pushSetting({ trailPointsFromPct: pfCb.checked });
      syncInterlocks();
    };
  const lotEl = $("rtEngLot");
  if (lotEl) lotEl.onchange = () => pushSetting({ lotSize: num(lotEl.value) });
  const lotsEl = $("rtEngLots");
  if (lotsEl) lotsEl.onchange = () => { if (!(STATE && STATE.autoLots)) pushSetting({ lots: num(lotsEl.value) }); };
  const alCb = $("rtEngAutoLots");
  if (alCb) alCb.onchange = () => API.autoLots({ autoLots: !!alCb.checked }).then(refresh);

  // "Only +green premium strikes" and "Opposite side fallback (if all minus)" are
  // the required base the "Execute Trade In" dropdown runs on for the full
  // engine. Paper Trade 2 does NOT force them: its "Execute Trade In"
  // (Above/Below ATM + Number of Strikes) must also work with the +green base
  // off, so the operator can turn the preferences off freely there.
  const STRIKE_BASE_GUARD = {
    onlyPositive: "Only +green premium strikes",
    fallbackOppositeSide: "Opposite side fallback (if all minus)",
  };
  document.querySelectorAll("#tab-realtime [data-set]").forEach((inp) => {
    inp.onchange = () => {
      const k = inp.getAttribute("data-set");
      if (k === "noTradeSideways") {
        document.querySelectorAll('#tab-realtime [data-set="noTradeSideways"]').forEach((c) => {
          c.checked = inp.checked;
        });
      }
      if (k === "antiFakeOn") {
        document.querySelectorAll('#tab-realtime [data-set="antiFakeOn"]').forEach((c) => {
          c.checked = inp.checked;
        });
      }
      if (!PAPER2 && !EQUITY_ONLY && inp.type === "checkbox" && !inp.checked && STRIKE_BASE_GUARD[k]) {
        uiAlert(
          '"' + STRIKE_BASE_GUARD[k] + '" ON rakhna zaroori hai. "Execute Trade In" ke sare dropdown options isi +green base par kaam karte hain, isliye ise enable kar diya gaya hai.'
        );
        inp.checked = true;
      }
      syncInterlocks();
      API.settings(settingsFromDom()).then(refresh);
    };
  });

  // Crossover read mode: "forming" and "closed" are mutually exclusive. Ticking
  // one fades + disables the other; un-ticking the active one leaves BOTH
  // unchecked (both inactive), so neither mode is forced. Choosing forming also
  // releases the global "Formed candle only" switch, because that switch forces
  // every filter (including crossovers) to read the closed bar.
  [
    ["rtIndCrossForming", "rtIndCrossClosed"],
    ["rtIndCrossClosed", "rtIndCrossForming"],
  ].forEach(([id, otherId]) => {
    const cb = document.getElementById(id);
    if (!cb) return;
    cb.onchange = () => {
      const other = document.getElementById(otherId);
      if (cb.checked && other) other.checked = false;
      if (cb.checked && cb.id === "rtIndCrossForming") {
        const g = document.querySelector('#tab-realtime [data-set="closedBarCompute"]');
        if (g && g.checked) g.checked = false;
      }
      syncInterlocks();
      API.settings(settingsFromDom()).then(refresh);
    };
  });

  // Chart Type: the generic [data-set] handler above already persists the value;
  // this only refreshes which Renko geometry fields are visible and fades the
  // controls with the "Run filters on display chart" master switch.
  const dispSw = document.getElementById("rtFilterOnDisplay");
  if (dispSw) dispSw.addEventListener("change", refreshChartKindUi);
  ["rtChartKind", "rtRenkoMode"].forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.addEventListener("change", refreshChartKindUi);
  });

  // Manual Strike Select (testing): the checkbox is a normal [data-set] setting
  // (the generic handler above saves it; syncInterlocks hides/shows the box and
  // fades the automatic strike controls). These handlers drive the picker.
  const msCb = document.getElementById("rtManualStrikeCb");
  if (msCb) {
    msCb.addEventListener("change", () => {
      if (msCb.checked) manualLoadInstruments(false);
    });
  }
  const msInst = document.getElementById("rtManualInstrument");
  if (msInst) msInst.onchange = () => manualLoadChain();
  const msExp = document.getElementById("rtManualExpiry");
  if (msExp) msExp.onchange = () => manualLoadChain();
  const msAdd = document.getElementById("rtManualStrikeAdd");
  if (msAdd) msAdd.onclick = () => addManualStrike();

  document.querySelectorAll("#tab-realtime [data-act]").forEach((b) => {
    const act = b.getAttribute("data-act");
    b.onclick = async () => {
      if (act === "runref") {
        refresh();
      } else if (act === "ticknow") {
        // Optional "force now" shortcut. The master switch already ticks on ON
        // and scans on its own; this only nudges an ON engine and must never
        // start trading while the master is OFF.
        if (!(STATE && STATE.engineOn)) return;
        const r = await API.tick();
        if (r && r.ok === false) uiAlert(r.error || "tick failed");
        refresh();
      } else if (act === "closeall") {
        await squareOffAllTrades();
      } else if (act === "resetpnl") {
        if (!(await uiConfirm("Clear closed trades + logs and reset realized P&L?", { danger: true }))) return;
        await API.reset({ what: "closed" });
        await API.reset({ what: "logs" });
        // Drop the client-side closed/day-stats caches so the emptied book is
        // actually reflected (the 1s poll only ships the newest slice and would
        // otherwise keep painting the cached rows).
        resetBookCaches();
        refresh();
      }
    };
  });

  const sqBtn = $("rtSquareOff");
  if (sqBtn) sqBtn.onclick = () => squareOffAllTrades();
  const refreshAccBtn = $("rtRefreshAccount");
  if (refreshAccBtn) refreshAccBtn.onclick = () => refreshAccount();

  const condClear = $("rtCondLogClear");
  if (condClear)
    condClear.onclick = async () => {
      if (!(await uiConfirm("Clear the Condition Log?"))) return;
      await API.reset({ what: "logs" });
      refresh();
    };
  const blkClear = $("rtBlockerClear");
  if (blkClear)
    blkClear.onclick = async () => {
      await API.blockersClear();
      loadBlockers();
    };
  const blkRefresh = $("rtBlockerRefresh");
  if (blkRefresh) blkRefresh.onclick = () => loadBlockers();
  const diagCap = $("rtDiagCapture");
  if (diagCap)
    diagCap.onclick = async () => {
      const note = $("rtDiagNote");
      if (note) note.textContent = "Capturing...";
      diagCap.disabled = true;
      try {
        const r = await API.diagCapture();
        if (note) {
          note.textContent = r && r.ok
            ? "Saved: " + (r.file || "(file written)")
            : "Capture failed: " + ((r && r.error) || "unknown error");
          note.style.color = r && r.ok ? "#7ec8ff" : "#ff8a80";
        }
      } catch (e) {
        if (note) { note.style.color = "#ff8a80"; note.textContent = "Capture failed: " + String(e.message || e); }
      } finally {
        diagCap.disabled = false;
      }
    };
  const diagBnd = $("rtDiagBundle");
  if (diagBnd)
    diagBnd.onclick = async () => {
      const note = $("rtDiagNote");
      if (note) note.textContent = "Building support bundle...";
      diagBnd.disabled = true;
      try {
        const r = await API.diagBundleFile();
        if (note) {
          note.textContent = r && r.ok
            ? "Bundle saved: " + (r.file || "(file written)") + " - isi file ko bhejein"
            : "Bundle failed: " + ((r && r.error) || "unknown error");
          note.style.color = r && r.ok ? "#7ec8ff" : "#ff8a80";
        }
      } catch (e) {
        if (note) { note.style.color = "#ff8a80"; note.textContent = "Bundle failed: " + String(e.message || e); }
      } finally {
        diagBnd.disabled = false;
      }
    };
  const condRefresh = $("rtCondLogRefresh");
  if (condRefresh) condRefresh.onclick = () => refreshLogs();
  const condLvl = $("rtCondLogLevel");
  if (condLvl) condLvl.onchange = () => renderConditionLog();
  const condPause = $("rtCondLogPause");
  if (condPause) condPause.onchange = () => renderConditionLog();

  // Live Data Pool
  $("rtDataPool").onchange = () => {
    const on = $("rtDataPool").checked;
    applyDataPoolUi(on);
    API.settings(settingsFromDom()).then(refresh);
  };
  $("rtDataPoolRefresh").onclick = () => loadPool(true);

  // Templates: one universal pool shared by every engine tab. Saving tags the
  // template with its origin tab; Apply copies its settings into THIS engine.
  refreshTemplateList();
  $("rtTplSave").onclick = async () => {
    const name = ($("rtTplName").value || "").trim();
    if (!name) {
      uiAlert("Template name required");
      return;
    }
    const side = ($("rtTplMode").value || "bullish");
    const r = await API.template({ action: "save", name, side });
    if (!r || !r.ok) {
      $("rtTplInfo").textContent = (r && r.error) || "save failed";
      return;
    }
    $("rtTplName").value = "";
    $("rtTplInfo").textContent = "saved (from " + (r.origin || "this tab") + ")";
    await refreshTemplateList();
  };
  $("rtTplOpen").onchange = () => {
    const name = $("rtTplOpen").value;
    const t = TPLS[name] || {};
    $("rtTplInfo").textContent = name
      ? "selected " + name + (t.origin ? " (from " + t.origin + ")" : "")
      : "";
  };
  $("rtTplApply").onclick = async () => {
    const name = $("rtTplOpen").value;
    if (!name) {
      uiAlert("Pick a saved template first");
      return;
    }
    const r = await API.template({ action: "apply", name });
    $("rtTplInfo").textContent = r && r.ok
      ? "applied " + name + (r.origin ? " (from " + r.origin + ")" : "")
      : (r && r.error) || "apply failed";
    if (r && r.ok) refresh();
  };
  $("rtTplDelete").onclick = async () => {
    const name = $("rtTplOpen").value;
    if (!name) {
      uiAlert("Pick a saved template first");
      return;
    }
    if (!(await uiConfirm("Delete universal template \"" + name + "\"? It will be removed from every engine tab.", { danger: true }))) return;
    const r = await API.template({ action: "delete", name });
    $("rtTplInfo").textContent = r && r.ok ? "deleted" : (r && r.error) || "delete failed";
    await refreshTemplateList();
  };

  // AST saved templates quick run (top of the tab, old-app parity).
  const qrBtn = $("rtAstTplRunBtn");
  if (qrBtn) qrBtn.onclick = () => quickRunTemplate();

  // AST master toggles (Top Movers / NIFTY Trend / Commodities).
  document.querySelectorAll("#tab-realtime [data-toggle]").forEach((b) => {
    b.onclick = () => {
      const k = b.getAttribute("data-toggle");
      const cur = !!((STATE && STATE.settings && STATE.settings[k]));
      const next = !cur;
      const patch = { [k]: next };
      // Paper Trade 2 (indices only) and Paper Trade 3 (commodities only) keep
      // a single instrument master. Realtime 2/3 keep the combined master that
      // arms the index + commodity scanners together.
      if (INDICES_ONLY && k === "moversOn") {
        patch.moversOn = next;
        patch.commodityOn = false;
      } else if (COMMODITIES_ONLY && k === "commodityOn") {
        patch.commodityOn = next;
        patch.moversOn = false;
      } else if (PAPER2 && !INDICES_ONLY && !COMMODITIES_ONLY && (k === "moversOn" || k === "commodityOn")) {
        patch.moversOn = next;
        patch.commodityOn = next;
      } else if (next && (k === "moversOn" || k === "niftyTrendOn")) {
        // Top Movers and NIFTY Trend Following are connected and must run
        // together: switching either one ON also switches the other ON. Turning
        // one OFF stays individual (the other keeps its own state), and the old
        // fade-out that forced one off while the other was on is gone.
        patch.moversOn = true;
        patch.niftyTrendOn = true;
      }
      pushSetting(patch);
    };
  });

  // Commodity picker: add / clear MCX futures in the commodityList.
  const cAdd = $("rtCommodityAddBtn");
  // Fake-move filter settings gear (thresholds for the anti-fake checkbox).
  const afBtn = $("rtAntiFakeSettingsBtn");
  if (afBtn) afBtn.onclick = () => openAntiFakeSettings();
  if (cAdd)
    cAdd.onclick = () => {
      const sel = $("rtCommodityAdd");
      const id = sel && sel.value;
      if (!id) return;
      const inp = document.querySelector('#tab-realtime [data-list="commodityList"]');
      const ids = String((inp && inp.value) || "").split(",").map((x) => x.trim()).filter(Boolean);
      if (ids.indexOf(String(id)) < 0) ids.push(String(id));
      if (inp) inp.value = ids.join(", ");
      API.settings(settingsFromDom()).then(() => {
        renderChips("commodityList", "rtCommodityChips", "commodity");
        refresh();
      });
    };
  const cClear = $("rtCommodityClearBtn");
  if (cClear)
    cClear.onclick = () => {
      const inp = document.querySelector('#tab-realtime [data-list="commodityList"]');
      if (inp) inp.value = "";
      // Clearing the list also drops every commodity leg assignment.
      STATE.settings = Object.assign({}, (STATE && STATE.settings) || {}, { commodityLegs: [] });
      API.settings(settingsFromDom()).then(() => {
        renderChips("commodityList", "rtCommodityChips", "commodity");
        refresh();
      });
    };

  // Liquid F&O pickers (top gainer -> CE / top loser -> PE / manual add).
  wireLiquidControls();

  // Index / indicator dropdown pickers.
  wireAstPickers();

  // Multi-session trade windows (Trade times -> Trading sessions).
  wireTradeSessions();

  // Individual Bullish / Bearish indicator-filter checkboxes.
  // `filtersBusy` holds the row steady across the save round-trip so the 1s
  // snapshot poll cannot revert a tick the user just made.
  const saveFilters = () => {
    filtersBusy = true;
    return API.settings(settingsFromDom()).finally(() => {
      filtersBusy = false;
    });
  };
  document.querySelectorAll("#tab-realtime [data-filter]").forEach((cb) => {
    cb.onchange = () => {
      syncInterlocks();
      saveFilters().then(refresh, refresh);
    };
  });

  // Free-text id/indicator lists (Top Movers indices, NIFTY index list /
  // confirm indicators, commodity ids): persist on blur/change.
  document.querySelectorAll("#tab-realtime [data-list]").forEach((inp) => {
    inp.onchange = () => {
      syncInterlocks();
      API.settings(settingsFromDom()).then(refresh);
    };
  });

  // Bullish / Bearish "(select all)" masters.
  document.querySelectorAll("#tab-realtime [data-master]").forEach((cb) => {
    cb.onchange = () => {
      const side = cb.getAttribute("data-master");
      const list = side === "Bull" ? FILTER_BULL : FILTER_BEAR;
      const on = cb.checked;
      const sec = document.getElementById("rtFilterSection" + side);
      if (sec) sec.querySelectorAll("[data-filter]").forEach((f) => (f.checked = on));
      syncInterlocks();
      saveFilters().then(refresh, refresh);
    };
  });

  // Mirror the selected side's filters to the other side.
  const mirror = $("rtMirrorFilters");
  if (mirror)
    mirror.onclick = () => {
      const count = (side) => Array.from(document.querySelectorAll("#rtFilterSection" + side + " [data-filter]")).filter((c) => c.checked).length;
      const bullN = count("Bull");
      const bearN = count("Bear");
      // Nothing ticked on either side: there is nothing to copy.
      if (bullN === 0 && bearN === 0) return;
      // Copy from the side that actually holds filters; when both sides have
      // some, the fuller one wins (old behaviour) so a lone tick always mirrors.
      const from = bullN >= bearN ? "Bull" : "Bear";
      const to = from === "Bull" ? "Bear" : "Bull";
      const map = from === "Bull" ? FILTER_MIRROR_TO_BEAR : FILTER_MIRROR_TO_BULL;
      document.querySelectorAll("#rtFilterSection" + from + " [data-filter]").forEach((src) => {
        const srcKey = src.getAttribute("data-filter");
        const key = map[srcKey] || srcKey;
        const dst = document.querySelector('#rtFilterSection' + to + ' [data-filter="' + key + '"]');
        if (dst) dst.checked = src.checked;
      });
      // syncInterlocks() re-derives each section master and dim state.
      syncInterlocks();
      saveFilters().then(refresh, refresh);
    };

  // Entry Timing Diagnostics.
  const etClear = $("rtEtClear");
  if (etClear)
    etClear.onclick = async () => {
      await API.entryTimingSet({ action: "clear" });
      loadAstSections();
    };
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------
// Paint the header engine/arm pills + toggle label. Used by the snapshot render
// and by the engine toggle for an instant optimistic update, so the button always
// reacts even before the next (now lightweight) poll confirms it.
function paintEnginePills(on, armed) {
  const pEngine = document.getElementById("rtEnginePill");
  const pArm = document.getElementById("rtArmPill");
  if (pEngine) {
    pEngine.textContent = on ? "Engine: ON" : "Engine: OFF";
    pEngine.className = "rt-pill " + (on ? "on" : "off");
  }
  if (pArm) {
    pArm.textContent = armed ? "ARMED" : "DISARMED";
    pArm.className = "rt-pill " + (armed ? "armed" : "off");
  }
  const tg = document.getElementById("rtEngineToggle");
  if (tg) {
    tg.textContent = "AI Smart Trading: " + (on ? "ON" : "OFF");
    tg.disabled = false;
    tg.title = "Master switch: ON = engine run + ARMED + auto tick; OFF = stop + disarm.";
    tg.style.opacity = "1";
    tg.style.cursor = "pointer";
  }
  const tickBtn = document.getElementById("rtTickNow");
  if (tickBtn) {
    tickBtn.disabled = !on;
    tickBtn.style.opacity = on ? "1" : "0.45";
    tickBtn.style.cursor = on ? "pointer" : "not-allowed";
  }
  // Auto Trading pill: tells the operator, at a glance, whether the coordinated
  // trend-following preset is what is driving new entries. It is a view label
  // only - the engine reads the persisted `autoTradeOn` setting, never this.
  const autoWrap = document.getElementById("rtAutoTradeWrap");
  if (autoWrap) {
    const autoOn = !!(STATE && STATE.settings && STATE.settings.autoTradeOn);
    autoWrap.style.borderColor = autoOn ? "#00e676" : "#1f6f4a";
    autoWrap.style.background = autoOn ? "#0f2419" : "#161a20";
    autoWrap.style.color = autoOn ? "#00e676" : "#8a9bb0";
    autoWrap.title = autoOn
      ? "Auto Trading ON: trend-following preset active (NIFTY direction + movers universe + indicator filters + ATR stop + profit trail + Target-Lab auto-cut). Master switch ON = automatic entries."
      : "Auto Trading OFF: no new entries from the preset. Open trades keep their SL / trail / target.";
  }
  const warn = document.getElementById("rtDisarmWarn");
  if (warn) warn.style.display = on && !armed ? "inline-block" : "none";
}

// Paper Trade 3's NIFTY Trend Following drives an auto-managed normal BB%b
// (Pane) indicator on the NIFTY spot chart. The chart lives in the parent
// window, so the engine state is posted across the frame boundary and the
// parent focuses the NIFTY spot chart on the engine's 1min/5min timeframe and
// adds/removes the pane. Only a change is posted (stringified edge) so the
// parent is never spammed on the 1s poll.
let lastNiftyPaneMsg = "";
function syncNiftyBbPctPane(s) {
  if (!PAPER3) return;
  const cfg = (s && s.settings) || {};
  const on = !!cfg.niftyTrendOn && !!cfg.bbpctPaneOn;
  // Follow the engine's timeframe checkboxes (the same ones the scan uses).
  // When both are ticked the backend runs "both"; the chart shows one series, so
  // 5min is the primary pane (also the default when neither is ticked).
  const tf = cfg.tf5min || !cfg.tf1min ? "5min" : "1min";
  const msg = { type: "niftyBbPctPane", on, tf };
  const key = JSON.stringify(msg);
  if (key === lastNiftyPaneMsg) return;
  lastNiftyPaneMsg = key;
  try {
    if (window.parent && window.parent !== window) window.parent.postMessage(msg, "*");
  } catch (_) {}
}

// ---------------------------------------------------------------------------
// Paper Trade 3: pane-only NIFTY BB%b
// ---------------------------------------------------------------------------
// The Paper 3 tab has no candlestick chart, so the normal BB%b (Pane) indicator
// is drawn on a small canvas mounted right below the NIFTY Trend Following
// section. The series is fetched from the engine (`/api/rt/nifty-pane`, routed
// to `/api/paper3/*` inside this frame) and computed server-side on NIFTY spot
// at the engine's selected 1min/5min timeframe, so the pane always matches the
// exact series the trend engine reads.
let niftyPaneTimer = null;
// Interactive view state for the pane-only BB%b canvas: the series itself is
// replaced on every poll, while `start` / `count` / `follow` hold the operator's
// pan+zoom so a refresh never yanks the view back to the live edge. `follow`
// keeps the newest candle pinned until the operator pans/zooms.
const niftyPane = {
  points: [],
  start: 0,
  count: 0,
  follow: true,
  drag: null,
  hoverX: null,
  wired: false,
  zoneOn: false,
  overbought: 1,
  oversold: 0,
  obOn: true,
  osOn: true,
  trendOn: false,
  trendBullOn: true,
  trendBearOn: true,
  comboOn: false,
  paneOn: false,
  dir: 0,
  lineDir: 0,
  zoneDir: 0,
  zoneDrag: null,
  savedZoneOn: false,
  savedOb: 1,
  savedOs: 0,
  savedObOn: true,
  savedOsOn: true,
  savedTrendOn: false,
  savedTrendBullOn: true,
  savedTrendBearOn: true,
  savedComboOn: false,
  savedPaneOn: false,
  zoneDirty: false,
};

function mountNiftyBbPctPane() {
  const anchor = document.getElementById("rtNiftyTrendList");
  if (!anchor || document.getElementById("rtNiftyPaneWrap")) return;
  const wrap = document.createElement("div");
  wrap.id = "rtNiftyPaneWrap";
  wrap.className = "rt-engine-row";
  wrap.style.cssText =
    "flex-basis:100%;flex-direction:column;align-items:stretch;border-color:#2d2d50;" +
    "background:#12122a;padding:6px 8px;margin-top:4px";
  wrap.innerHTML =
    '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">' +
      '<b class="rt-cap" style="color:#64b5f6">NIFTY BB%b (Pane):</b>' +
      '<label class="rtom-f" id="rtNiftyPaneOnWrap" style="color:#64b5f6;font-weight:700" title="BB%b (Pane) section ka master ON/OFF. OFF karne par ye poora section strictly inert ho jata hai - OB/OS zone aur line-trend modes ignore ho jate hain aur in par koi trade nahi banti. Save Zones dabane par apply hota hai.">' +
        '<input type="checkbox" id="rtNiftyPaneOnCb" style="accent-color:#64b5f6"> active</label>' +
      '<span id="rtNiftyPaneTf" style="font-size:9px;color:#888"></span>' +
      '<span id="rtNiftyPaneLast" style="font-size:11px;font-weight:700;color:#e0e0ff"></span>' +
      '<span id="rtNiftyPaneHover" style="font-size:10px;color:#ffd700"></span>' +
      '<span id="rtNiftyPaneState" style="font-size:9px;color:#888"></span>' +
      '<span style="font-size:8px;color:#666;margin-left:auto">drag = pan &middot; wheel = zoom &middot; dbl-click = reset</span>' +
    "</div>" +
    '<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-top:3px">' +
      '<label class="rtom-f" id="rtNiftyZoneModeWrap" style="color:#ffb74d;transition:opacity .15s" title="ON: NIFTY Trend Following ke chune side ka confirmation. Trend BULLISH ho aur %B oversold (%B neeche OS) ho -> Top Gainers (CE); trend BEARISH ho aur %B overbought (%B upar OB) ho -> Top Losers (PE). Ye mode direction decide NAHI karta - direction kewal NIFTY Trend Following se aata hai. Save Zones dabane par apply.">' +
        '<input type="checkbox" id="rtNiftyZoneCb" style="accent-color:#ffb74d"> BB%b OB/OS zone mode</label>' +
      '<label class="rtom-f" id="rtNiftyTrendModeWrap" style="color:#64b5f6;transition:opacity .15s" title="ON: NIFTY Trend Following ke chune side ka confirmation. Trend BULLISH aur BB%b line bullish (upar/green) -> Top Gainers (CE); trend BEARISH aur line bearish (neeche/red) -> Top Losers (PE). Ye mode direction decide NAHI karta - direction kewal NIFTY Trend Following se aata hai. Save Zones dabane par apply.">' +
        '<input type="checkbox" id="rtNiftyBbTrendCb" style="accent-color:#64b5f6"> BB%b line Bullish/Bearish trend</label>' +
      '<label class="rtom-f" id="rtNiftyComboWrap" style="color:#ab47bc;font-weight:700;transition:opacity .15s" title="COMBO double confirmation: NIFTY Trend Following BULLISH aur BB%b line BULLISH aur %B oversold -> Top Gainers (CE); trend BEARISH aur line BEARISH aur %B overbought -> Top Losers (PE). Dono (line + zone) match karna zaroori. Direction kewal NIFTY Trend Following se aata hai. Iske ON hone par zone/trend ke individual modes override hote hain. Save Zones dabane par apply.">' +
        '<input type="checkbox" id="rtNiftyComboCb" style="accent-color:#ab47bc"> COMBO: bullish+oversold = CE, bearish+overbought = PE</label>' +
      '<span id="rtNiftyTrendSideWrap" style="display:inline-flex;align-items:center;gap:10px;transition:opacity .15s">' +
        '<label class="rtom-f" style="color:#00d4aa;font-weight:700" title="ON: jab NIFTY Trend Following BULLISH ho, BB%b line bullish (rising / green) hone par Top Gainers (CE) trade allowed. OFF: is line condition par CE side strictly blocked. Save Zones ke baad apply.">' +
          '<input type="checkbox" id="rtNiftyTrendBullCb" style="accent-color:#00d4aa"> Top Gainers (CE) - line BULLISH</label>' +
        '<label class="rtom-f" style="color:#ef5350;font-weight:700" title="ON: jab NIFTY Trend Following BEARISH ho, BB%b line bearish (falling / red) hone par Top Losers (PE) trade allowed. OFF: is line condition par PE side strictly blocked. Save Zones ke baad apply.">' +
          '<input type="checkbox" id="rtNiftyTrendBearCb" style="accent-color:#ef5350"> Top Losers (PE) - line BEARISH</label>' +
      "</span>" +
      '<span id="rtNiftyObWrap" style="display:inline-flex;align-items:center;gap:2px;transition:opacity .15s">' +
        '<input type="checkbox" id="rtNiftyObOnCb" style="accent-color:#ef5350" title="ON: jab NIFTY Trend Following BEARISH ho, %B overbought (%B upar OB) hone par Top Losers (PE) trade allowed. OFF: is zone par PE side strictly blocked (Save Zones ke baad).">' +
        '<span style="color:#ef5350;font-size:10px;font-weight:700" title="Overbought line: trend BEARISH + %B is value ke UPAR hone par Top Losers (PE) confirmation.">Top Losers (PE) &middot; OB</span>' +
        '<input type="number" id="rtNiftyObIn" step="0.05" min="0" max="1" style="width:62px">' +
        '<button type="button" class="btn-action" id="rtNiftyObUp" title="Overbought line upar" style="width:auto;padding:1px 6px;margin:0;font-size:10px">^</button>' +
        '<button type="button" class="btn-action" id="rtNiftyObDn" title="Overbought line neeche" style="width:auto;padding:1px 6px;margin:0;font-size:10px">v</button>' +
      "</span>" +
      '<span id="rtNiftyOsWrap" style="display:inline-flex;align-items:center;gap:2px;transition:opacity .15s">' +
        '<input type="checkbox" id="rtNiftyOsOnCb" style="accent-color:#00d4aa" title="ON: jab NIFTY Trend Following BULLISH ho, %B oversold (%B neeche OS) hone par Top Gainers (CE) trade allowed. OFF: is zone par CE side strictly blocked (Save Zones ke baad).">' +
        '<span style="color:#00d4aa;font-size:10px;font-weight:700" title="Oversold line: trend BULLISH + %B is value ke NEECHE hone par Top Gainers (CE) confirmation.">Top Gainers (CE) &middot; OS</span>' +
        '<input type="number" id="rtNiftyOsIn" step="0.05" min="0" max="1" style="width:62px">' +
        '<button type="button" class="btn-action" id="rtNiftyOsUp" title="Oversold line upar" style="width:auto;padding:1px 6px;margin:0;font-size:10px">^</button>' +
        '<button type="button" class="btn-action" id="rtNiftyOsDn" title="Oversold line neeche" style="width:auto;padding:1px 6px;margin:0;font-size:10px">v</button>' +
      "</span>" +
      '<button type="button" class="btn-action" id="rtNiftyZoneSave" title="Set kiye gaye OB/OS zones ko final save karo - ab engine inhi zones par Top Gainers (CE) / Top Losers (PE) trades lega." style="width:auto;padding:3px 12px;margin:0;font-size:10px;background:#4a2d7e;border-color:#7e57c2;color:#fff;font-weight:700">Save Zones</button>' +
      '<span id="rtNiftyPaneZoneDir" style="font-size:10px;font-weight:700"></span>' +
    "</div>" +
    '<div id="rtNiftyZoneSaveMsg" style="font-size:8px;color:#888;margin-top:2px">Zones: default (OB 1.00 / OS 0.00) - save karke apply karein.</div>' +
    '<div style="font-size:8px;color:#666;margin-top:2px">Pane par OB/OS line ko seedha drag karo, ya ^ / v buttons se move karo, phir <b>Save Zones</b> dabao - final values save hongi aur usi base par trade hoga.</div>' +
    '<canvas id="rtNiftyPaneCanvas" style="width:100%;height:160px;display:block;margin-top:4px;' +
      'background:#0a0a18;border:1px solid #1e1e40;border-radius:4px"></canvas>';
  anchor.insertAdjacentElement("afterend", wrap);
  const cv = document.getElementById("rtNiftyPaneCanvas");
  if (cv) {
    cv.style.cursor = "grab";
    cv.addEventListener("mousedown", (e) => {
      if (e.button !== 0) return;
      e.preventDefault();
      // A press near the OB/OS zone line grabs that line; anywhere else pans.
      const zone = niftyPaneZoneHit(e.clientY - cv.getBoundingClientRect().top, cv.getBoundingClientRect().height);
      if (zone) {
        niftyPane.zoneDrag = zone;
        cv.style.cursor = "ns-resize";
        return;
      }
      niftyPane.drag = { x: e.clientX, start: niftyPane.start };
      niftyPane.follow = false;
      cv.style.cursor = "grabbing";
    });
    cv.addEventListener("mousemove", (e) => {
      const rect = cv.getBoundingClientRect();
      if (niftyPane.zoneDrag) return;
      if (niftyPane.drag) return;
      niftyPane.hoverX = e.clientX - rect.left;
      // Hovering the zone line shows the vertical-resize cursor.
      cv.style.cursor = niftyPaneZoneHit(e.clientY - rect.top, rect.height) ? "ns-resize" : "grab";
      drawNiftyBbPctPane();
    });
    cv.addEventListener("mouseleave", () => {
      if (niftyPane.drag || niftyPane.zoneDrag) return;
      niftyPane.hoverX = null;
      cv.style.cursor = "grab";
      drawNiftyBbPctPane();
    });
    cv.addEventListener("wheel", niftyPaneZoom, { passive: false });
    cv.addEventListener("dblclick", () => {
      niftyPane.count = 0;
      niftyPane.follow = true;
      niftyPane.hoverX = null;
      drawNiftyBbPctPane();
    });
  }
  // OB/OS zone editing: drag, the ^ / v mover buttons and typing only update the
  // DRAFT (pane lines + fields). Nothing reaches the engine until "Save Zones"
  // commits the draft in one settings write - and that saved draft is what the
  // Top Gainers (CE) / Top Losers (PE) trades are based on.
  const obIn = document.getElementById("rtNiftyObIn");
  const osIn = document.getElementById("rtNiftyOsIn");
  const zoneCb = document.getElementById("rtNiftyZoneCb");
  const bindStep = (id, which, delta) => {
    const b = document.getElementById(id);
    if (b) b.addEventListener("click", () => {
      const cur = which === "ob" ? niftyPane.overbought : niftyPane.oversold;
      const v = Math.max(0, Math.min(1, Math.round((cur + delta) * 1000) / 1000));
      if (which === "ob") niftyPane.overbought = v;
      else niftyPane.oversold = v;
      niftyPaneSetInput(which, v);
      niftyPaneMarkDirty();
      drawNiftyBbPctPane();
    });
  };
  bindStep("rtNiftyObUp", "ob", 0.05);
  bindStep("rtNiftyObDn", "ob", -0.05);
  bindStep("rtNiftyOsUp", "os", 0.05);
  bindStep("rtNiftyOsDn", "os", -0.05);
  if (obIn) obIn.addEventListener("input", () => {
    niftyPane.overbought = num(obIn.value);
    niftyPaneMarkDirty();
    drawNiftyBbPctPane();
  });
  if (osIn) osIn.addEventListener("input", () => {
    niftyPane.oversold = num(osIn.value);
    niftyPaneMarkDirty();
    drawNiftyBbPctPane();
  });
  const trendCb = document.getElementById("rtNiftyBbTrendCb");
  // OB/OS zone mode and line-trend mode are independent. Each can run alone, and
  // with BOTH ON they combine into double confirmation (overbought + bearish
  // trend -> PE, oversold + bullish trend -> CE), so ticking one never clears the
  // other.
  if (zoneCb) zoneCb.addEventListener("change", () => {
    niftyPane.zoneOn = zoneCb.checked;
    niftyPaneUpdateDisabled();
    niftyPaneMarkDirty();
    drawNiftyBbPctPane();
  });
  if (trendCb) trendCb.addEventListener("change", () => {
    niftyPane.trendOn = trendCb.checked;
    niftyPaneUpdateDisabled();
    niftyPaneMarkDirty();
    drawNiftyBbPctPane();
  });
  const comboCb = document.getElementById("rtNiftyComboCb");
  if (comboCb) comboCb.addEventListener("change", () => {
    niftyPane.comboOn = comboCb.checked;
    niftyPaneUpdateDisabled();
    niftyPaneMarkDirty();
    drawNiftyBbPctPane();
  });
  // Section master switch: OFF strictly inerts the whole BB%b (Pane) section.
  const paneOnCb = document.getElementById("rtNiftyPaneOnCb");
  if (paneOnCb) paneOnCb.addEventListener("change", () => {
    niftyPane.paneOn = paneOnCb.checked;
    niftyPaneUpdateDisabled();
    niftyPaneMarkDirty();
    drawNiftyBbPctPane();
    // OFF is not a draft: unchecking the section master must inert the whole
    // BB%b confirmation immediately (not only after "Save Zones"). ON still goes
    // through Save Zones like every other field in this section. Zone values and
    // mode ticks stay drafts either way.
    if (!niftyPane.paneOn) {
      API.settings({ bbpctPaneOn: false }).then(() => {
        niftyPane.savedPaneOn = false;
        niftyPaneMarkDirty();
        niftyPaneRenderSaveMsg();
        drawNiftyBbPctPane();
      }).catch(() => {});
    }
  });
  // Line-trend per-side enables: each owns one side's strategy + trade run.
  const trendBullCb = document.getElementById("rtNiftyTrendBullCb");
  if (trendBullCb) trendBullCb.addEventListener("change", () => {
    niftyPane.trendBullOn = trendBullCb.checked;
    niftyPaneMarkDirty();
    drawNiftyBbPctPane();
  });
  const trendBearCb = document.getElementById("rtNiftyTrendBearCb");
  if (trendBearCb) trendBearCb.addEventListener("change", () => {
    niftyPane.trendBearOn = trendBearCb.checked;
    niftyPaneMarkDirty();
    drawNiftyBbPctPane();
  });
  const obOnCb = document.getElementById("rtNiftyObOnCb");
  const osOnCb = document.getElementById("rtNiftyOsOnCb");
  if (obOnCb) obOnCb.addEventListener("change", () => {
    niftyPane.obOn = obOnCb.checked;
    niftyPaneMarkDirty();
    drawNiftyBbPctPane();
  });
  if (osOnCb) osOnCb.addEventListener("change", () => {
    niftyPane.osOn = osOnCb.checked;
    niftyPaneMarkDirty();
    drawNiftyBbPctPane();
  });
  const commit = () => niftyPaneSaveZones(obIn, osIn, zoneCb);
  if (obIn) obIn.addEventListener("keydown", (e) => { if (e.key === "Enter") commit(); });
  if (osIn) osIn.addEventListener("keydown", (e) => { if (e.key === "Enter") commit(); });
  const saveBtn = document.getElementById("rtNiftyZoneSave");
  if (saveBtn) saveBtn.addEventListener("click", commit);
  if (!niftyPane.wired) {
    niftyPane.wired = true;
    window.addEventListener("mousemove", niftyPaneGlobalMove);
    window.addEventListener("mouseup", niftyPaneGlobalUp);
  }
  if (!niftyPaneTimer) niftyPaneTimer = setInterval(refreshNiftyBbPctPane, 5000);
  niftyPaneUpdateDisabled();
  refreshNiftyBbPctPane();
}

// Zone-line hit test in CSS pixels: OB (overbought) or OS (oversold) when the
// pointer is within ~6px of the drawn line, else null.
function niftyPaneZoneHit(y, h) {
  const top = 6;
  const bot = h - 6;
  const yOf = (v) => bot - Math.max(0, Math.min(1, v)) * (bot - top);
  const obY = yOf(niftyPane.overbought);
  const osY = yOf(niftyPane.oversold);
  const dOb = Math.abs(y - obY);
  const dOs = Math.abs(y - osY);
  if (dOb <= 6 && dOb <= dOs) return "ob";
  if (dOs <= 6) return "os";
  return null;
}

function niftyPaneSetInput(which, v) {
  const inp = document.getElementById(which === "ob" ? "rtNiftyObIn" : "rtNiftyOsIn");
  if (inp && document.activeElement !== inp) inp.value = Math.round(v * 1000) / 1000;
}

// Fade + disable a control group so an inactive control cannot be edited.
function niftyPaneSetFaded(el, faded) {
  if (!el) return;
  el.style.opacity = faded ? "0.35" : "1";
  el.style.pointerEvents = faded ? "none" : "";
  el.querySelectorAll("input,button").forEach((x) => { x.disabled = !!faded; });
}

// Fade + disable a control group so an inactive mode cannot be edited. The zone
// and line-trend groups are independent now (both may run at once for double
// confirmation), so only the section master switch fades them: with the master
// OFF the whole mode area goes inert:
//  * pane OFF -> OB/OS zone mode, line-trend mode and the OB/OS controls all fade.
function niftyPaneUpdateDisabled() {
  const paneOn = !!niftyPane.paneOn;
  niftyPaneSetFaded(document.getElementById("rtNiftyZoneModeWrap"), !paneOn);
  niftyPaneSetFaded(document.getElementById("rtNiftyObWrap"), !paneOn);
  niftyPaneSetFaded(document.getElementById("rtNiftyOsWrap"), !paneOn);
  niftyPaneSetFaded(document.getElementById("rtNiftyTrendModeWrap"), !paneOn);
  niftyPaneSetFaded(document.getElementById("rtNiftyTrendSideWrap"), !paneOn);
  niftyPaneSetFaded(document.getElementById("rtNiftyComboWrap"), !paneOn);
  const cv = document.getElementById("rtNiftyPaneCanvas");
  if (cv) cv.style.opacity = paneOn ? "1" : "0.45";
}

// A draft edit (drag / mover / typing / any checkbox) is not live until Save.
function niftyPaneMarkDirty() {
  niftyPane.zoneDirty =
    Math.abs(niftyPane.overbought - niftyPane.savedOb) > 1e-9 ||
    Math.abs(niftyPane.oversold - niftyPane.savedOs) > 1e-9 ||
    niftyPane.zoneOn !== niftyPane.savedZoneOn ||
    niftyPane.obOn !== niftyPane.savedObOn ||
    niftyPane.osOn !== niftyPane.savedOsOn ||
    niftyPane.trendOn !== niftyPane.savedTrendOn ||
    niftyPane.trendBullOn !== niftyPane.savedTrendBullOn ||
    niftyPane.trendBearOn !== niftyPane.savedTrendBearOn ||
    niftyPane.comboOn !== niftyPane.savedComboOn ||
    niftyPane.paneOn !== niftyPane.savedPaneOn;
  niftyPaneRenderSaveMsg();
}

// Short label of the currently drafted BB%b confirmation mode in the pane. The
// NIFTY direction always comes from NIFTY Trend Following; BB%b only confirms the
// matching side.
function niftyPaneModeLabel() {
  if (!niftyPane.paneOn) return "section OFF (inert)";
  if (niftyPane.comboOn) {
    return "COMBO confirm (trend BULLISH + line BULLISH + %B oversold -> CE; trend BEARISH + line BEARISH + %B overbought -> PE)";
  }
  if (niftyPane.zoneOn && niftyPane.trendOn) {
    return "EITHER confirm (OS " + (niftyPane.osOn ? "on" : "off") + " or line BULLISH -> CE; OB " + (niftyPane.obOn ? "on" : "off") + " or line BEARISH -> PE)";
  }
  if (niftyPane.trendOn) {
    return "BB%b LINE confirm (CE " + (niftyPane.trendBullOn ? "on" : "off") + " / PE " + (niftyPane.trendBearOn ? "on" : "off") + ")";
  }
  if (niftyPane.zoneOn) {
    return "OB/OS zone confirm (" + (niftyPane.obOn ? "PE/OB on" : "PE/OB off") + " / " + (niftyPane.osOn ? "CE/OS on" : "CE/OS off") + ")";
  }
  return "no BB%b mode (Trend Following direction)";
}

function niftyPaneRenderSaveMsg() {
  const m = document.getElementById("rtNiftyZoneSaveMsg");
  if (!m) return;
  const ob = num(niftyPane.overbought).toFixed(2);
  const os = num(niftyPane.oversold).toFixed(2);
  if (niftyPane.zoneDirty) {
    m.textContent = "Unsaved: OB " + ob + " / OS " + os + " - " + niftyPaneModeLabel() + " - Save Zones dabao.";
    m.style.color = "#ffd700";
  } else if (!niftyPane.savedPaneOn) {
    m.textContent = "Saved: BB%b (Pane) section OFF - poora section inert, koi BB%b confirmation nahi.";
    m.style.color = "#888";
  } else if (niftyPane.savedComboOn) {
    m.textContent = "Saved & active: COMBO confirm - CE jab NIFTY Trend Following BULLISH + BB%b line BULLISH + %B oversold; PE jab trend BEARISH + line BEARISH + %B overbought. Dono (line + zone) match honge tabhi trade. Direction kewal NIFTY Trend Following se.";
    m.style.color = "#ab47bc";
  } else if (niftyPane.savedZoneOn && niftyPane.savedTrendOn) {
    m.textContent = "Saved & active: EITHER confirm - CE jab OS zone " + (niftyPane.savedOsOn ? "ON" : "OFF (blocked)") + " ya line BULLISH; PE jab OB zone " + (niftyPane.savedObOn ? "ON" : "OFF (blocked)") + " ya line BEARISH. Direction kewal NIFTY Trend Following se.";
    m.style.color = "#ab47bc";
  } else if (niftyPane.savedTrendOn) {
    m.textContent = "Saved & active: BB%b line confirm - CE " + (niftyPane.savedTrendBullOn ? "ON" : "OFF (blocked)") + ", PE " + (niftyPane.savedTrendBearOn ? "ON" : "OFF (blocked)") + ". NIFTY Trend Following ke side ke saath line match honi chahiye.";
    m.style.color = "#64b5f6";
  } else if (niftyPane.savedZoneOn) {
    m.textContent = "Saved & active: OB/OS zone confirm - PE on OB " + (niftyPane.savedObOn ? "ON" : "OFF (blocked)") + ", CE on OS " + (niftyPane.savedOsOn ? "ON" : "OFF (blocked)") + ". NIFTY Trend Following ke side ke saath zone match hona chahiye.";
    m.style.color = "#00d4aa";
  } else {
    m.textContent = "Saved: BB%b (Pane) ON par koi mode OFF - OB " + ob + " / OS " + os + ". Zone ya line-trend ON karke Save karo. Dono ON = EITHER confirm, Combo = dono zaroori.";
    m.style.color = "#888";
  }
}

// Commit the draft (master switch + values + per-zone enables + zone/line-trend
// modes) in a single settings write. The section master is a hard gate: with it
// OFF the zone/line-trend modes are strictly ignored by the engine. The NIFTY
// direction always comes from NIFTY Trend Following; these modes only CONFIRM
// that the chosen side matches. Zone and line-trend are independent; with BOTH ON
// EITHER may confirm. The dedicated Combo switch requires BOTH (line + zone):
// bullish line + oversold zone -> CE, bearish line + overbought zone -> PE.
function niftyPaneSaveZones(obIn, osIn, zoneCb) {
  let ob = Math.max(0, Math.min(1, num(obIn && obIn.value)));
  let os = Math.max(0, Math.min(1, num(osIn && osIn.value)));
  const pane = !!niftyPane.paneOn;
  const trend = !!niftyPane.trendOn;
  const trendBull = !!niftyPane.trendBullOn;
  const trendBear = !!niftyPane.trendBearOn;
  const combo = !!niftyPane.comboOn;
  let on = zoneCb ? !!zoneCb.checked : niftyPane.zoneOn;
  if (pane && !combo && trend && !trendBull && !trendBear) {
    const m = document.getElementById("rtNiftyZoneSaveMsg");
    if (m) {
      m.textContent = "Line-trend mode ON par dono side OFF hain - kam se kam ek side (Top Gainers CE ya Top Losers PE) ON karo warna koi trend trade nahi banegi.";
      m.style.color = "#ef5350";
    }
    return;
  }
  if (pane && combo && !niftyPane.obOn && !niftyPane.osOn) {
    const m = document.getElementById("rtNiftyZoneSaveMsg");
    if (m) {
      m.textContent = "COMBO ON par dono zone OFF hain - kam se kam ek zone (Top Losers PE on OB ya Top Gainers CE on OS) enable karo warna combo se koi trade nahi banegi.";
      m.style.color = "#ef5350";
    }
    return;
  }
  if (pane && (on || combo)) {
    if (ob <= os) {
      const m = document.getElementById("rtNiftyZoneSaveMsg");
      if (m) {
        m.textContent = "Overbought (" + ob.toFixed(2) + ") oversold (" + os.toFixed(2) + ") se upar hona chahiye - sahi karke save karo.";
        m.style.color = "#ef5350";
      }
      return;
    }
  }
  niftyPane.overbought = ob;
  niftyPane.oversold = os;
  niftyPane.zoneOn = on;
  niftyPaneSetInput("ob", ob);
  niftyPaneSetInput("os", os);
  const s = Object.assign({}, (STATE && STATE.settings) || {}, {
    bbpctPaneOn: pane,
    bbpctTrendOn: trend,
    bbpctTrendBullOn: trendBull,
    bbpctTrendBearOn: trendBear,
    bbpctComboOn: combo,
    bbpctZoneOn: on,
    bbpctOverbought: Math.round(ob * 1000) / 1000,
    bbpctOversold: Math.round(os * 1000) / 1000,
    bbpctObOn: !!niftyPane.obOn,
    bbpctOsOn: !!niftyPane.osOn,
  });
  API.settings(s).then(() => {
    niftyPane.savedPaneOn = pane;
    niftyPane.savedTrendOn = trend;
    niftyPane.savedTrendBullOn = trendBull;
    niftyPane.savedTrendBearOn = trendBear;
    niftyPane.savedComboOn = combo;
    niftyPane.savedZoneOn = on;
    niftyPane.savedOb = ob;
    niftyPane.savedOs = os;
    niftyPane.savedObOn = niftyPane.obOn;
    niftyPane.savedOsOn = niftyPane.osOn;
    niftyPane.zoneDirty = false;
    niftyPaneRenderSaveMsg();
    drawNiftyBbPctPane();
  });
}

// Visible bar count for `n`. Defaults to a readable ~150-candle window and is
// clamped so the operator can never zoom past the available data.
function niftyPaneVisibleCount(n) {
  if (n <= 0) return 0;
  let c = niftyPane.count > 0 ? niftyPane.count : Math.min(n, 150);
  c = Math.round(Math.max(10, Math.min(n, c)));
  return c;
}

// Window start/count for the current view, honouring follow (live edge) unless
// the operator has panned/zoomed, and clamping `start` to the data bounds.
function niftyPaneWindow(n) {
  const count = niftyPaneVisibleCount(n);
  const maxStart = Math.max(0, n - count);
  let start;
  if (niftyPane.follow || niftyPane.drag) {
    start = maxStart;
  } else {
    start = Math.max(0, Math.min(niftyPane.start, maxStart));
  }
  niftyPane.count = count;
  niftyPane.start = start;
  return { start, count };
}

function niftyPaneGlobalMove(e) {
  const cv = document.getElementById("rtNiftyPaneCanvas");
  if (!cv) return;
  // Dragging an OB/OS zone line updates its value + field live.
  if (niftyPane.zoneDrag) {
    const rect = cv.getBoundingClientRect();
    const top = 6;
    const bot = rect.height - 6;
    let v = (bot - (e.clientY - rect.top)) / (bot - top);
    v = Math.max(0, Math.min(1, Math.round(v * 1000) / 1000));
    if (niftyPane.zoneDrag === "ob") {
      niftyPane.overbought = v;
      niftyPaneSetInput("ob", v);
    } else {
      niftyPane.oversold = v;
      niftyPaneSetInput("os", v);
    }
    niftyPaneMarkDirty();
    drawNiftyBbPctPane();
    return;
  }
  if (!niftyPane.drag) return;
  const n = niftyPane.points.length;
  if (n < 2) return;
  const count = niftyPaneVisibleCount(n);
  const barW = (cv.clientWidth || 300) / count;
  const dx = e.clientX - niftyPane.drag.x;
  niftyPane.count = count;
  niftyPane.follow = false;
  niftyPane.start = Math.max(0, Math.min(niftyPane.drag.start - dx / barW, Math.max(0, n - count)));
  drawNiftyBbPctPane();
}

function niftyPaneGlobalUp() {
  const cv = document.getElementById("rtNiftyPaneCanvas");
  if (niftyPane.zoneDrag) {
    niftyPane.zoneDrag = null;
    if (cv) cv.style.cursor = "grab";
    niftyPaneMarkDirty();
    drawNiftyBbPctPane();
    return;
  }
  if (!niftyPane.drag) return;
  niftyPane.drag = null;
  if (cv) cv.style.cursor = "grab";
  drawNiftyBbPctPane();
}

// Wheel = zoom in/out around the cursor (the candle under the pointer stays put).
function niftyPaneZoom(e) {
  e.preventDefault();
  const cv = document.getElementById("rtNiftyPaneCanvas");
  if (!cv) return;
  const n = niftyPane.points.length;
  if (n < 2) return;
  const w = cv.clientWidth || 300;
  const x0 = 32;
  const x1 = w - 2;
  const rect = cv.getBoundingClientRect();
  const mx = Math.max(x0, Math.min(x1, e.clientX - rect.left));
  const count0 = niftyPaneVisibleCount(n);
  const frac = x1 > x0 ? (mx - x0) / (x1 - x0) : 0.5;
  const anchor = niftyPane.start + frac * count0;
  const factor = e.deltaY < 0 ? 0.85 : 1 / 0.85;
  const count = Math.round(Math.max(10, Math.min(n, count0 * factor)));
  niftyPane.count = count;
  niftyPane.follow = false;
  niftyPane.start = Math.max(0, Math.min(anchor - frac * count, Math.max(0, n - count)));
  drawNiftyBbPctPane();
}

async function refreshNiftyBbPctPane() {
  if (!document.getElementById("rtNiftyPaneWrap")) return;
  // Pause while the Paper 3 frame is hidden behind another parent tab.
  const root = document.getElementById("tab-realtime");
  if (root && !root.classList.contains("active")) return;
  let d = null;
  try { d = await API.niftyPane(); } catch (_) { return; }
  if (!d || !d.ok) return;
  const tf = document.getElementById("rtNiftyPaneTf");
  if (tf) tf.textContent = "NIFTY spot \u00b7 " + (d.tf === "1min" ? "1 min" : "5 min");
  // The section master switch owns this whole pane: with it OFF nothing here is
  // live, so the header must never read as an active trend readout (the NIFTY
  // Trend Following master is a separate feature and is shown in its own row).
  const paneOn = d.paneOn == null ? false : !!d.paneOn;
  const st = document.getElementById("rtNiftyPaneState");
  if (st) {
    st.textContent = paneOn ? "BB%b (Pane): ON" : "BB%b (Pane): OFF (inactive)";
    st.style.color = paneOn ? "#64b5f6" : "#666";
  }
  const last = document.getElementById("rtNiftyPaneLast");
  if (last) {
    if (!paneOn) {
      last.textContent = "--";
      last.style.color = "#666";
    } else {
      last.textContent = d.last == null
        ? "--"
        : num(d.last).toFixed(3) + "  (" + (num(d.last) * 100).toFixed(1) + "%)";
      last.style.color = d.last == null ? "#888" : num(d.last) >= 0.5 ? "#00d4aa" : "#ef5350";
    }
  }
  // OB/OS zones: `d.*` is the SAVED state the engine trades on. Pull it into the
  // saved cache always; only copy it into the editable draft/DOM when the
  // operator has no unsaved edits, so a poll never clobbers a work-in-progress.
  niftyPane.savedZoneOn = !!d.zoneOn;
  niftyPane.savedOb = d.overbought == null ? 1 : num(d.overbought);
  niftyPane.savedOs = d.oversold == null ? 0 : num(d.oversold);
  niftyPane.savedObOn = d.obOn == null ? true : !!d.obOn;
  niftyPane.savedOsOn = d.osOn == null ? true : !!d.osOn;
  niftyPane.savedTrendOn = !!d.trendOn;
  niftyPane.savedTrendBullOn = d.trendBullOn == null ? true : !!d.trendBullOn;
  niftyPane.savedTrendBearOn = d.trendBearOn == null ? true : !!d.trendBearOn;
  niftyPane.savedComboOn = !!d.comboOn;
  niftyPane.savedPaneOn = d.paneOn == null ? false : !!d.paneOn;
  if (!niftyPane.zoneDrag && !niftyPane.zoneDirty) {
    niftyPane.zoneOn = niftyPane.savedZoneOn;
    niftyPane.overbought = niftyPane.savedOb;
    niftyPane.oversold = niftyPane.savedOs;
    niftyPane.obOn = niftyPane.savedObOn;
    niftyPane.osOn = niftyPane.savedOsOn;
    niftyPane.trendOn = niftyPane.savedTrendOn;
    niftyPane.trendBullOn = niftyPane.savedTrendBullOn;
    niftyPane.trendBearOn = niftyPane.savedTrendBearOn;
    niftyPane.comboOn = niftyPane.savedComboOn;
    niftyPane.paneOn = niftyPane.savedPaneOn;
    const paneOnCb = document.getElementById("rtNiftyPaneOnCb");
    if (paneOnCb) paneOnCb.checked = niftyPane.paneOn;
    const cb = document.getElementById("rtNiftyZoneCb");
    if (cb) cb.checked = niftyPane.zoneOn;
    const obOnCb = document.getElementById("rtNiftyObOnCb");
    if (obOnCb) obOnCb.checked = niftyPane.obOn;
    const osOnCb = document.getElementById("rtNiftyOsOnCb");
    if (osOnCb) osOnCb.checked = niftyPane.osOn;
    const trendCb = document.getElementById("rtNiftyBbTrendCb");
    if (trendCb) trendCb.checked = niftyPane.trendOn;
    const trendBullCb = document.getElementById("rtNiftyTrendBullCb");
    if (trendBullCb) trendBullCb.checked = niftyPane.trendBullOn;
    const trendBearCb = document.getElementById("rtNiftyTrendBearCb");
    if (trendBearCb) trendBearCb.checked = niftyPane.trendBearOn;
    const comboCb = document.getElementById("rtNiftyComboCb");
    if (comboCb) comboCb.checked = niftyPane.comboOn;
    niftyPaneSetInput("ob", niftyPane.overbought);
    niftyPaneSetInput("os", niftyPane.oversold);
  }
  niftyPaneRenderSaveMsg();
  niftyPaneUpdateDisabled();
  niftyPane.dir = num(d.dir);
  niftyPane.lineDir = num(d.lineDir);
  niftyPane.zoneDir = num(d.zoneDir);
  // The NIFTY direction comes from NIFTY Trend Following; the BB%b modes only
  // confirm that the chosen side matches. The badge shows whether the live line /
  // zone currently confirms the trend's side.
  const zd = document.getElementById("rtNiftyPaneZoneDir");
  if (zd) {
    const combo = niftyPane.savedComboOn;
    const trendOn = niftyPane.savedTrendOn;
    const zoneOn = niftyPane.savedZoneOn;
    if (!niftyPane.savedPaneOn || (!combo && !trendOn && !zoneOn)) {
      zd.textContent = "";
    } else if (niftyPane.dir === 0) {
      zd.textContent = "NIFTY Trend Following: waiting (no committed direction)";
      zd.style.color = "#888";
    } else {
      const bull = niftyPane.dir > 0;
      const want = bull ? 1 : -1;
      const lineToggle = bull ? niftyPane.savedTrendBullOn : niftyPane.savedTrendBearOn;
      const zoneToggle = bull ? niftyPane.savedOsOn : niftyPane.savedObOn;
      const lineOk = trendOn && lineToggle && niftyPane.lineDir === want;
      const zoneOk = zoneOn && zoneToggle && niftyPane.zoneDir === want;
      const confirmed = combo ? (lineOk && zoneOk) : (lineOk || zoneOk);
      const side = bull ? "BULLISH" : "BEARISH";
      const target = bull ? "Top Gainers (CE)" : "Top Losers (PE)";
      if (confirmed) {
        zd.textContent = "Trend " + side + " + BB%b confirm \u2192 " + target;
        zd.style.color = bull ? "#00d4aa" : "#ef5350";
      } else {
        zd.textContent = "Trend " + side + " \u2192 " + target + " BLOCKED (BB%b confirm nahi)";
        zd.style.color = "#ffd700";
      }
    }
  }
  drawNiftyBbPctPane(d.points || []);
}

function drawNiftyBbPctPane(points) {
  if (Array.isArray(points)) niftyPane.points = points.filter((p) => p && p[1] != null);
  const cv = document.getElementById("rtNiftyPaneCanvas");
  if (!cv) return;
  const dpr = window.devicePixelRatio || 1;
  const w = cv.clientWidth || 300;
  const h = cv.clientHeight || 160;
  if (cv.width !== Math.round(w * dpr) || cv.height !== Math.round(h * dpr)) {
    cv.width = Math.round(w * dpr);
    cv.height = Math.round(h * dpr);
  }
  const ctx = cv.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);
  // Master switch OFF = the whole pane is inert: no %B series, no OB/OS lines,
  // no confirm readout. Show a plain inactive placeholder instead so the section
  // can never look like it is still driving trades.
  if (!niftyPane.paneOn) {
    ctx.fillStyle = "#4a4a66";
    ctx.font = "bold 12px monospace";
    ctx.textAlign = "center";
    ctx.fillText("BB%b (Pane) OFF - inactive", w / 2, h / 2);
    ctx.textAlign = "start";
    return;
  }
  const pad = 6;
  const top = pad;
  const bot = h - pad;
  // Reserve a left gutter for the numeric value scale.
  const x0 = 32;
  const x1 = w - 2;
  const yOf = (v) => bot - Math.max(0, Math.min(1, v)) * (bot - top);
  // Value scale 0.00 (lower band) .. 1.00 (upper band), with decimals so every
  // line's numeric level is readable straight off the pane.
  const ticks = [0, 0.25, 0.5, 0.75, 1];
  ctx.strokeStyle = "#1e1e40";
  ctx.lineWidth = 1;
  ctx.setLineDash([3, 3]);
  ticks.forEach((v) => {
    const y = yOf(v);
    ctx.beginPath();
    ctx.moveTo(x0, y);
    ctx.lineTo(w, y);
    ctx.stroke();
  });
  ctx.setLineDash([]);
  ctx.fillStyle = "#6b6b88";
  ctx.font = "9px monospace";
  ticks.forEach((v) => ctx.fillText(v.toFixed(2), 1, yOf(v) + 3));
  // Overbought / Oversold zone lines. A line is "live" only when the master zone
  // mode is ON and its own enable switch is ticked; otherwise it is dimmed but
  // still draggable, so the operator can place it before arming.
  [[niftyPane.overbought, "#ffb74d", "OB", niftyPane.obOn], [niftyPane.oversold, "#00d4aa", "OS", niftyPane.osOn]].forEach(([v, col, tag, on]) => {
    const y = yOf(v);
    const live = (niftyPane.zoneOn || niftyPane.comboOn) && on;
    ctx.strokeStyle = col;
    ctx.lineWidth = live ? 1.6 : 1;
    ctx.globalAlpha = live ? 1 : 0.4;
    ctx.setLineDash(on ? [6, 3] : [2, 4]);
    ctx.beginPath();
    ctx.moveTo(x0, y);
    ctx.lineTo(w - 2, y);
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = col;
    ctx.font = "bold 9px monospace";
    const label = tag + " " + num(v).toFixed(2) + (on ? "" : " off");
    const tw = ctx.measureText(label).width;
    ctx.fillText(label, Math.max(x0, w - tw - 4), y - 2);
    ctx.globalAlpha = 1;
  });
  const hover = document.getElementById("rtNiftyPaneHover");
  const pts = niftyPane.points;
  const n = pts.length;
  if (n < 2) {
    if (hover) hover.textContent = "";
    ctx.fillStyle = "#666";
    ctx.font = "10px monospace";
    ctx.fillText(n ? "warming up..." : "no NIFTY data", 12, h / 2);
    return;
  }
  const { start, count } = niftyPaneWindow(n);
  const slice = pts.slice(start, start + count);
  const m = slice.length;
  const xOf = (i) => (m <= 1 ? x0 : x0 + (i / (m - 1)) * (x1 - x0));
  // Colour each segment by its slope (green rising, red falling) so the pane
  // reads like the chart's BB%b direction.
  ctx.lineWidth = 1.6;
  for (let i = 1; i < m; i++) {
    const v0 = num(slice[i - 1][1]);
    const v1 = num(slice[i][1]);
    ctx.strokeStyle = v1 >= v0 ? "#00d4aa" : "#ef5350";
    ctx.beginPath();
    ctx.moveTo(xOf(i - 1), yOf(v0));
    ctx.lineTo(xOf(i), yOf(v1));
    ctx.stroke();
  }
  // Newest visible candle: dot + its numeric value (shown in the value scale).
  const lv = num(slice[m - 1][1]);
  const lx = xOf(m - 1);
  const ly = yOf(lv);
  ctx.fillStyle = lv >= 0.5 ? "#00d4aa" : "#ef5350";
  ctx.beginPath();
  ctx.arc(lx, ly, 2.4, 0, Math.PI * 2);
  ctx.fill();
  const ltxt = lv.toFixed(3);
  ctx.font = "bold 9px monospace";
  const ltw = ctx.measureText(ltxt).width;
  const lbx = lx - ltw - 5 < x0 ? lx + 5 : lx - ltw - 5;
  ctx.fillStyle = "rgba(0,0,0,0.75)";
  ctx.fillRect(lbx - 2, ly - 12, ltw + 4, 11);
  ctx.fillStyle = lv >= 0.5 ? "#00d4aa" : "#ef5350";
  ctx.fillText(ltxt, lbx, ly - 3);
  // Crosshair + hovered value (numeric, same scale as the fields).
  let hx = null;
  if (niftyPane.hoverX != null) {
    hx = Math.max(x0, Math.min(w, niftyPane.hoverX));
  }
  if (hx != null && m > 1) {
    const idx = Math.round(((hx - x0) / (x1 - x0)) * (m - 1));
    const ci = Math.max(0, Math.min(m - 1, idx));
    const cx = xOf(ci);
    const hv = num(slice[ci][1]);
    ctx.strokeStyle = "#555";
    ctx.setLineDash([2, 2]);
    ctx.beginPath();
    ctx.moveTo(cx, top);
    ctx.lineTo(cx, bot);
    ctx.stroke();
    ctx.setLineDash([]);
    const txt = hv.toFixed(3);
    const tw = ctx.measureText(txt).width;
    const bx = Math.min(w - tw - 6, Math.max(x0, cx + 4));
    ctx.fillStyle = "rgba(0,0,0,0.75)";
    ctx.fillRect(bx, top + 1, tw + 6, 13);
    ctx.fillStyle = hv >= 0.5 ? "#00d4aa" : "#ef5350";
    ctx.fillText(txt, bx + 3, top + 11);
    if (hover) {
      hover.textContent = "cursor: " + txt;
      hover.style.color = hv >= 0.5 ? "#00d4aa" : "#ef5350";
    }
  } else if (hover) {
    hover.textContent = "";
  }
}

function renderSnapshot(s) {
  if (!s) return;
  STATE = s;
  if (!methodInit && s.method) {
    activeMethod = s.method;
    methodInit = true;
    buildMethodCards();
  }

  const on = !!s.engineOn;
  const armed = !!s.armed;
  paintEnginePills(on, armed);
  updateBalanceReadouts();

  // apply settings to the engine-control checkboxes/inputs
  applySettingsToDom(s.settings);
  // Per-engine broker routing row (hidden on the paper books).
  renderBrokerRow();

  // Smart P&L summary: the engine panes show the LAST TRADING DAY (the newest IST
  // day that has a closed trade) - the full history lives in the Trade Stats tabs.
  // `st` is the all-time closed book used only as a fallback until the
  // `range=today` (now "last trading day") stats land; once DAY_STATS is available
  // it drives realized / win-rate / trade counts / charges. Live gross P&L stays
  // the last trading day's realized plus the currently open (unrealized) book.
  const st = s.stats || {};
  const day = DAY_STATS;
  const sum = document.getElementById("rtSummary");
  if (sum) {
    const total = day && day.n != null ? num(day.n) : num(st.total);
    const wins = day && day.wins != null ? num(day.wins) : num(st.wins);
    const losses = day && day.losses != null ? num(day.losses) : Math.max(0, total - wins);
    const realized = day && day.net != null ? num(day.net) : num(st.realized);
    const unrealized = num(st.unrealized);
    const live = realized + unrealized;
    const wr = total ? (wins / total) * 100 : 0;
    const charges = day && day.charges != null ? num(day.charges) : num(st.charges);
    const chOn = !!st.chargesOn;
    const card = (label, val, color) =>
      `<div style="flex:1;min-width:130px;border:1px solid #1e1e40;border-radius:4px;background:#12122a;padding:5px 9px">` +
      `<div style="font-size:9px;color:#888;white-space:nowrap">${label}</div>` +
      `<div style="font-size:14px;font-weight:700;color:${color};white-space:nowrap">${val}</div></div>`;
    sum.innerHTML = [
      card("Smart Live P&L (Last day)", (live >= 0 ? "+" : "-") + fmtMoney(Math.abs(live)), live >= 0 ? "#00d4aa" : "#ef5350"),
      card("Smart Realized P&L (Last day)", (realized >= 0 ? "+" : "-") + fmtMoney(Math.abs(realized)), realized >= 0 ? "#00d4aa" : "#ef5350"),
      card("Last-day Win Rate", wr.toFixed(2) + "%", wr >= 50 ? "#00d4aa" : "#ff9800"),
      card("Last-day Trades (W/L)", `${total} (${wins}W / ${losses}L)`, "#d0d0d0"),
      card("Last-day Charges", (chOn ? "-" : "") + fmtMoney(charges), "#ff9800"),
      `<div style="flex:0 0 auto;display:flex;align-items:center;padding:2px">` +
        `<button class="btn-action warn" data-smartreset="1" style="width:auto;padding:7px 14px;margin:0;font-size:10px" ` +
        `title="Sab Smart P&L zero karo: closed trades + logs${PAPER ? " + open paper positions" : ""} clear ho jayenge. Undo nahi hoga.">Reset</button></div>`,
    ].join("");
    const rb = sum.querySelector("[data-smartreset]");
    if (rb) {
      rb.onclick = async () => {
        const msg = "Reset all Smart P&L?\n\nYe closed trades aur logs" + (PAPER ? " aur open paper positions" : "") + " clear kar dega. Undo nahi hoga.";
        if (!(await uiConfirm(msg, { danger: true }))) return;
        await API.reset({ what: "smart" });
        resetBookCaches();
        refresh();
      };
    }
  }

  syncCards();
  renderMarginBar(s.margin);
  renderRunning(s);
  renderClosed(s);
  // Pull the full ledger once whenever the closed count changes (first load or a
  // fresh close); the 1s poll itself only ever ships the newest slice.
  const closedTotal = s.closedCount != null ? num(s.closedCount) : null;
  if (closedTotal == null && CLOSED_CACHE == null) loadClosed();
  // The on-demand ledger is authoritative and never behind the snapshot, so only
  // re-pull when it is actually missing rows (cache shorter than the count). The
  // old `!==` test re-fetched the whole multi-MB ledger every second whenever the
  // two briefly disagreed, which alone could freeze the pane.
  else if (closedTotal != null && (!CLOSED_CACHE || CLOSED_CACHE.length < closedTotal)) loadClosed();
  // Keep the TODAY-only summary fresh: reload day stats whenever the closed
  // count moves (or on the slow throttle inside loadDayStats).
  loadDayStats(closedTotal);
  renderHoldings(s);
  loadPool();
  loadScanners();
  loadAstSections();
  renderConditionLog();
  renderBlockers(s);
  renderQuickRun();
  renderStrikes();
  // Paper Trade 3 only: mirror the NIFTY-trend pane state to the parent chart.
  syncNiftyBbPctPane(s);
}

// Live Top Movers + NIFTY trend readouts (auto CE/PE side source).
async function loadScanners() {
  // Fast readouts: the Top Movers / NIFTY trend lists must track the engine's
  // 100ms scan so a fresh pick is visible almost immediately (not 2.5s late).
  const now = Date.now();
  if (now - scannerPollAt < 250) return;
  scannerPollAt = now;
  const cfg = (STATE && STATE.settings) || {};
  const ml = document.getElementById("rtMoversList");
  const tl = document.getElementById("rtNiftyTrendList");
  const cl = document.getElementById("rtCommodityStatus");
  const symName = (id) => {
    const s = CATALOG.symbols.find((x) => num(x.id) === num(id));
    return s ? s.name : id;
  };
  // The three scanners are fetched unconditionally (the server rate-limits each
  // to its own cadence), so turning a master toggle on shows data immediately
  // instead of waiting for the next poll, and the commodity picker is always
  // populated regardless of the master toggle.
  //
  // Liquid F&O readout is fetched FIRST and isolated in its own try/catch: the
  // Top Movers endpoint can be slow (or briefly fail) and used to sit ahead of
  // this block in one shared try, which starved #rtLiquidList - the readout
  // never rendered while movers was awaiting. Each scanner now stands alone.
  if (!PAPER2) {
    try {
      await loadLiquidUniverse();
      const lq = await API.liquid();
      renderLiquidReadout(lq);
    } catch (_e) { /* best-effort */ }
    syncLiquidSelects(cfg);
  }
  try {
    const m = await API.movers();
    if (ml && cfg.moversOn && !cfg.manualStrikesEnabled) {
      ml.style.display = "block";
      const chip = (r) =>
        `<span style="display:inline-flex;align-items:center;gap:3px;background:#16163a;border:1px solid #2d2d50;border-radius:8px;padding:1px 4px;margin:1px">` +
        `<span>${esc(symName(r.securityId))} (${num(r.changePct).toFixed(2)}%)</span>` +
        `<button data-sc-remove="${num(r.securityId)}" data-sc-name="${esc(symName(r.securityId))}" title="Remove - engine will not pick/trade this" style="background:none;border:none;color:#ef5350;cursor:pointer;font-size:11px;padding:0 2px;line-height:1">\u00d7</button></span>`;
      const fmt = (rows) => (rows || []).map(chip).join(" ") || "--";
      ml.innerHTML =
        `<b style="color:#00d4aa">Gainers</b> ${fmt(m && m.gainers)}<br>` +
        `<b style="color:#ef5350">Losers</b> ${fmt(m && m.losers)}<br>` +
        `<span style="color:#888">Bias: ${m && m.bias > 0 ? "gainers lead -> CE" : m && m.bias < 0 ? "losers lead -> PE" : "balanced"}` +
        ` (up ${(m && m.up) || 0} / down ${(m && m.down) || 0})</span>`;
      ml.querySelectorAll("[data-sc-remove]").forEach((b) => {
        b.onclick = (e) => {
          e.preventDefault();
          removeScannerPick(num(b.getAttribute("data-sc-remove")), b.getAttribute("data-sc-name"));
        };
      });
    } else if (ml) {
      ml.style.display = "none";
    }

    const t = await API.trend();
    if (tl && cfg.niftyTrendOn && !cfg.manualStrikesEnabled) {
      tl.style.display = "block";
      const dirTxt =
        t && t.dir
          ? `<b class="${t.dir > 0 ? "rt-pos" : "rt-neg"}">${t.dir > 0 ? "BULLISH (CE)" : "BEARISH (PE)"}</b>`
          : "neutral / computing... (needs Dhan connection + 35 candles)";
      const picks = (t && t.picks) || [];
      const pfmt = picks
        .map(
          (p) =>
            `<span style="display:inline-flex;align-items:center;gap:3px;background:#16163a;border:1px solid #2d2d50;border-radius:8px;padding:1px 4px;margin:1px">` +
            `<span>${esc(symName(p.securityId))} (${num(p.changePct).toFixed(2)}%)</span>` +
            `<button data-sc-remove="${num(p.securityId)}" data-sc-name="${esc(symName(p.securityId))}" title="Remove - engine will not pick/trade this" style="background:none;border:none;color:#ef5350;cursor:pointer;font-size:11px;padding:0 2px;line-height:1">\u00d7</button></span>`
        )
        .join(" ");
      tl.innerHTML =
        `Direction: ${dirTxt}<br>` +
        `<span style="color:#888">Picks (${picks.length}):</span> ${pfmt || "(none qualify yet)"}`;
      tl.querySelectorAll("[data-sc-remove]").forEach((b) => {
        b.onclick = (e) => {
          e.preventDefault();
          removeScannerPick(num(b.getAttribute("data-sc-remove")), b.getAttribute("data-sc-name"));
        };
      });
    } else if (tl) {
      tl.style.display = "none";
    }

    const c = await API.commodities();
    populateCommodityAdd(c);
    if (cl) {
      if (cfg.commodityOn) {
        const selected = (cfg.commodityList || []).map(num).filter((x) => x > 0);
        const byId = {};
        ((c && c.commodities) || []).forEach((x) => (byId[num(x.security_id)] = x.name));
        const names = selected.length ? selected.map((id) => byId[id] || id) : [];
        cl.textContent = names.length
          ? "ON - " + names.join(", ")
          : "ON - trading the default MCX futures set (add specific ids to narrow)";
      } else {
        cl.textContent = "Add MCX commodity futures to trade them directly.";
      }
    }
  } catch (e) {
    /* scanner readout is best-effort */
    console.warn("loadScanners failed", e);
  }
}

// ---- Liquid F&O stocks (Realtime / Paper Trade 1) --------------------------
// One scanner beside Top Movers that ranks ONLY the liquid index basket, plus
// three dropdowns (top gainer -> CE, top loser -> PE, add any basket stock) and
// a removable chip row. Side rule matches the rest of the engine: an explicit
// gainer pins CE, an explicit loser pins PE, and a hand-added stock builds both
// legs so the indicator filters choose.
function liquidName(id) {
  const n = liquidNames[num(id)];
  if (n) return n;
  const s = CATALOG.symbols.find((x) => num(x.id) === num(id));
  return s ? s.name : String(id);
}
function liquidIds(key) {
  const inp = document.querySelector(`#tab-realtime [data-list="${key}"]`);
  return String((inp && inp.value) || "")
    .split(",")
    .map((x) => num(x.trim()))
    .filter((x) => x > 0);
}
function liquidSetIds(key, ids) {
  const inp = document.querySelector(`#tab-realtime [data-list="${key}"]`);
  if (inp) inp.value = ids.join(", ");
}
async function loadLiquidUniverse() {
  // Equity Indicator Based Trade: the "universe" is the daily-scan ranked list,
  // which changes every scan, so it is refreshed on every call (not cached like
  // the static F&O basket below).
  if (EQUITY_ONLY) {
    try {
      // Engine-scoped endpoint: inside the paper4 frame `/api/rt/liquid` is
      // rewritten to `/api/paper4/liquid`, which serves the daily equity scan
      // (the global `/api/liquid` serves the static F&O basket instead).
      const r = await get("/api/rt/liquid");
      const stocks = ((r && r.stocks) || []).slice();
      stocks.forEach((x) => (liquidNames[num(x.securityId)] = x.name || String(x.securityId)));
      LIQUID_STOCKS = stocks;
      const sel = document.getElementById("rtEqScanSelect");
      if (sel) {
        const ae = document.activeElement;
        const keep = sel === ae ? sel.value : null;
        sel.innerHTML =
          `<option value="">-- pick stock --</option>` +
          stocks.map((x) => `<option value="${esc(x.securityId)}">${esc(x.name)} · ₹${num(x.turnoverCr).toFixed(1)}cr</option>`).join("");
        if (keep) sel.value = keep;
      }
      const info = document.getElementById("rtEqScanInfo");
      if (info) {
        if (r && r.stocks && r.stocks.length) {
          info.textContent =
            `Scanned ${num(r.scanned) || stocks.length} stocks` +
            (r.day ? " (today)" : "") +
            ` · top ${num(r.tradeCount) || num(((STATE && STATE.settings) || {}).liquidTradeCount) || ""} traded` +
            (r.minTurnoverCr != null ? ` · min ₹${num(r.minTurnoverCr)}cr` : "");
        } else if (r && r.waiting) {
          info.textContent = "Waiting for scrip master / Dhan feed - scan will run automatically.";
        } else {
          info.textContent = r && r.scheduled ? `Daily scan scheduled at ${r.scheduled} IST - or click "Scan now".` : "";
        }
      }
      info && (info.style.display = "");
      return;
    } catch (e) {
      console.warn("equity liquidity scan load failed", e);
      return;
    }
  }
  if (liquidPopulated) return;
  try {
    const r = await get("/api/liquid");
    LIQUID_STOCKS = ((r && r.stocks) || []).slice();
    LIQUID_STOCKS.forEach((x) => (liquidNames[num(x.securityId)] = x.name || String(x.securityId)));
    liquidPopulated = LIQUID_STOCKS.length > 0;
  } catch (e) {
    console.warn("liquid universe load failed", e);
    return;
  }
  const opts =
    `<option value="">-- pick stock --</option>` +
    LIQUID_STOCKS.map((x) => `<option value="${esc(x.securityId)}">${esc(x.name)}</option>`).join("");
  ["rtLiquidGainerSelect", "rtLiquidLoserSelect", "rtLiquidAddSelect"].forEach((id) => {
    const sel = document.getElementById(id);
    if (sel && sel.options.length <= 1) sel.innerHTML = opts;
  });
}
function syncLiquidSelects(s) {
  const ae = document.activeElement;
  // Only mirror the saved pick once the universe options actually exist:
  // assigning a value to a <select> that has no matching <option> silently
  // blanks it, and the next settings save would then persist 0.
  const set = (id, want) => {
    const sel = document.getElementById(id);
    if (!sel || sel === ae || sel.options.length <= 1) return;
    const v = num(want) > 0 ? String(num(want)) : "";
    if (v && !Array.from(sel.options).some((o) => o.value === v)) return;
    if (sel.value !== v) sel.value = v;
  };
  set("rtLiquidGainerSelect", s.liquidGainer);
  set("rtLiquidLoserSelect", s.liquidLoser);
}
function renderLiquidChips() {
  const box = document.getElementById("rtLiquidChips");
  if (!box) return;
  const ids = liquidIds("liquidList");
  box.innerHTML = ids.length
    ? ids
        .map(
          (id) =>
            `<span style="display:inline-flex;align-items:center;gap:4px;background:#16163a;border:1px solid #2d2d50;border-radius:8px;padding:1px 4px;margin:1px">${esc(liquidName(id))}` +
            `<button data-liquid-remove="${id}" title="Remove - engine will stop trading this stock" style="background:none;border:none;color:#ef5350;cursor:pointer;font-size:11px;padding:0 2px;line-height:1">\u00d7</button></span>`
        )
        .join("")
    : `<span style="color:#666">None added.</span>`;
  box.querySelectorAll("[data-liquid-remove]").forEach((b) => {
    b.onclick = (e) => {
      e.preventDefault();
      const id = num(b.getAttribute("data-liquid-remove"));
      liquidSetIds("liquidList", liquidIds("liquidList").filter((x) => x !== id));
      API.settings(settingsFromDom()).then(() => {
        renderLiquidChips();
        refresh();
      });
    };
  });
}
/// Remove a stock the scanner auto-picked (top gainer / top loser / manual add)
/// by adding it to the shared scanner blacklist, exactly like the Top Movers
/// chips do.
function removeLiquidPick(id, name) {
  id = num(id);
  if (!(id > 0)) return;
  const ids = scannerExcludedIds();
  if (ids.indexOf(id) >= 0) return;
  ids.push(id);
  setScannerExclude(ids);
  const s = (STATE && STATE.settings) || {};
  const patch = {};
  if (num(s.liquidGainer) === id) patch.liquidGainer = 0;
  if (num(s.liquidLoser) === id) patch.liquidLoser = 0;
  if (Object.keys(patch).length) API.settings(settingsFromDom()).then(() => refresh());
  if (typeof linkToast === "function") linkToast("ok", `${name || liquidName(id)} liquid scan se hata diya`);
}
function renderLiquidReadout(m) {
  const box = document.getElementById("rtLiquidList");
  if (!box) return;
  const cfg = (STATE && STATE.settings) || {};
  if (!cfg.liquidOn || cfg.manualStrikesEnabled) {
    box.style.display = "none";
    return;
  }
  box.style.display = "block";
  // Equity Indicator Based Trade: render the ranked daily-scan stocks (turnover
  // x growth), not the F&O gainers/losers option leg readout.
  if (EQUITY_ONLY) {
    const rows = (m && m.stocks) || [];
    const cap = (m && m.tradeCount) || num(cfg.liquidTradeCount) || 0;
    const chip = (r) =>
      `<span style="display:inline-flex;align-items:center;gap:3px;background:#16163a;border:1px solid #2d2d50;border-radius:8px;padding:1px 4px;margin:1px">` +
      `<span>${esc(r.name || liquidName(r.securityId))} ` +
      `<span style="color:#888">· ₹${num(r.turnoverCr).toFixed(1)}cr</span>` +
      `${r.growthPct != null ? ` <span style="color:#00e676">+${num(r.growthPct).toFixed(0)}%</span>` : ""}` +
      ` <span style="color:${num(r.changePct) >= 0 ? "#00e676" : "#ef5350"}">${num(r.changePct).toFixed(2)}%</span></span>` +
      `<button data-liquid-pick-remove="${num(r.securityId)}" data-liquid-pick-name="${esc(r.name || liquidName(r.securityId))}" title="Remove - engine will not trade this" style="background:none;border:none;color:#ef5350;cursor:pointer;font-size:11px;padding:0 2px;line-height:1">\u00d7</button></span>`;
    const wait = m && m.waiting
      ? `<span style="color:#ffb74d">Waiting for the scrip master / Dhan feed... scan runs once it is ready.</span><br>`
      : "";
    const sched = !rows.length && m && m.scheduled
      ? `<span style="color:#888">Daily scan scheduled at ${esc(m.scheduled)} IST - click "Scan now" to run it immediately.</span>`
      : "";
    box.innerHTML =
      wait + sched +
      `<b style="color:#4fc3f7">Top ${cap} most-liquid NSE equity (traded):</b> ${rows.slice(0, Math.max(cap, 1)).map(chip).join(" ") || "--"}<br>` +
      `<span style="color:#888">${rows.length ? `Ranked ${num(m.scanned) || rows.length} stocks by turnover × growth, scanned ${m.day ? "today" : ""}. Green % = turnover growth vs previous session.` : ""}</span>`;
    box.querySelectorAll("[data-liquid-pick-remove]").forEach((b) => {
      b.onclick = (e) => {
        e.preventDefault();
        removeLiquidPick(
          num(b.getAttribute("data-liquid-pick-remove")),
          b.getAttribute("data-liquid-pick-name")
        );
      };
    });
    return;
  }
  const chip = (r) =>
    `<span style="display:inline-flex;align-items:center;gap:3px;background:#16163a;border:1px solid #2d2d50;border-radius:8px;padding:1px 4px;margin:1px">` +
    `<span>${esc(liquidName(r.securityId))} (${num(r.changePct).toFixed(2)}%)</span>` +
    `<button data-liquid-pick-remove="${num(r.securityId)}" data-liquid-pick-name="${esc(liquidName(r.securityId))}" title="Remove - engine will not pick/trade this" style="background:none;border:none;color:#ef5350;cursor:pointer;font-size:11px;padding:0 2px;line-height:1">\u00d7</button></span>`;
  const fmt = (rows) => (rows || []).map(chip).join(" ") || "--";
  const wait = m && m.waiting
    ? `<span style="color:#ffb74d">Waiting for Dhan feed... scanner armed, ranking starts once the live feed is up.</span><br>`
    : "";
  box.innerHTML =
    wait +
    `<b style="color:#00d4aa">Gainers (CE)</b> ${fmt(m && m.gainers)}<br>` +
    `<b style="color:#ef5350">Losers (PE)</b> ${fmt(m && m.losers)}<br>` +
    `<b style="color:#4fc3f7">Added</b> ${fmt(m && m.added)}<br>` +
    `<span style="color:#888">Bias: ${m && m.bias > 0 ? "gainers lead -> CE" : m && m.bias < 0 ? "losers lead -> PE" : "balanced"}</span>`;
  box.querySelectorAll("[data-liquid-pick-remove]").forEach((b) => {
    b.onclick = (e) => {
      e.preventDefault();
      removeLiquidPick(
        num(b.getAttribute("data-liquid-pick-remove")),
        b.getAttribute("data-liquid-pick-name")
      );
    };
  });
}
function wireLiquidControls() {
  // Equity Indicator Based Trade: "Scan now" forces the daily full-universe
  // liquidity scan; the scanned-stocks dropdown pins a name the same way the F&O
  // "Add stock" does (always included beyond top-N, builds a spot BUY).
  if (EQUITY_ONLY) {
    const scanBtn = document.getElementById("rtEqScanNow");
    if (scanBtn) {
      scanBtn.onclick = async () => {
        scanBtn.disabled = true;
        const old = scanBtn.textContent;
        scanBtn.textContent = "Scanning...";
        try {
          // Persist the DOM first so a freshly-typed "Stocks to trade" value is
          // the number the scan ranks/arms against, not the previous saved one.
          try {
            await API.settings(settingsFromDom());
          } catch (_e) { /* scan still runs on the saved settings */ }
          // Remember the cache stamp BEFORE the scan so we can detect the fresh
          // one. The POST returns immediately (the heavy sweep runs detached);
          // we then poll `/liquid` and keep "Scanning..." up until the stamp
          // moves, so the button reflects reality instead of flashing idle.
          let before = 0;
          try {
            const cur = await get("/api/rt/liquid");
            before = num(cur && cur.at);
          } catch (_e) { /* ignore */ }
          await post("/api/rt/liquid", { action: "scan" });
          let fresh = null;
          for (let i = 0; i < 150; i++) {
            await new Promise((r) => setTimeout(r, 2000));
            let cur = null;
            try {
              cur = await get("/api/rt/liquid");
            } catch (_e) {
              continue;
            }
            if (!cur) continue;
            // Done when the cache stamp advanced past the pre-scan value, or a
            // scan that ran this day already carries rows with a newer `at`.
            const at = num(cur.at);
            const hasRows = (cur.stocks || []).length > 0;
            if (hasRows && (!before || at > before)) {
              fresh = cur;
              break;
            }
          }
          if (fresh) {
            renderLiquidReadout(fresh);
            await loadLiquidUniverse();
            const n = (fresh.stocks && fresh.stocks.length) || 0;
            if (typeof linkToast === "function") linkToast("ok", `Liquidity scan complete: ${n} stocks ranked`);
          } else if (typeof linkToast === "function") {
            linkToast("warn", "Scan is taking longer than usual - it will update automatically when done");
          }
          refresh();
        } catch (e) {
          if (typeof linkToast === "function") linkToast("err", "Liquidity scan failed");
        } finally {
          scanBtn.disabled = false;
          scanBtn.textContent = old;
        }
      };
    }
    const scanSel = document.getElementById("rtEqScanSelect");
    if (scanSel) {
      scanSel.onchange = () => {
        const id = num(scanSel.value);
        if (!(id > 0)) return;
        const ids = liquidIds("liquidList");
        if (ids.indexOf(id) < 0) ids.push(id);
        liquidSetIds("liquidList", ids);
        const ex = scannerExcludedIds().filter((x) => x !== id);
        STATE.settings = Object.assign({}, (STATE && STATE.settings) || {}, {
          scannerExclude: ex,
          liquidList: ids,
        });
        scanSel.value = "";
        API.settings(settingsFromDom()).then(() => {
          renderLiquidChips();
          renderScannerRemoved();
          refresh();
        });
      };
    }
  }
  // Re-run on every apply: the universe load is async, so the + Add button and
  // the two pickers must be (re)bound even if they were wired before the
  // options existed (an empty <select> cannot emit a pick).
  const gt = document.getElementById("rtLiquidAddBtn");
  if (gt) {
    gt.onclick = () => {
      const sel = document.getElementById("rtLiquidAddSelect");
      const id = num(sel && sel.value);
      if (!(id > 0)) return;
      const ids = liquidIds("liquidList");
      if (ids.indexOf(id) < 0) ids.push(id);
      liquidSetIds("liquidList", ids);
      // A hand-add must also clear any blacklist entry, or the universe drops it.
      // Mirrored into STATE *before* settingsFromDom runs so the carried-over
      // list is already correct.
      const ex = scannerExcludedIds().filter((x) => x !== id);
      STATE.settings = Object.assign({}, (STATE && STATE.settings) || {}, {
        scannerExclude: ex,
        liquidList: ids,
      });
      const sel2 = document.getElementById("rtLiquidAddSelect");
      if (sel2) sel2.value = "";
      API.settings(settingsFromDom()).then(() => {
        renderLiquidChips();
        renderScannerRemoved();
        refresh();
      });
    };
  }
  const wireSelect = (id, key) => {
    const sel = document.getElementById(id);
    if (!sel) return;
    sel.onchange = () => {
      const v = num(sel.value);
      const patch = {};
      patch[key] = v > 0 ? v : 0;
      if (v > 0) {
        // An explicit pick overrides a previous "remove" for the same stock.
        const ex = scannerExcludedIds().filter((x) => x !== v);
        if (ex.length !== scannerExcludedIds().length) {
          if (STATE.settings) STATE.settings.scannerExclude = ex;
          patch.scannerExclude = ex;
          renderScannerRemoved();
        }
      }
      pushSetting(patch);
    };
  };
  wireSelect("rtLiquidGainerSelect", "liquidGainer");
  wireSelect("rtLiquidLoserSelect", "liquidLoser");
}

function populateCommodityAdd(c) {
  const all = (c && c.commodities) || [];
  all.forEach((x) => {
    COMM_NAMES[num(x.security_id)] = x.name || x.symbol || String(x.security_id);
  });
  // Paper Trade 2 can only trade commodities that list options, so the picker
  // offers option-chain commodities only.
  const rows = PAPER2 ? all.filter((x) => x.has_options) : all;
  const sel = document.getElementById("rtCommodityAdd");
  if (sel && rows.length) {
    // Repopulate whenever the count changes, not just once: on a fresh install
    // the first fetch can land while the server's contract master is still
    // warming (empty list). Once the real contracts arrive the dropdown must
    // fill in, otherwise it stays empty for the whole session.
    const key = rows.map((x) => num(x.security_id)).join(",");
    if (sel.dataset.commKey !== key) {
      sel.innerHTML =
        `<option value="">-- pick commodity --</option>` +
        rows.map((x) => `<option value="${esc(x.security_id)}">${esc(x.name)} · ${esc(x.symbol)}</option>`).join("");
      sel.dataset.commKey = key;
      commodityPopulated = true;
    }
  }
  renderChips("commodityList", "rtCommodityChips", "commodity");
}

// ---- AST dropdown pickers (Indices / Confirm indicators / Commodities) -------
function indexName(id) {
  const s = CATALOG.symbols.find((x) => num(x.id) === num(id));
  if (s) return s.name;
  if (COMM_NAMES[num(id)]) return COMM_NAMES[num(id)];
  return String(id);
}
function indicatorName(id) {
  const extra = EXTRA_CONF_INDICATORS.find((x) => x[0] === id);
  if (extra) return extra[1];
  const bb = BB_PCT_CONF_INDICATORS.find((x) => x[0] === id);
  if (bb) return bb[1];
  if (TREND_FILTER_NAME[id]) return TREND_FILTER_NAME[id];
  const i = (CATALOG.indicators || []).find((x) => x.id === id);
  return i ? i.name || i.id : id;
}
function chipLabel(kind, id) {
  if (kind === "indicator") return indicatorName(id);
  if (kind === "filterkey") return filterKeyLabel(id);
  return indexName(id);
}
function chipHTML(key, id, kind) {
  return `<span style="display:inline-flex;align-items:center;gap:4px;background:#16163a;border:1px solid #2d2d50;border-radius:8px;padding:1px 4px;margin:1px">${esc(chipLabel(kind, id))}<button data-chip-key="${esc(key)}" data-chip-id="${esc(String(id))}" style="background:none;border:none;color:#ef5350;cursor:pointer;font-size:11px;padding:0 2px;line-height:1">\u00d7</button></span>`;
}
function listValues(key) {
  const inp = document.querySelector(`#tab-realtime [data-list="${key}"]`);
  return inp ? String(inp.value || "").split(",").map((x) => x.trim()).filter(Boolean) : [];
}
function renderChips(key, containerId, kind) {
  // Index / commodity chips carry their own CE/PE leg selector, so they render
  // specially.
  if (kind === "index") { renderIndexChips(); return; }
  if (kind === "commodity") { renderCommodityChips(); return; }
  const box = document.getElementById(containerId);
  if (!box) return;
  const vals = listValues(key);
  box.innerHTML = vals.length
    ? vals.map((v) => chipHTML(key, v, kind)).join("")
    : `<span style="color:#666">None added.</span>`;
  box.querySelectorAll("[data-chip-id]").forEach((b) => {
    b.onclick = (e) => {
      e.preventDefault();
      const id = b.getAttribute("data-chip-id");
      const inp = document.querySelector(`#tab-realtime [data-list="${key}"]`);
      if (!inp) return;
      inp.value = vals.filter((x) => x !== id).join(", ");
      API.settings(settingsFromDom()).then(() => renderChips(key, containerId, kind));
    };
  });
}
// ---- CE/PE leg selectors for added Indices / Commodities --------------------
// An added index or commodity trades ONLY the leg the operator picks (CE =
// bullish view, PE = bearish view). The selected leg also drives which indicator
// filters apply on that instrument (CE -> bullish filter set, PE -> bearish).
// An index with no leg is skipped (index trading needs an explicit trend call);
// a commodity with no leg keeps the global scanner side.
function listLegValue(settingsKey, id) {
  const arr = (STATE && STATE.settings && STATE.settings[settingsKey]) || [];
  const hit = arr.find((x) => num(x && x.securityId) === num(id));
  const s = hit ? String(hit.side || "").toUpperCase() : "";
  return s === "CE" || s === "PE" ? s : "";
}
function setListLeg(settingsKey, id, side, redraw) {
  const cur = (((STATE && STATE.settings && STATE.settings[settingsKey]) || []).slice())
    .map((x) => ({ securityId: num(x && x.securityId), side: String((x && x.side) || "") }));
  const i = cur.findIndex((x) => x.securityId === num(id));
  if (side === "CE" || side === "PE") {
    if (i >= 0) cur[i] = { securityId: num(id), side: side };
    else cur.push({ securityId: num(id), side: side });
  } else if (i >= 0) {
    cur.splice(i, 1);
  }
  const patch = {};
  patch[settingsKey] = cur;
  STATE.settings = Object.assign({}, (STATE && STATE.settings) || {}, patch);
  API.settings(settingsFromDom()).then(redraw);
}
function legOptionsHTML(leg, noneLabel) {
  return [["", noneLabel || "leg: none"], ["CE", "CE"], ["PE", "PE"]]
    .map(([o, label]) => `<option value="${o}"${o === leg ? " selected" : ""}>${label}</option>`)
    .join("");
}
const INDEX_LEG_CHIPS = {
  listKey: "moversIndices",
  containerId: "rtMoversIndicesList",
  settingsKey: "moversIndexLegs",
  attr: "data-idx-leg",
  noneLabel: "leg: none (skip)",
  title: "Leg this index trades. CE = bullish filters, PE = bearish filters. No leg = the index is skipped (index trading needs an explicit trend call).",
};
const COMMODITY_LEG_CHIPS = {
  listKey: "commodityList",
  containerId: "rtCommodityChips",
  settingsKey: "commodityLegs",
  attr: "data-cmd-leg",
  noneLabel: "leg: none",
  title: "Leg this commodity trades. CE = bullish filters, PE = bearish filters. No leg = follow the global scanner side.",
};
function renderListLegChips(opts) {
  const box = document.getElementById(opts.containerId);
  if (!box) return;
  const vals = listValues(opts.listKey);
  // Every engine tab (Realtime, Paper Trade, Paper Trade 2, Paper Trade 3)
  // exposes the CE/PE leg call for added indices / commodities: the chosen leg is
  // the contract the engine trades. Paper Trade 2 used to hide it and follow the
  // live global side; it now shows the same dropdown as the other tabs.
  const showLeg = true;
  // The 1s snapshot poll calls `applySettingsToDom` -> `renderAllChips`, which
  // used to re-assign this container's innerHTML on every pass. Replacing the
  // DOM destroys the native CE/PE <select> the moment it is opened, so the
  // dropdown closed instantly and a leg could never be chosen. Rebuild only when
  // the (id + resolved name + leg) set actually changed.
  const sig = vals.map((v) => showLeg
    ? `${v}:${indexName(v)}:${listLegValue(opts.settingsKey, v)}`
    : `${v}:${indexName(v)}`).join("|");
  if (box.getAttribute("data-leg-sig") === sig) return;
  box.setAttribute("data-leg-sig", sig);
  if (!vals.length) {
    box.innerHTML = `<span style="color:#666">None added.</span>`;
    return;
  }
  box.innerHTML = vals.map((v) => {
    const leg = showLeg ? listLegValue(opts.settingsKey, v) : "";
    const border = showLeg ? (leg ? "#2d6b50" : "#6b3a3a") : "#2d2d50";
    const selectHTML = showLeg
      ? `<select ${opts.attr}="${esc(String(v))}" data-leg-title="${esc(opts.title)}" title="${esc(opts.title)}" style="background:#0e1626;color:#00d4aa;border:1px solid #2d2d50;border-radius:4px;font-size:9px;font-weight:700;padding:1px 2px">${legOptionsHTML(leg, opts.noneLabel)}</select>`
      : "";
    return `<span style="display:inline-flex;align-items:center;gap:4px;background:#16163a;border:1px solid ${border};border-radius:8px;padding:1px 4px;margin:1px">${esc(indexName(v))}`
      + selectHTML
      + `<button data-chip-key="${esc(opts.listKey)}" data-chip-id="${esc(String(v))}" title="Remove" style="background:none;border:none;color:#ef5350;cursor:pointer;font-size:11px;padding:0 2px;line-height:1">\u00d7</button></span>`;
  }).join("");
  box.querySelectorAll(`[${opts.attr}]`).forEach((sel) => {
    sel.onchange = (e) => {
      e.stopPropagation();
      setListLeg(opts.settingsKey, sel.getAttribute(opts.attr), sel.value, () => renderListLegChips(opts));
    };
  });
  box.querySelectorAll("[data-chip-id]").forEach((b) => {
    b.onclick = (e) => {
      e.preventDefault();
      const id = b.getAttribute("data-chip-id");
      const inp = document.querySelector(`#tab-realtime [data-list="${opts.listKey}"]`);
      if (!inp) return;
      inp.value = listValues(opts.listKey).filter((x) => x !== id).join(", ");
      // Drop the removed instrument's assigned leg too, then persist both edits.
      const cur = (((STATE && STATE.settings && STATE.settings[opts.settingsKey]) || []).slice())
        .filter((x) => num(x && x.securityId) !== num(id));
      const patch = {};
      patch[opts.settingsKey] = cur;
      STATE.settings = Object.assign({}, (STATE && STATE.settings) || {}, patch);
      API.settings(settingsFromDom()).then(() => renderListLegChips(opts));
    };
  });
}
function renderIndexChips() { renderListLegChips(INDEX_LEG_CHIPS); }
function renderCommodityChips() { renderListLegChips(COMMODITY_LEG_CHIPS); }
// Paper Trade 2: while a per-instrument trend section is ON, the manual CE/PE leg
// dropdown for that category is strictly inactive - the engine picks the leg
// automatically from the instrument's own bullish/bearish trend. Adding/removing
// instruments still works; only the leg choice is locked (and visually faded).
function syncTrendLegLock() {
  if (!PAPER2) return;
  const s = (STATE && STATE.settings) || {};
  const lockBox = (containerId, attr, on, lockMsg) => {
    const box = document.getElementById(containerId);
    if (!box) return;
    box.querySelectorAll("select[" + attr + "]").forEach((sel) => {
      sel.disabled = on;
      sel.style.opacity = on ? "0.4" : "1";
      sel.style.pointerEvents = on ? "none" : "";
      sel.style.cursor = on ? "not-allowed" : "";
      sel.title = on ? lockMsg : (sel.getAttribute("data-leg-title") || "");
    });
  };
  lockBox(
    "rtMoversIndicesList",
    "data-idx-leg",
    !!s.indexTrendOn,
    "Leg locked: Indices Trend Following ON hai - leg har index ke apne bullish/bearish trend se automatic select hoga."
  );
  lockBox(
    "rtCommodityChips",
    "data-cmd-leg",
    !!s.commTrendOn,
    "Leg locked: Commodities Trend Following ON hai - leg har commodity ke apne bullish/bearish trend se automatic select hoga."
  );
}
function renderAllChips() {
  renderChips("moversIndices", "rtMoversIndicesList", "index");
  renderChips("niftyTrendConfInds", "rtNiftyTrendConfIndList", "indicator");
  renderChips("entryPrimaryFilters", "rtEntryPrimaryList", "filterkey");
  if (PAPER2) {
    renderChips("indexTrendConfInds", "rtIndexTrendConfIndList", "indicator");
    renderChips("commTrendConfInds", "rtCommTrendConfIndList", "indicator");
  }
  renderChips("commodityList", "rtCommodityChips", "commodity");
  renderLiquidChips();
  syncLiquidSelects((STATE && STATE.settings) || {});
  wireLiquidControls();
  renderManualStrikes();
  syncTrendLegLock();
}

// ---------------------------------------------------------------------------
// Manual Strike Select (testing only): pick an underlying + strike by hand and
// the engine resolves its option leg to that contract instead of the automatic
// ATM selection. Instrument + strike lists come from the engine-agnostic
// /api/instruments + /api/manual_strikes/chain endpoints; the picked list is a
// normal setting, so the real and paper tabs keep independent lists.
// ---------------------------------------------------------------------------
let MANUAL_INSTRUMENTS = null;
let manualInstrumentsRetry = null;
let manualLoadedOk = false;
let MANUAL_CHAIN = null;
let MANUAL_LOADING = false;

function manualOptKey(sec) {
  return String(sec.security_id) + "|" + String(sec.exchange_segment || "");
}

function manualFindInstrument(key) {
  if (!key || !MANUAL_INSTRUMENTS) return null;
  const d = MANUAL_INSTRUMENTS.data || {};
  const all = [].concat(d.indices || [], d.fno || [], d.commodities || []);
  return all.find((it) => manualOptKey(it) === key) || null;
}

function manualFillInstruments() {
  const sel = document.getElementById("rtManualInstrument");
  if (!sel) return;
  const keep = sel.value;
  const d = (MANUAL_INSTRUMENTS && MANUAL_INSTRUMENTS.data) || {};
  const mk = (arr) => (arr || []).map((it) => `<option value="${esc(manualOptKey(it))}">${esc(it.name)}</option>`).join("");
  let html = '<option value="">-- pick instrument --</option>';
  if ((d.indices || []).length) html += '<optgroup label="Indices">' + mk(d.indices) + "</optgroup>";
  if ((d.fno || []).length) html += '<optgroup label="F&amp;O Stocks">' + mk(d.fno) + "</optgroup>";
  if ((d.commodities || []).length) html += '<optgroup label="Commodities (MCX)">' + mk(d.commodities) + "</optgroup>";
  sel.innerHTML = html;
  if (keep) sel.value = keep;
}

async function manualLoadInstruments(force) {
  const sel = document.getElementById("rtManualInstrument");
  if (!sel) return;
  if (MANUAL_INSTRUMENTS && !force) {
    manualFillInstruments();
    return;
  }
  if (MANUAL_LOADING) return;
  MANUAL_LOADING = true;
  sel.innerHTML = '<option value="">-- loading --</option>';
  try {
    MANUAL_INSTRUMENTS = await get("/api/instruments");
  } catch (e) {
    MANUAL_LOADING = false;
    sel.innerHTML = '<option value="">-- load failed --</option>';
    return;
  }
  MANUAL_LOADING = false;
  manualFillInstruments();
  // Fresh install: the contract master may still be warming, so the commodity
  // section of this picker can be empty. Retry in the background until it fills
  // instead of caching the empty response for the whole session.
  const d = (MANUAL_INSTRUMENTS && MANUAL_INSTRUMENTS.data) || {};
  if ((d.commodities || []).length === 0 && (d.warming === true || (d.indices || []).length === 0)) {
    if (!manualInstrumentsRetry) {
      manualInstrumentsRetry = setTimeout(() => {
        manualInstrumentsRetry = null;
        if (MANUAL_INSTRUMENTS && !manualLoadedOk) manualLoadInstruments(true);
      }, 3000);
    }
  } else if ((d.indices || []).length || (d.commodities || []).length) {
    manualLoadedOk = true;
  }
}

function manualFillStrikes() {
  const sel = document.getElementById("rtManualStrikePick");
  if (!sel) return;
  const rows = (MANUAL_CHAIN && MANUAL_CHAIN.data) || [];
  if (!rows.length) {
    sel.innerHTML = '<option value="">-- no strikes --</option>';
    return;
  }
  sel.innerHTML = rows
    .map((r) => {
      const k = num(r.Strike);
      const ce = num(r["CE LTP"]);
      const pe = num(r["PE LTP"]);
      const label = k + "  CE " + (ce > 0 ? ce.toFixed(2) : "-") + "  PE " + (pe > 0 ? pe.toFixed(2) : "-");
      return `<option value="${k}">${esc(label)}</option>`;
    })
    .join("");
}

async function manualLoadChain() {
  const instKey = (document.getElementById("rtManualInstrument") || {}).value || "";
  const strikeSel = document.getElementById("rtManualStrikePick");
  const expSel = document.getElementById("rtManualExpiry");
  const status = document.getElementById("rtManualStrikeStatus");
  const inst = manualFindInstrument(instKey);
  MANUAL_CHAIN = null;
  if (!inst) {
    if (strikeSel) strikeSel.innerHTML = "";
    if (expSel) expSel.innerHTML = "";
    if (status) status.textContent = "";
    return;
  }
  if (strikeSel) strikeSel.innerHTML = '<option value="">-- loading strikes --</option>';
  if (status) status.textContent = "loading strikes...";
  const wantExpiry = expSel ? expSel.value : "";
  // Best-effort spot so the strike window centres on the live price. Without a
  // session this stays 0 and the backend centres on the middle of the ladder.
  let spot = 0;
  try {
    const q = await post("/api/quotes", {
      securities: [{ security_id: inst.security_id, exchange_segment: inst.exchange_segment }],
    });
    const key = inst.exchange_segment === "IDX_I" ? "IDX_I:" + inst.security_id : String(inst.security_id);
    spot = num(q && q.data && q.data[key] ? q.data[key].ltp : 0);
  } catch (e) {
    spot = 0;
  }
  let j;
  try {
    j = await post("/api/manual_strikes/chain", {
      security_id: inst.security_id,
      exchange_segment: inst.exchange_segment,
      symbol_name: inst.name,
      expiry: wantExpiry,
      spot: spot,
    });
  } catch (e) {
    if (status) status.textContent = "chain load failed";
    return;
  }
  if (!j || j.status === "loading") {
    if (status) status.textContent = "scrip master loading - retrying...";
    setTimeout(manualLoadChain, 1500);
    return;
  }
  MANUAL_CHAIN = j;
  if (expSel) {
    const exps = MANUAL_CHAIN.expiries || [];
    expSel.innerHTML = exps.map((e) => `<option value="${esc(e)}">${esc(e)}</option>`).join("");
    if (MANUAL_CHAIN.expiry) expSel.value = MANUAL_CHAIN.expiry;
  }
  manualFillStrikes();
  if (status) status.textContent = (MANUAL_CHAIN.count || 0) + " strikes (spot " + num(MANUAL_CHAIN.spot).toFixed(2) + ")";
}

function manualStrikeList() {
  return (STATE && STATE.settings && STATE.settings.manualStrikes) || [];
}

function renderManualStrikes() {
  const host = document.getElementById("rtManualStrikeList");
  if (!host) return;
  const rows = manualStrikeList();
  if (!rows.length) {
    host.innerHTML = '<span style="color:#666">No manual strikes added yet.</span>';
    return;
  }
  host.innerHTML = rows
    .map((m, i) => {
      const col = String(m.optionType).toUpperCase() === "CE" ? "#00d4aa" : "#ef5350";
      return (
        `<span style="display:inline-flex;align-items:center;gap:4px;background:#12122a;border:1px solid #2d2d50;border-radius:4px;padding:2px 6px">` +
        `<b style="color:${col}">${esc(m.symbolName)} ${esc(m.expiry)} ${num(m.strike)} ${esc(m.optionType)}</b>` +
        `<button data-ms-remove="${i}" title="Remove" style="background:none;border:none;color:#ef5350;cursor:pointer;font-size:12px;padding:0 2px;line-height:1">\u00d7</button>` +
        `</span>`
      );
    })
    .join("");
  host.querySelectorAll("[data-ms-remove]").forEach((b) => {
    b.onclick = (e) => {
      e.preventDefault();
      const arr = manualStrikeList().slice();
      arr.splice(num(b.getAttribute("data-ms-remove")), 1);
      API.settings({ manualStrikes: arr }).then(refresh);
    };
  });
}

function addManualStrike() {
  const status = document.getElementById("rtManualStrikeStatus");
  const inst = manualFindInstrument((document.getElementById("rtManualInstrument") || {}).value || "");
  if (!inst || !MANUAL_CHAIN) {
    if (status) status.textContent = "pick an instrument first";
    return;
  }
  const strike = num((document.getElementById("rtManualStrikePick") || {}).value);
  const side = (document.getElementById("rtManualSide") || {}).value || "both";
  const row = (MANUAL_CHAIN.data || []).find((r) => Math.abs(num(r.Strike) - strike) < 0.001);
  if (!row) {
    if (status) status.textContent = "pick a strike first";
    return;
  }
  const list = manualStrikeList().slice();
  const sides = side === "both" ? ["CE", "PE"] : [side];
  let added = 0;
  for (const s of sides) {
    if (num(row[s + " SID"]) <= 0) continue;
    const dup = list.some(
      (m) =>
        m.symbolName === inst.name &&
        m.expiry === MANUAL_CHAIN.expiry &&
        Math.abs(num(m.strike) - strike) < 0.001 &&
        String(m.optionType).toUpperCase() === s
    );
    if (dup) continue;
    list.push({
      symbolName: inst.name,
      securityId: inst.security_id,
      exchangeSegment: inst.exchange_segment,
      expiry: MANUAL_CHAIN.expiry,
      strike: strike,
      optionType: s,
      lot: num(MANUAL_CHAIN.lot_size),
    });
    added++;
  }
  if (!added) {
    if (status) status.textContent = "already added";
    return;
  }
  if (list.length > 20) {
    if (status) status.textContent = "max 20 strikes";
    return;
  }
  if (status) status.textContent = "added " + added;
  API.settings({ manualStrikes: list }).then(refresh);
}

// "Picked Strikes": the CE/PE option contracts the engine resolved for the live
// Top Movers / NIFTY trend (and MCX) picks. Fed by the snapshot `strikes` array.
// Paper Trade 2 has no stocks, so the backend's "Top Movers" source is really
// its Indices feed - relabel it there.
function sourceLabel(src) {
  const t = String(src || "");
  return PAPER2 && t === "Top Movers" ? "Indices" : t;
}function renderStrikes() {
  const host = document.getElementById("rtStrikesList");
  if (!host) return;
  const cfg = (STATE && STATE.settings) || {};
  const rows = (STATE && STATE.strikes) || [];
  const manualOn = !!cfg.manualStrikesEnabled;
  const note = document.getElementById("rtStrikesNote");
  if (note) {
    note.textContent = manualOn
      ? "Manual Strike Select ON - only these manual contracts run/trade; Top Movers & NIFTY Trend are inactive"
      : INDICES_ONLY
      ? "Option strikes the engine picked to execute for the Indices section (auto-shown while it is ON)"
      : COMMODITIES_ONLY
      ? "Option strikes the engine picked to execute for the Commodities section (auto-shown while it is ON)"
      : PAPER2
      ? "Option strikes the engine picked to execute for the Indices & Commodities section (auto-shown while it is ON)"
      : "Option strikes the engine picked to execute (auto-shown while Top Movers / NIFTY Trend Follow is ON)";
  }
  if (!manualOn && !cfg.niftyTrendOn && !cfg.moversOn && !cfg.commodityOn && !cfg.liquidOn) {
    host.style.display = "none";
    return;
  }
  host.style.display = "block";
  if (!rows.length) {
    host.innerHTML = manualOn
      ? `<span style="color:#666">Manual Strike Select ON - add strikes in the Manual Strike list above; only these will run/trade.</span>`
      : INDICES_ONLY
      ? `<span style="color:#666">No option strikes resolved yet - waiting for live Dhan quotes on the Indices picks.</span>`
      : COMMODITIES_ONLY
      ? `<span style="color:#666">No option strikes resolved yet - waiting for live Dhan quotes on the Commodities picks.</span>`
      : PAPER2
      ? `<span style="color:#666">No option strikes resolved yet - waiting for live Dhan quotes on the Indices &amp; Commodities picks.</span>`
      : `<span style="color:#666">No option strikes resolved yet - waiting for live Dhan quotes on the Top Movers / NIFTY Trend picks.</span>`;
    renderScannerRemoved();
    return;
  }
  host.innerHTML = rows
    .map((r) => {
      const bull = String(r.side || "").toUpperCase() === "CE";
      const col = bull ? "#00d4aa" : "#ef5350";
      const uid = num(r.underlyingSecurityId);
      const isManual = manualOn || String(r.source || "") === "Manual";
      const rm = !isManual && uid > 0
        ? `<button data-sc-remove="${uid}" data-sc-name="${esc(r.underlying)}" title="Remove - engine will not pick/trade this" style="background:none;border:none;color:#ef5350;cursor:pointer;font-size:12px;padding:0 3px;line-height:1">\u00d7</button>`
        : "";
      const spot = num(r.spot) > 0
        ? `<span style="color:#888">spot ${num(r.spot).toFixed(2)} &middot; ${num(r.changePct).toFixed(2)}%</span>`
        : "";
      const oLtp = num(r.optionLtp);
      const oChg = num(r.optionChange);
      const opt = oLtp > 0
        ? `<span style="color:${oChg >= 0 ? "#00d4aa" : "#ef5350"};font-weight:600">premium ${oLtp.toFixed(2)} (${oChg >= 0 ? "+" : ""}${oChg.toFixed(2)} pts)</span>`
        : "";
      return (
        `<div style="display:flex;gap:8px;align-items:center;padding:1px 0">` +
        `<span style="color:${col};font-weight:700;min-width:70px">${esc(r.underlying)} ${esc(r.side)}</span>` +
        `<span style="color:#fff">${esc(r.tradingSymbol)}</span>` +
        spot +
        opt +
        `<span style="color:#555;margin-left:auto">${esc(sourceLabel(r.source))}</span>` +
        rm +
        `</div>`
      );
    })
    .join("");
  host.querySelectorAll("[data-sc-remove]").forEach((b) => {
    b.onclick = (e) => {
      e.preventDefault();
      removeScannerPick(num(b.getAttribute("data-sc-remove")), b.getAttribute("data-sc-name"));
    };
  });
  renderScannerRemoved();
}

// ---- Scanner "remove" picks (operator blacklist) ---------------------------
// The old app's removeMoverIndex / removeNiftyTrendIndex, but server-enforced:
// the excluded underlying is dropped from scan_universe, so it can never be
// ranked, resolved to an option leg or traded until restored. Persisted in
// settings.scannerExclude so it survives restarts and both tabs stay in sync.
function scannerExcludedIds() {
  const cfg = (STATE && STATE.settings) || {};
  return (cfg.scannerExclude || []).map(num).filter((x) => x > 0);
}
function renderScannerRemoved() {
  const row = document.getElementById("rtScannerRemovedRow");
  const box = document.getElementById("rtScannerRemovedList");
  const inp = document.getElementById("rtScannerExcludeList");
  if (!row || !box) return;
  const ids = scannerExcludedIds();
  if (inp) inp.value = ids.join(", ");
  if (!ids.length) {
    row.style.display = "none";
    box.innerHTML = "";
    return;
  }
  row.style.display = "block";
  box.innerHTML = ids
    .map(
      (id) =>
        `<span style="display:inline-flex;align-items:center;gap:4px;background:#2a1414;border:1px solid #5a2222;border-radius:8px;padding:1px 4px;margin:1px">${esc(indexName(id))}` +
        `<button data-sc-restore="${id}" title="Restore - engine will pick it again" style="background:none;border:none;color:#00d4aa;cursor:pointer;font-size:11px;padding:0 2px;line-height:1">\u21ba</button></span>`
    )
    .join("");
  box.querySelectorAll("[data-sc-restore]").forEach((b) => {
    b.onclick = (e) => {
      e.preventDefault();
      restoreScannerPick(num(b.getAttribute("data-sc-restore")));
    };
  });
}
function setScannerExclude(ids) {
  const uniq = [];
  (ids || []).map(num).forEach((x) => {
    if (x > 0 && uniq.indexOf(x) < 0) uniq.push(x);
  });
  if (STATE.settings) STATE.settings.scannerExclude = uniq;
  const inp = document.getElementById("rtScannerExcludeList");
  if (inp) inp.value = uniq.join(", ");
  renderScannerRemoved();
  return API.settings({ scannerExclude: uniq });
}
function removeScannerPick(id, name) {
  id = num(id);
  if (!(id > 0)) return;
  const ids = scannerExcludedIds();
  if (ids.indexOf(id) >= 0) return;
  ids.push(id);
  setScannerExclude(ids);
  if (typeof linkToast === "function") linkToast("ok", `${name || indexName(id)} scanner picks se hata diya`);
}
function restoreScannerPick(id) {
  setScannerExclude(scannerExcludedIds().filter((x) => x !== num(id)));
}
function populateAstSelects() {
  if (astSelectsPopulated) return;
  const fill = (selId, rows, labelFn) => {
    const sel = document.getElementById(selId);
    if (!sel) return;
    const cur = sel.value;
    const groups = new Map();
    rows.forEach((r) => {
      const g = r.g || "";
      if (!groups.has(g)) groups.set(g, []);
      groups.get(g).push(r);
    });
    const opts = Array.from(groups.entries())
      .map(([g, rs]) => {
        const body = rs.map((r) => `<option value="${esc(r.v)}">${esc(labelFn(r))}</option>`).join("");
        return g ? `<optgroup label="${esc(g)}">${body}</optgroup>` : body;
      })
      .join("");
    sel.innerHTML = `<option value="">-- pick --</option>` + opts;
    if (cur) sel.value = cur;
  };
  const indices = CATALOG.symbols
    .filter((s) => String(s.inst || "").toUpperCase() === "INDEX" || String(s.exch || "").includes("IDX_I"))
    .map((s) => ({ v: s.id, s }));
  const inds = (CATALOG.indicators || []).map((i) => ({ v: i.id, i }));
  if (!indices.length && !inds.length) return;
  fill("rtMoversIndicesSelect", indices, (r) => `${r.s.name} (${r.v})`);
  // Every tab confirms its trend from the straight-line family PLUS the whole
  // indicator-filter catalogue (read as the filter's held side, so a bullish /
  // bearish crossover flips the trend). Paper Trade 3 additionally offers the
  // BB%b variants (BB%b, BB%b Straightline Pane, Bollinger Bands %B - BB%b
  // Straight Line is already a straight-line).
  const straightInds = inds
    .filter((r) => STRAIGHT_LINE_IDS.includes(String(r.v)))
    .map((r) => Object.assign({}, r, { g: "Chart / straight line" }))
    .concat(EXTRA_CONF_INDICATORS.map(([v, name]) => ({ v, i: { id: v, name }, g: "Chart / straight line" })));
  if (PAPER3) {
    const seen = new Set(straightInds.map((r) => String(r.v)));
    BB_PCT_CONF_INDICATORS.forEach(([v, name]) => {
      if (!seen.has(v)) { straightInds.push({ v, i: { id: v, name }, g: "BB%b" }); seen.add(v); }
    });
  }
  const trendInds = straightInds.concat(TREND_FILTER_INDICATORS);
  // PRIMARY "Entry indicator filter" picker: every secondary filter row is
  // offered, grouped by side, because a primary condition is just one of the
  // same filters promoted to the front of the gate.
  fill(
    "rtEntryPrimarySelect",
    FILTER_BULL.map((r) => ({ v: r[0], i: { name: r[1], id: r[0] }, g: "Bullish (CE)" }))
      .concat(FILTER_BEAR.map((r) => ({ v: r[0], i: { name: r[1], id: r[0] }, g: "Bearish (PE)" }))),
    (r) => r.i.name
  );
  fill("rtNiftyTrendConfIndSelect", trendInds, (r) => `${r.i.name || r.i.id}${r.i.cat ? " · " + r.i.cat : ""}`);
  fill("rtIndexTrendConfIndSelect", trendInds, (r) => `${r.i.name || r.i.id}${r.i.cat ? " · " + r.i.cat : ""}`);
  fill("rtCommTrendConfIndSelect", trendInds, (r) => `${r.i.name || r.i.id}${r.i.cat ? " · " + r.i.cat : ""}`);
  astSelectsPopulated = true;
  renderAllChips();
}
function wirePicker(selectId, addId, key, listId, kind) {
  const sel = document.getElementById(selectId);
  const add = document.getElementById(addId);
  if (!add) return;
  add.onclick = () => {
    const v = sel && sel.value;
    if (!v) return;
    const inp = document.querySelector(`#tab-realtime [data-list="${key}"]`);
    if (!inp) return;
    const vals = listValues(key);
    if (vals.indexOf(v) < 0) vals.push(v);
    inp.value = vals.join(", ");
    API.settings(settingsFromDom()).then(() => renderChips(key, listId, kind));
  };
}
function wireAstPickers() {
  wirePicker("rtMoversIndicesSelect", "rtMoversIndicesAdd", "moversIndices", "rtMoversIndicesList", "index");
  wirePicker("rtNiftyTrendConfIndSelect", "rtNiftyTrendConfIndAdd", "niftyTrendConfInds", "rtNiftyTrendConfIndList", "indicator");
  wirePicker("rtEntryPrimarySelect", "rtEntryPrimaryAdd", "entryPrimaryFilters", "rtEntryPrimaryList", "filterkey");
  if (PAPER2) {
    wirePicker("rtIndexTrendConfIndSelect", "rtIndexTrendConfIndAdd", "indexTrendConfInds", "rtIndexTrendConfIndList", "indicator");
    wirePicker("rtCommTrendConfIndSelect", "rtCommTrendConfIndAdd", "commTrendConfInds", "rtCommTrendConfIndList", "indicator");
  }
  wireAssign("rtTplMoverBull", "rtAssignMoverBull", "moverBullTemplate", "bullish");
  wireAssign("rtTplMoverBear", "rtAssignMoverBear", "moverBearTemplate", "bearish");
  renderAllChips();
}

// ---------------------------------------------------------------------------
// Interlock / fade logic (ported from the old app's aismart.js sync*UI helpers)
//
// Selecting one option fades out (disables + dims) the mutually-exclusive one:
//   - AI Stop-Loss  <->  Manual Overall SL / Trail SL
//   - Risk:Reward   ->   all TP controls
//   - Pick modes, strike count, premium lock, NIFTY-Trend/Top-Movers...
// A faded control is .disabled + pointerEvents:none, exactly like the old app.
// ---------------------------------------------------------------------------
function fadeControl(el, on) {
  if (!el) return;
  el.disabled = !!on;
  el.style.opacity = on ? "0.35" : "1";
  el.style.pointerEvents = on ? "none" : "";
  el.style.cursor = on ? "not-allowed" : "";
  const lbl = el.closest ? el.closest("label") : null;
  // Dim the label for the visual cue only when it is dedicated to this control.
  // When the label also holds a sibling control (e.g. an enable checkbox plus a
  // value input like "Set TP by Risk:Reward [x] <value>" or the Trade-times
  // rows), dimming the label would grey out - and pointer-lock - the sibling
  // toggle, so that whole section looked faded and could not be switched back
  // on. The control itself is already disabled + dimmed above.
  if (lbl && lbl.querySelectorAll("input, select, button").length <= 1) {
    lbl.style.opacity = on ? "0.35" : "1";
  } else if (lbl) {
    lbl.style.opacity = "1";
  }
}
function dimControl(el, on) {
  if (!el) return;
  el.disabled = !!on;
  el.style.opacity = on ? "0.5" : "1";
}

function renderRiskStatus() {
  const q = (k) => document.querySelector('#tab-realtime [data-set="' + k + '"]');
  const rrEl = q("rrEnabled");
  const rrOn = !!(rrEl && rrEl.checked);
  const rrValEl = q("rrValue");
  const rr = Number(rrValEl && rrValEl.value) > 0 ? Number(rrValEl.value) : 2;
  const rrBox = document.getElementById("rtRrStatus");
  if (rrBox) {
    const slChk = document.getElementById("rtEngSl");
    const slPctEl = document.getElementById("rtEngSlPct");
    const slPct = slChk && slChk.checked ? Number(slPctEl && slPctEl.value) || 0 : 0;
    if (!rrOn) {
      rrBox.style.color = "#666";
      rrBox.textContent = "RR off - TP from AI TP%";
    } else if (slPct > 0) {
      rrBox.style.color = "#00d4aa";
      rrBox.textContent = "RR " + rr + "x SL " + slPct + "% => TP target " + (rr * slPct).toFixed(2) + "% (overrides TP + AI TP%)";
    } else {
      rrBox.style.color = "#ffb74d";
      rrBox.textContent = "RR " + rr + "x SL distance (AI/ATR stop) - target set live per trade";
    }
  }
}

function renderTradesStatus() {
  const q = (k) => document.querySelector('#tab-realtime [data-set="' + k + '"]');
  const box = document.getElementById("rtTradesStatus");
  if (!box) return;
  const tlEl = q("tradeLimit");
  const aiEl = q("aiTrades");
  const cntEl = q("tradeLimitCount");
  const tlOn = !!(tlEl && tlEl.checked);
  const aiOn = !!(aiEl && aiEl.checked);
  const count = Number(cntEl && cntEl.value) || 0;
  if (aiOn) {
    box.style.color = "#7c9cff";
    box.textContent = "AI auto trades ON - per-strategy cap ignored (unlimited entries)";
  } else if (tlOn && count > 0) {
    box.style.color = "#00d4aa";
    box.textContent = "Max " + count + " trades per strategy (counted across open + closed entries)";
  } else if (tlOn) {
    box.style.color = "#ffb74d";
    box.textContent = "Max trades ON but count is 0 - no cap applied, set a positive number";
  } else {
    box.style.color = "#666";
    box.textContent = "No per-strategy trade limit";
  }
}

function renderTimeStatus() {
  const q = (k) => document.querySelector('#tab-realtime [data-set="' + k + '"]');
  const box = document.getElementById("rtTimeStatus");
  if (!box) return;
  const on = (k) => !!(q(k) && q(k).checked);
  const val = (k) => { const e = q(k); return e && e.value ? e.value : ""; };
  const parts = [];
  if (on("startAfterEnabled")) parts.push("entries open at " + val("startAfter"));
  if (on("noTradeAfterEnabled")) parts.push("no new entries after " + val("noTradeAfter"));
  if (on("autoSquareOffEnabled")) parts.push("auto square-off all at " + val("autoSquareOffTime"));
  const ses = tradeSessions().filter((x) => x.enabled && tsParseTime(x.start) != null && tsParseTime(x.end) != null && tsParseTime(x.start) < tsParseTime(x.end));
  if (ses.length) parts.push("sessions " + ses.map((x) => tsFmt12(tsParseTime(x.start)) + "-" + tsFmt12(tsParseTime(x.end))).join(", "));
  box.style.color = parts.length ? "#ffb74d" : "#666";
  box.textContent = parts.length ? " | " + parts.join("  ·  ") : " | No trade-time gate - entries run all session";
  // F&O-stock expiry-day guard readout (Realtime + Paper Trade 1 only).
  const fbox = document.getElementById("rtFnoExpiryStatus");
  if (fbox) {
    const onExp = on("fnoExpirySquareOffEnabled");
    const noDay = on("fnoExpiryNoTradeDayEnabled");
    const t = val("fnoExpirySquareOffTime") || "14:00";
    const info = (STATE && STATE.fnoExpiries) || {};
    const up = Array.isArray(info.upcoming) ? info.upcoming : [];
    const near = up[0] || "";
    const nearTxt = near
      ? (info.isExpiryDay ? "today " : "next ") + "F&O stock expiry: " + near
      : "F&O stock expiry: --";
    const more = up.length > 1 ? "  (" + up.slice(1).join(", ") + ")" : "";
    const armed = info.isExpiryDay && (onExp || noDay);
    let guard;
    if (noDay) guard = "no F&O-stock entry all day";
    else if (onExp) guard = "square-off + no stock entry after " + t;
    else guard = "expiry guard off";
    fbox.style.color = armed ? "#ef5350" : onExp || noDay ? "#ffb74d" : "#666";
    fbox.textContent =
      "| " + guard +
      (armed ? "  [ACTIVE today]" : "") +
      "  ·  " + nearTxt + more;
  }
}

// ---- Multi-session trade windows (Trade times -> Trading sessions) ----------
// The operator can add several intraday windows (e.g. 09:30-10:30 and
// 14:00-15:14). The server gate (`sessions_gate_ok`) is authoritative: entries
// only fire while the IST minute is inside an enabled, well-formed window.
// This row is a pure editor that persists the list in settings.tradeSessions.
function tsISTMinutes() {
  const now = new Date();
  const ist = new Date(now.getTime() + (330 + now.getTimezoneOffset()) * 60000);
  return ist.getHours() * 60 + ist.getMinutes();
}
// Accepts both 24h ("09:30", "14:00") and 12h with AM/PM ("9:30 AM", "02:00 PM",
// "2 PM"). Returns minutes past midnight, or null. Storage always stays 24h
// "HH:MM" (the server parses that); AM/PM is purely the display layer.
function tsParseTime(s) {
  const t = String(s || "").trim().toUpperCase().replace(/\s+/g, " ");
  const m = /^(\d{1,2}):?(\d{2})?\s*(AM|PM)?$/.exec(t);
  if (!m) return null;
  let h = +m[1];
  const mi = m[2] == null ? 0 : +m[2];
  const ap = m[3];
  if (mi > 59) return null;
  if (ap) {
    if (h < 1 || h > 12) return null;
    if (h === 12) h = 0;
    if (ap === "PM") h += 12;
  } else if (h > 23) {
    return null;
  }
  return h * 60 + mi;
}
// Minutes -> "09:30 AM" / "02:00 PM" / "12:00 PM".
function tsFmt12(mins) {
  if (mins == null || isNaN(mins)) return "";
  mins = ((Math.round(mins) % 1440) + 1440) % 1440;
  const h24 = (mins / 60) | 0;
  const mi = mins % 60;
  const ap = h24 >= 12 ? "PM" : "AM";
  let h = h24 % 12;
  if (h === 0) h = 12;
  return String(h).padStart(2, "0") + ":" + String(mi).padStart(2, "0") + " " + ap;
}
// Minutes -> stored 24h "HH:MM".
function tsFmtHHMM(mins) {
  mins = ((Math.round(mins) % 1440) + 1440) % 1440;
  return String((mins / 60) | 0).padStart(2, "0") + ":" + String(mins % 60).padStart(2, "0");
}
function tradeSessions() {
  const s = (STATE && STATE.settings) || {};
  if (!Array.isArray(s.tradeSessions)) return [];
  return s.tradeSessions.map((x) => ({
    start: (x && x.start) || "",
    end: (x && x.end) || "",
    enabled: !x || x.enabled !== false,
  }));
}
function persistTradeSessions(list) {
  if (STATE.settings) STATE.settings.tradeSessions = list;
  renderTradeSessions(true);
  renderTimeStatus();
  return API.settings({ tradeSessions: list });
}
function renderTradeSessions(force) {
  const host = document.getElementById("rtTradeSessionsList");
  if (!host) return;
  // Never yank the DOM out from under an in-progress edit (the 1s snapshot
  // refresh calls this); a focused time input / checkbox would lose focus.
  // Our own edits pass force=true so add/remove still repaint immediately.
  if (!force && host.contains(document.activeElement)) return;
  const list = tradeSessions();
  const nowM = tsISTMinutes();
  const validOf = (s) => {
    const a = tsParseTime(s.start), b = tsParseTime(s.end);
    return s.enabled && a != null && b != null && a < b;
  };
  host.innerHTML = list.length
    ? list
        .map((s, i) => {
          const a = tsParseTime(s.start), b = tsParseTime(s.end);
          const bad = a == null || b == null || a >= b;
          const live = validOf(s) && nowM >= a && nowM <= b;
          return (
            `<span style="display:inline-flex;align-items:center;gap:4px;background:#16163a;border:1px solid ${live ? "#00d4aa" : bad ? "#5a2222" : "#2d2d50"};border-radius:6px;padding:2px 6px">` +
            `<input type="text" data-ts-start="${i}" value="${esc(tsFmt12(a))}" list="rtSessTimes" autocomplete="off" style="width:86px;font-size:10px;text-align:center">` +
            `<span style="color:#888;font-size:9px">to</span>` +
            `<input type="text" data-ts-end="${i}" value="${esc(tsFmt12(b))}" list="rtSessTimes" autocomplete="off" style="width:86px;font-size:10px;text-align:center">` +
            `<label style="font-size:9px;color:#aaa"><input type="checkbox" data-ts-on="${i}" ${s.enabled ? "checked" : ""}> on</label>` +
            `<button type="button" data-ts-del="${i}" title="Remove session" style="background:none;border:none;color:#ef5350;cursor:pointer;font-size:12px;padding:0 2px;line-height:1">\u00d7</button>` +
            (live ? `<span style="color:#00d4aa;font-size:8px;font-weight:700">LIVE</span>` : bad && s.enabled ? `<span style="color:#ef5350;font-size:8px">invalid</span>` : "") +
            `</span>`
          );
        })
        .join("")
    : `<span style="color:#666">No session added - entries run any time (subject to Start/No-trade gate).</span>`;
  const box = document.getElementById("rtSessionStatus");
  if (box) {
    const valid = list.filter(validOf);
    box.style.color = valid.length ? "#ffb74d" : "#666";
    box.textContent = valid.length
      ? ` | entries only inside ${valid.map((s) => tsFmt12(tsParseTime(s.start)) + " to " + tsFmt12(tsParseTime(s.end))).join(", ")}`
      : "";
  }
  const applyEdit = (i, field, raw) => {
    const l = tradeSessions();
    if (!l[i]) return;
    const m = tsParseTime(raw);
    if (m == null) {
      uiAlert("Valid time dein (e.g. 09:30 AM ya 14:00).");
      renderTradeSessions(true);
      return;
    }
    l[i][field] = tsFmtHHMM(m);
    persistTradeSessions(l);
  };
  host.querySelectorAll("[data-ts-start]").forEach((el) => {
    el.onchange = () => applyEdit(+el.getAttribute("data-ts-start"), "start", el.value);
    el.onkeydown = (e) => { if (e.key === "Enter") el.blur(); };
  });
  host.querySelectorAll("[data-ts-end]").forEach((el) => {
    el.onchange = () => applyEdit(+el.getAttribute("data-ts-end"), "end", el.value);
    el.onkeydown = (e) => { if (e.key === "Enter") el.blur(); };
  });
  host.querySelectorAll("[data-ts-on]").forEach((el) => {
    el.onchange = () => {
      const l = tradeSessions();
      const i = +el.getAttribute("data-ts-on");
      if (!l[i]) return;
      l[i].enabled = el.checked;
      persistTradeSessions(l);
    };
  });
  host.querySelectorAll("[data-ts-del]").forEach((el) => {
    el.onclick = (e) => {
      e.preventDefault();
      const l = tradeSessions();
      l.splice(+el.getAttribute("data-ts-del"), 1);
      persistTradeSessions(l);
    };
  });
  syncTradeSessionInterlock();
}
// When at least one enabled, well-formed session exists it becomes the single
// authority for entry timing, so the old single "Start trading after / No trade
// after" controls are disabled (greyed, non-interactive) to avoid a confusing
// double gate. Removing every session re-enables them exactly as before.
function syncTradeSessionInterlock() {
  const active = tradeSessions().some((s) => {
    const a = tsParseTime(s.start), b = tsParseTime(s.end);
    return s.enabled && a != null && b != null && a < b;
  });
  const row = document.querySelector('#tab-realtime [data-set="startAfterEnabled"]');
  const holder = row && row.closest(".rt-engine-row");
  ["startAfterEnabled", "startAfter", "noTradeAfterEnabled", "noTradeAfter"].forEach((k) => {
    const el = document.querySelector('#tab-realtime [data-set="' + k + '"]');
    if (!el) return;
    el.disabled = active;
    el.style.pointerEvents = active ? "none" : "";
    if (active) el.style.opacity = "0.3";
  });
  // Not active: hand opacity back to the engine's own interlock so an unticked
  // Start/No-trade box fades exactly as before.
  if (!active && typeof syncInterlocks === "function") syncInterlocks();
  if (holder) holder.title = active
    ? "Trading sessions active hain - ye single Start/No-trade gate disable hai. Sab sessions hatane par wapas on ho jayega."
    : "";
}
function addTradeSession() {
  const a = document.getElementById("rtSessionStart");
  const b = document.getElementById("rtSessionEnd");
  const rawA = (a && a.value) || "";
  const rawB = (b && b.value) || "";
  const sa = tsParseTime(rawA), sb = tsParseTime(rawB);
  if (sa == null || sb == null) {
    uiAlert("Session start aur end time valid dein (e.g. 09:30 AM / 02:00 PM).");
    return;
  }
  if (sa >= sb) {
    uiAlert("Invalid session: start < end hona chahiye (e.g. 09:30 AM to 10:30 AM).");
    return;
  }
  const start = tsFmtHHMM(sa), end = tsFmtHHMM(sb);
  const l = tradeSessions();
  if (l.some((s) => tsParseTime(s.start) === sa && tsParseTime(s.end) === sb)) {
    uiAlert("Ye session already added hai.");
    return;
  }
  l.push({ start, end, enabled: true });
  l.sort((x, y) => (tsParseTime(x.start) || 0) - (tsParseTime(y.start) || 0));
  persistTradeSessions(l);
  if (a) a.value = tsFmt12(sb);
  if (b) b.value = tsFmt12(sb + 30);
}
function wireTradeSessions() {
  const btn = document.getElementById("rtSessionAdd");
  if (btn) btn.onclick = addTradeSession;
  const a = document.getElementById("rtSessionStart");
  const b = document.getElementById("rtSessionEnd");
  const bump = () => {
    const t = tsParseTime(a && a.value);
    if (t != null && b) b.value = tsFmt12(t + 60);
  };
  if (a) a.onchange = bump;
  if (b) b.onkeydown = (e) => { if (e.key === "Enter") addTradeSession(); };
  renderTradeSessions();
}

function renderNiftyStatus() {
  const box = document.getElementById("rtNiftyStatus");
  if (!box) return;
  const nt = (STATE && STATE.niftyTrend) || {};
  const s = (STATE && STATE.settings) || {};
  const on = !!(s.niftyTrendOn || nt.on);
  const tf = nt.tf || s.niftyTf || "5min";
  const tfLabel = tf === "both" ? "1min+5min" : tf.replace("min", " min");
  if (!on) {
    box.style.color = "#666";
    box.textContent = "Trend follow OFF";
    return;
  }
  const dir = nt.dir === 1 ? "BULLISH" : nt.dir === -1 ? "BEARISH" : "neutral";
  const sideway = !!nt.sideways;
  const sidewaysOn = !!s.noTradeSideways;
  box.style.color = sideway ? "#ff8a65" : nt.dir === 1 ? "#00d4aa" : nt.dir === -1 ? "#ef5350" : "#ffd700";
  box.textContent = "Ensemble " + tfLabel + " -> " + (sideway ? "SIDEWAYS" : dir);
  if (sideway) {
    const rd = nt.sidewaysRead || {};
    const bits = [];
    if (rd.adx != null) bits.push("ADX " + rd.adx);
    if (rd.chop != null) bits.push("CHOP " + rd.chop);
    if (rd.slopeAtr != null) bits.push("slope " + rd.slopeAtr + " ATR");
    if (rd.rangeAtr != null) bits.push("range " + rd.rangeAtr + " ATR");
    if (bits.length) box.textContent += " (" + bits.join(", ") + ")";
    box.textContent += sidewaysOn ? " - no new trade" : " - switch OFF, entries allowed";
  }
  // Surface the last committed flip so a trend change is visible even between
  // snapshot ticks, not only as a momentary colour.
  const fl = (STATE && STATE.niftyFlip) || {};
  const flLabel = (v) => (v === 1 ? "BULL" : v === -1 ? "BEAR" : "NEUTRAL");
  if (fl && fl.to != null && fl.at) {
    const secs = Math.max(0, Math.round((Date.now() - fl.at) / 1000));
    const ago = secs < 60 ? secs + "s" : Math.round(secs / 60) + "m";
    box.textContent += "  - last flip " + flLabel(fl.from) + "->" + flLabel(fl.to) + " " + ago + " ago";
  }
  // Per-indicator breakdown of the last read, so a "flip not detected" report
  // can be traced to the exact indicator that is flat / lagging.
  let votes = document.getElementById("rtNiftyVotes");
  const rows = Array.isArray(nt.votes) ? nt.votes : [];
  if (!rows.length) {
    if (votes) votes.remove();
    return;
  }
  if (!votes) {
    votes = document.createElement("div");
    votes.id = "rtNiftyVotes";
    votes.style.cssText = "color:#888;font-size:10px;margin-top:2px";
    box.parentNode.insertBefore(votes, box.nextSibling);
  }
  const vLabel = (v) => (v === 1 ? "BULL" : v === -1 ? "BEAR" : "FLAT");
  votes.textContent = "Votes: " + rows
    .map((r) => indicatorName(r.id) + " " + vLabel(Number(r.side)))
    .join("  ·  ");
}

// Paper Trade 2 / Realtime Trading Engine 2: readout for EVERY added index and
// commodity. Shows the trend the applied confirmation indicators resolved, the
// pinned leg, and whether that leg flipped (CE<->PE) - including the last flip
// time, so a change stays visible after the momentary flip tick.
function renderInstTrendStatus() {
  if (!PAPER2) return;
  const s = (STATE && STATE.settings) || {};
  const agoText = (ms) => {
    const secs = Math.max(0, Math.round((Date.now() - ms) / 1000));
    return secs < 60 ? secs + "s" : Math.round(secs / 60) + "m";
  };
  const paint = (boxId, on, rows, inds, waiting) => {
    const box = document.getElementById(boxId);
    if (!box) return;
    if (!on) {
      box.style.display = "none";
      return;
    }
    box.style.display = "block";
    const list = Array.isArray(rows) ? rows : [];
    const indLabel = (inds || []).map((id) => indicatorName(id)).join(", ");
    let html = '<div style="color:#888;margin-bottom:3px">Trend indicators: ' + esc(indLabel || "none") + "</div>";
    if (!list.length) {
      html += '<div style="color:#666">' + esc(waiting) + "</div>";
    }
    for (const r of list) {
      const trend = String(r.trend || "NEUTRAL").toUpperCase();
      const col = trend === "BULLISH" ? "#00d4aa" : trend === "BEARISH" ? "#ef5350" : trend === "SIDEWAYS" ? "#ff8a65" : "#ffd700";
      const leg = r.leg || "";
      const lastLeg = r.lastLeg || "";
      const legText = leg || (lastLeg ? "- (last " + lastLeg + ")" : "-");
      let flip;
      let flipCol = "#888";
      if (r.flipped) {
        const when = r.flippedAt ? " " + agoText(r.flippedAt) + " ago" : "";
        flip = "LEG FLIPPED " + (r.flipFrom || "?") + " -> " + leg + when;
        flipCol = "#ffd700";
      } else if (r.flippedAt && r.flipFrom) {
        const nowLeg = leg || lastLeg || "?";
        flip = "last flip " + r.flipFrom + " -> " + nowLeg + " " + agoText(r.flippedAt) + " ago";
        flipCol = "#ffb74d";
      } else if (r.newLeg) {
        flip = "new leg " + leg;
        flipCol = "#00d4aa";
      } else if (!leg) {
        flip = "waiting";
      } else {
        flip = "no flip";
      }
      const vLabel = (v) => (v === 1 ? "BULL" : v === -1 ? "BEAR" : "FLAT");
      const voteText = (Array.isArray(r.votes) ? r.votes : [])
        .map((v) => indicatorName(v.id) + " " + vLabel(Number(v.side)))
        .join(", ");
      html +=
        '<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;padding:1px 0">' +
        '<b style="color:#ddd;min-width:90px">' + esc(r.name || ("#" + r.securityId)) + "</b>" +
        '<span style="color:' + col + ';font-weight:bold">' + esc(trend) + "</span>" +
        '<span>Leg: <b style="color:' + (leg ? col : "#888") + '">' + esc(legText) + "</b></span>" +
        '<span style="color:' + flipCol + '">' + esc(flip) + "</span>" +
        (voteText ? '<span style="color:#888;font-size:10px">(' + esc(voteText) + ")</span>" : "") +
        "</div>";
    }
    box.innerHTML = html;
  };
  paint(
    "rtIndexTrendStatus",
    !!s.indexTrendOn,
    (STATE && STATE.indexTrendStatus) || [],
    s.indexTrendConfInds,
    "Index add karo / trend read ka wait karo."
  );
  paint(
    "rtCommTrendStatus",
    !!s.commTrendOn,
    (STATE && STATE.commTrendStatus) || [],
    s.commTrendConfInds,
    "Commodity add karo / trend read ka wait karo."
  );
}

function renderRoutingStatus() {
  const q = (k) => document.querySelector('#tab-realtime [data-set="' + k + '"]');
  const box = document.getElementById("rtRoutingStatus");
  if (!box) return;
  const val = (k) => { const e = q(k); return e && e.value ? e.value : ""; };
  const checked = (k) => !!(q(k) && q(k).checked);
  const runLabel = (v) => (v === "both" ? "Both" : v === "premium" ? "Premium" : "Spot");
  if (checked("premiumOnly")) {
    box.style.color = "#ffd700";
    box.textContent = "Premium chart only ON - every instrument runs AND trades on the option premium chart (run-in / trade-in choices locked). Only BULLISH indicator filters apply, on both the CE and PE side.";
    return;
  }
  // Realtime Trading Engine + Paper Trade 1 now run/trade F&O stocks only (the
  // Indices + Commodities pickers are removed on these tabs).
  if (!PAPER2 && !PAPER3) {
    const saved = [];
    if (checked("runInDefault")) saved.push("run-in");
    if (checked("tradeInDefault")) saved.push("trade-in");
    box.style.color = saved.length ? "#00d4aa" : "#888";
    box.textContent = "Run: F&O stocks " + runLabel(val("runFno")) + "  ·  Trade: F&O stocks Premium" + (saved.length ? "  (" + saved.join(" + ") + " saved as default for all future tasks)" : "");
    return;
  }
  const saved = [];
  if (checked("runInDefault")) saved.push("run-in");
  if (checked("tradeInDefault")) saved.push("trade-in");
  const tradeIdx = val("tradeInIndex") === "premium" ? "Premium" : val("tradeInIndex") === "both" ? "Both" : "Spot";
  const tradeComm = val("tradeInComm") === "premium" ? "Option premium" : "Futures contract";
  let run;
  let trade;
  if (INDICES_ONLY) {
    run = "Run: indices " + runLabel(val("runIndex"));
    trade = "Trade: indices " + tradeIdx;
  } else if (COMMODITIES_ONLY) {
    run = "Run: commodities " + runLabel(val("runComm"));
    trade = "Trade: commodities " + tradeComm;
  } else {
    run = "Run: indices " + runLabel(val("runIndex")) + ", F&O " + runLabel(val("runFno")) + ", commodities " + runLabel(val("runComm"));
    trade = "Trade: indices " + tradeIdx + ", F&O Premium, commodities " + tradeComm;
  }
  box.style.color = saved.length ? "#00d4aa" : "#888";
  box.textContent = run + "  ·  " + trade + (saved.length ? "  (" + saved.join(" + ") + " saved as default for all future tasks)" : "");
}

function syncInterlocks() {
  const q = (k) => document.querySelector('#tab-realtime [data-set="' + k + '"]');
  const checked = (k) => { const e = q(k); return !!(e && e.checked); };
  const val = (k) => { const e = q(k); return e ? e.value : ""; };
  const cq = (sel) => document.querySelector("#tab-realtime " + sel);
  const setv = (id, on) => {
    const row = document.getElementById(id);
    if (!row) return;
    row.style.opacity = on ? "0.35" : "1";
    row.style.pointerEvents = on ? "none" : "auto";
    row.querySelectorAll("select, input").forEach((x) => (x.disabled = on));
  };

  // --- Crossover read bar: forming vs closed, mutually exclusive ---
  const crossForming = q("indCrossOnForming");
  const crossClosed = q("indCrossOnClosed");
  if (crossForming && crossClosed) {
    // If both somehow arrive ticked, closed wins. Both unchecked is valid: no
    // crossover mode is active and neither box is faded.
    if (crossForming.checked && crossClosed.checked) crossForming.checked = false;
    fadeControl(crossForming, crossClosed.checked);
    fadeControl(crossClosed, crossForming.checked);
  }
  fadeControl(q("indCrossBars"), false);

  // --- AI Stop-Loss <-> Manual Overall SL / Trail SL ---
  const slChk = cq("#rtEngSl");
  const trailChk = cq("#rtEngTrailSl");
  const pointTrailChk = cq("#rtEngPointTrailSl");
  const linkChk = cq("#rtEngPtsFromPct");
  const manualSlOn = !!(slChk && slChk.checked);
  const manualTrailSlOn = !!(trailChk && trailChk.checked);
  const manualPointTrailSlOn = !!(pointTrailChk && pointTrailChk.checked);
  // "Pts = % (auto)": the engine derives the point jump from the Trail SL % per
  // strike (broker-only). While linked, the point-trail box + value are inert and
  // shown faded with an "auto" placeholder, because the % is the active control.
  const trailPtsLinkOn = !!(linkChk && linkChk.checked);
  const aiSlOn = checked("aiSl") && !manualSlOn && !manualTrailSlOn && !manualPointTrailSlOn && !trailPtsLinkOn;
  fadeControl(q("aiSl"), manualSlOn || manualTrailSlOn || manualPointTrailSlOn || trailPtsLinkOn);
  fadeControl(slChk, aiSlOn);
  fadeControl(trailChk, aiSlOn);
  fadeControl(pointTrailChk, aiSlOn || trailPtsLinkOn);
  fadeControl(linkChk, aiSlOn);
  fadeControl(cq("#rtEngSlPct"), aiSlOn || !manualSlOn);
  fadeControl(cq("#rtEngTrailSlPct"), aiSlOn || !manualTrailSlOn);
  fadeControl(cq("#rtEngPointTrailSlPts"), aiSlOn || trailPtsLinkOn || !manualPointTrailSlOn);

  // --- Risk:Reward overrides every TP control ---
  const rrOn = checked("rrEnabled");
  fadeControl(q("rrValue"), !rrOn);
  if (rrOn) {
    fadeControl(q("aiTpPct"), true);
  }
  renderRiskStatus();

  // --- Trade times: the time input only lives while its checkbox is on ---
  fadeControl(q("startAfter"), !checked("startAfterEnabled"));
  fadeControl(q("noTradeAfter"), !checked("noTradeAfterEnabled"));
  fadeControl(q("autoSquareOffTime"), !checked("autoSquareOffEnabled"));
  renderTimeStatus();
  renderNiftyStatus();
  renderInstTrendStatus();
  renderRoutingStatus();

  // --- Trades per strategy ---
  fadeControl(q("tradeLimitCount"), !(checked("tradeLimit") && !checked("aiTrades")));
  renderTradesStatus();

  // --- Strike pool: ATM / fastest-rising fade the normal count; fastest-rising
  //     is a REFINEMENT of "+green only" (biggest riser among the rising
  //     premiums), so it no longer fades/disable it - both work together.
  //     Manual Strike Select (testing) fades the whole automatic pool. ---
  const manualOn = checked("manualStrikesEnabled");
  const atm = val("strikeMode") === "atm";
  const fastOn = checked("fastestRising");
  const cnt = q("strikeCount");
  if (cnt) {
    const dis = atm || fastOn || manualOn;
    cnt.disabled = dis;
    cnt.style.opacity = dis ? "0.35" : "1";
    cnt.style.pointerEvents = dis ? "none" : "";
    const w = cnt.closest("label");
    if (w) { w.style.opacity = dis ? "0.5" : "1"; w.style.pointerEvents = dis ? "none" : ""; }
  }
  const fc = q("fastestCount");
  if (fc) {
    const on = fastOn && !manualOn;
    fc.disabled = !on;
    fc.style.opacity = on ? "1" : "0.35";
    fc.style.pointerEvents = on ? "" : "none";
    const w = fc.closest("label");
    if (w) { w.style.opacity = on ? "1" : "0.5"; w.style.pointerEvents = on ? "" : "none"; }
  }
  const pos = q("onlyPositive");
  if (pos) {
    const dis = manualOn;
    pos.disabled = dis;
    pos.style.opacity = dis ? "0.5" : "1";
    pos.style.pointerEvents = dis ? "none" : "";
    const w = pos.closest("label");
    if (w) { w.style.opacity = dis ? "0.6" : "1"; w.style.pointerEvents = dis ? "none" : ""; }
  }
  // --- Opposite-side fallback: needs +green on AND a single-sided "Execute
  //     Trade In" mode (both-sided / ATM-only have no opposite side). Works the
  //     same with fastest-rising on (both share the direction + fallback). ---
  const fb = q("fallbackOppositeSide");
  if (fb) {
    const mdl = val("strikeMode");
    const singleSide = mdl === "above" || mdl === "above_atm" || mdl === "below" || mdl === "below_atm";
    const dis = manualOn || !checked("onlyPositive") || !singleSide;
    fb.disabled = dis;
    fb.style.opacity = dis ? "0.5" : "1";
    fb.style.pointerEvents = dis ? "none" : "";
    const w = fb.closest("label");
    if (w) { w.style.opacity = dis ? "0.6" : "1"; w.style.pointerEvents = dis ? "none" : ""; }
  }
  fadeControl(q("optionSide"), manualOn);
  fadeControl(q("strikeMode"), manualOn);
  fadeControl(q("fastestRising"), manualOn);
  const msBox = document.getElementById("rtManualStrikeBox");
  if (msBox) msBox.style.display = manualOn ? "flex" : "none";
  const msHint = document.getElementById("rtManualStrikeHint");
  if (msHint) msHint.style.opacity = manualOn ? "1" : "0.6";
  if (manualOn && !MANUAL_INSTRUMENTS) manualLoadInstruments(false);
  // Manual Strike Select freezes the automatic scanner masters (Top Movers /
  // NIFTY Trend / Commodities). Their toggle label + disabled state is set in
  // syncAstToggles; here we dim the whole row so an inactive universe reads as
  // inactive. Their persisted settings are untouched and resume when the mode
  // is switched off.
  ["rtMoversToggle", "rtNiftyTrendToggle", "rtCommodityToggle", "rtIndexTrendToggle", "rtCommTrendToggle"].forEach((id) => {
    const b = document.getElementById(id);
    const row = b && b.closest ? b.closest(".rt-engine-row") : null;
    if (row) row.style.opacity = manualOn ? "0.45" : "1";
  });

  // --- Premium-only lock: the run-in / trade-in selectors are engine-pinned ---
  const prem = checked("premiumOnly");
  setv("rtRunInRow", prem);
  setv("rtTradeInRow", prem);

  // --- NIFTY trend-following: the confirmation-indicator picker is live only
  //     while the trend mode is active. ---
  const s = (STATE && STATE.settings) || {};
  const trendEnabled = !!s.niftyTrendOn;
  const ntActive = trendEnabled;
  const dimRow = (id, on) => {
    const row = document.getElementById(id);
    if (row) {
      row.disabled = !!on;
      row.style.opacity = on ? "0.4" : "1";
      row.style.pointerEvents = on ? "none" : "";
    }
  };
  dimRow("rtNiftyTrendConfIndSelect", !ntActive);
  dimRow("rtNiftyTrendConfIndAdd", !ntActive);
  dimRow("rtNiftyTrendColorCb", !ntActive);
  dimRow("rtNiftyNetStrictCb", !ntActive);
  dimRow("rtNiftyAllAgreeStrictCb", !ntActive);
  dimRow("rtNiftySidewaysCb", !ntActive);
  dimRow("rtNiftyAntiFakeCb", !ntActive);
  // Paper Trade 2 per-instrument trend pickers follow their own toggle.
  if (PAPER2) {
    const idxTrendActive = !!s.indexTrendOn;
    dimRow("rtIndexTrendConfIndSelect", !idxTrendActive);
    dimRow("rtIndexTrendConfIndAdd", !idxTrendActive);
    dimRow("rtIndexSidewaysCb", !idxTrendActive);
    dimRow("rtIndexAntiFakeCb", !idxTrendActive);
    const commTrendActive = !!s.commTrendOn;
    dimRow("rtCommTrendConfIndSelect", !commTrendActive);
    dimRow("rtCommTrendConfIndAdd", !commTrendActive);
    dimRow("rtCommSidewaysCb", !commTrendActive);
    dimRow("rtCommAntiFakeCb", !commTrendActive);
  }

  // --- Top Movers: independent of NIFTY trend-following. Both masters can be
  //     ON together and run side by side (they are connected), so there is no
  //     fade-out between them any more - only the controls for a scanner that
  //     is itself OFF are dimmed. ---
  const mvActive = !!s.moversOn;
  ["moversGainers", "moversLosers"].forEach((k) => dimControl(q(k), !mvActive));
  dimRow("rtMoversIndicesSelect", !mvActive);
  dimRow("rtMoversIndicesAdd", !mvActive);
  const mvChips = document.getElementById("rtMoversIndicesList");
  if (mvChips) mvChips.style.opacity = mvActive ? "1" : "0.5";

  // --- Liquid F&O scanner: ranks only the liquid index basket, so its pickers
  //     are gated on the scanner's own master toggle (and frozen by Manual
  //     Strike Select like every other automatic universe). ---
  const lqActive = !!s.liquidOn && !s.manualStrikesEnabled;
  ["liquidGainers", "liquidLosers"].forEach((k) => dimControl(q(k), !lqActive));
  ["rtLiquidGainerSelect", "rtLiquidLoserSelect", "rtLiquidAddSelect", "rtLiquidAddBtn"].forEach((id) =>
    dimRow(id, !lqActive)
  );
  const lqChips = document.getElementById("rtLiquidChips");
  if (lqChips) lqChips.style.opacity = lqActive ? "1" : "0.5";
  // Equity scan controls follow the same master toggle.
  ["rtEqTradeCountRow", "rtEqScanTimeRow", "rtEqMinTurnoverRow", "rtEqScanNow", "rtEqScanPickRow"].forEach((id) => {
    const el = document.getElementById(id);
    if (el && el.style.display !== "none") el.style.opacity = lqActive ? "1" : "0.6";
  });
  // The liquid-only lot policy is inert until the scanner arms liquid legs.
  const lqLotWrap = document.getElementById("rtLiquidLotWrap");
  if (lqLotWrap) lqLotWrap.style.opacity = lqActive ? "1" : "0.5";
  // Paper Trade 2's combined "Indices & Commodities" section: the single toggle
  // drives both pickers, so dim the commodity controls with the same ON state.
  if (PAPER2) {
    const combOn = !!s.moversOn || !!s.commodityOn;
    dimRow("rtCommodityAdd", !combOn);
    dimRow("rtCommodityAddBtn", !combOn);
    const cc = document.getElementById("rtCommodityChips");
    if (cc) cc.style.opacity = combOn ? "1" : "0.5";
  }

  // --- Indicator filter sections: a side with nothing selected is dimmed, and
  //     the section master auto-arms when any sub-filter is ticked. ---
  ["Bull", "Bear"].forEach((side) => {
    const sec = document.getElementById("rtFilterSection" + side);
    if (!sec) return;
    const master = sec.querySelector("[data-master]");
    const subs = Array.from(sec.querySelectorAll("[data-filter]"));
    let on = !!(master && master.checked) || subs.some((b) => b.checked);
    if (master) master.checked = on;
    sec.style.opacity = on ? "1" : "0.45";
  });

  // --- PRIMARY "Entry indicator filter": the dropdown/add/chips only matter
  //     while the switch is ON, and the info line spells out the gate order so
  //     the operator can see PRIMARY runs before the secondary filters. ---
  const epOn = !!s.entryPrimaryOn;
  const epList = Array.isArray(s.entryPrimaryFilters) ? s.entryPrimaryFilters.filter((x) => String(x || "").trim()) : [];
  ["rtEntryPrimarySelect", "rtEntryPrimaryAdd"].forEach((id) => {
    const e = document.getElementById(id);
    if (!e) return;
    e.disabled = !epOn;
    e.style.opacity = epOn ? "1" : "0.5";
  });
  const epListEl = document.getElementById("rtEntryPrimaryList");
  if (epListEl) epListEl.style.opacity = epOn ? "1" : "0.5";
  const epInfo = document.getElementById("rtEntryPrimaryInfo");
  if (epInfo) {
    epInfo.textContent = !epOn
      ? "OFF - sirf normal (secondary) indicator filters chalenge, jaise pehle."
      : epList.length
        ? `ON - pehle ${epList.length} primary condition (${epList.map(filterKeyLabel).join(", ")}) check hogi; pass hone par hi baaki normal filters (strict AND / AI Brain / majority) chalti hain. Fail = entry block.`
        : "ON - abhi koi primary filter add nahi kiya, isliye gate purana hi rahega. Dropdown se indicator filter add karo.";
  }

  // --- Manual filter list goes inactive while a saved AST template owns the
  //     active Top Movers direction (the template supplies the filters). ---
  const hasTpl = (k) => String(s[k] || "").trim() !== "";
  const listLocked = !!s.moversOn && (hasTpl("moverBullTemplate") || hasTpl("moverBearTemplate"));
  const frow = document.getElementById("rtFilterRow");
  if (frow) {
    frow.style.opacity = listLocked ? "0.6" : "";
    frow.querySelectorAll("input[type=checkbox], select, button").forEach((c) => {
      // Crossover forming/closed own their disabled state (the active one fades
      // and disables the other), so this row lock must not re-enable them.
      if (c.id === "rtIndCrossForming" || c.id === "rtIndCrossClosed") return;
      c.disabled = listLocked;
    });
    frow.title = listLocked
      ? "Manual indicator filters inactive: a saved AST template is assigned to the active Top Movers / NIFTY trend direction and supplies the filters."
      : "";
  }
  // Paper Trade 2: lock the manual CE/PE leg dropdown while its trend section is ON.
  syncTrendLegLock();
}

function applySettingsToDom(s) {
  if (!s) return;
  const ae = document.activeElement;
  const busy = (k) => ae && ae.hasAttribute && ((ae.getAttribute("data-set") === k) || (ae.getAttribute("data-list") === k));
  document.querySelectorAll("#tab-realtime [data-set]").forEach((inp) => {
    const k = inp.getAttribute("data-set");
    if (!(k in s)) return;
    if (busy(k)) return;
    if (inp.type === "checkbox") inp.checked = !!s[k];
    else inp.value = s[k] == null ? "" : s[k];
  });
  const intLists = ["moversIndices", "commodityList", "scannerExclude", "liquidList"];
  document.querySelectorAll("#tab-realtime [data-list]").forEach((inp) => {
    const k = inp.getAttribute("data-list");
    if (!(k in s)) return;
    if (busy(k)) return;
    const arr = s[k];
    inp.value = Array.isArray(arr) ? arr.join(intLists.indexOf(k) >= 0 ? ", " : ", ") : (arr || "");
  });
  const filters = s.filters || {};
  // While a filter toggle is still saving, keep the user's ticks untouched: a
  // snapshot fetched before the click would otherwise silently uncheck them.
  if (!filtersBusy)
    document.querySelectorAll("#tab-realtime [data-filter]").forEach((cb) => {
      const k = cb.getAttribute("data-filter");
      if (k in filters) cb.checked = !!filters[k];
    });
  syncAstToggles(s);
  applyDataPoolUi(!!s.data_pool);
  applyEngineScanUI(s);
  refreshChartKindUi();
  // Strike-selection base must stay ON for the full engine (see
  // strikeBaseNormalized). Paper Trade 2 is exempt: it keeps whatever the
  // operator chose, so the +green base can be turned OFF and the Above/Below-ATM
  // + Number-of-Strikes selection still applies.
  if (!PAPER2 && !EQUITY_ONLY && !strikeBaseNormalized && (s.onlyPositive === false || s.fallbackOppositeSide === false)) {
    strikeBaseNormalized = true;
    ["onlyPositive", "fallbackOppositeSide"].forEach((k) => {
      const e = document.querySelector('#tab-realtime [data-set="' + k + '"]');
      if (e) e.checked = true;
    });
    uiAlert(
      '"Only +green premium strikes" aur "Opposite side fallback (if all minus)" ON rakhna zaroori hai - "Execute Trade In" ke sare dropdown options isi base par kaam karte hain. Dono enable kar diye gaye hain.'
    );
    pushSetting({ onlyPositive: true, fallbackOppositeSide: true });
  }
  syncInterlocks();
  // Applied AFTER the interlocks: those dim/undim the filter row and would
  // otherwise re-enable controls the brain owns. Brain read-only wins last.
  syncBrainUi(s);
  applyRunModeUI();
  renderAllChips();
  renderTemplateAssignment();
  renderScannerRemoved();
  renderTradeSessions();
}

// Reflect engine toggles (Top Movers / NIFTY Trend / Commodities)
// on their button labels, matching the old AST master buttons.
function syncAstToggles(s) {
  const manualOn = !!s.manualStrikesEnabled;
  const set = (id, on, label, blocked) => {
    const b = document.getElementById(id);
    if (!b) return;
    b.textContent = label;
    b.disabled = !!blocked;
    b.style.pointerEvents = blocked ? "none" : "";
    b.style.cursor = blocked ? "not-allowed" : "";
    b.classList.toggle("on", !blocked && on);
    b.classList.toggle("off", !blocked && !on);
    b.classList.toggle("blocked", !!blocked);
    b.style.background = "";
    b.style.color = "";
    b.style.opacity = blocked ? "0.5" : "1";
  };
  // Manual Strike Select makes every automatic scanner universe inactive, so the
  // three master toggles are frozen (their persisted state is kept and resumes
  // the moment the mode is switched off).
  const suffix = manualOn ? " (inactive)" : "";
  const trendOn = !!s.niftyTrendOn;
  const commOn = !!s.commodityOn;
  // Paper Trade 2 is indices only ("Indices:" master); Paper Trade 3 is
  // commodities only (the hidden movers master is irrelevant); Realtime 2/3
  // merge indices + commodities into one master that arms both scanners.
  const combinedOn = !!(s.moversOn || commOn);
  const moversState = INDICES_ONLY ? !!s.moversOn : (PAPER2 && !COMMODITIES_ONLY ? combinedOn : !!s.moversOn);
  const moversLabel = INDICES_ONLY
    ? "Indices: "
    : PAPER2 && !COMMODITIES_ONLY
    ? "Indices & Commodities: "
    : "Top Movers: ";
  set(
    "rtMoversToggle",
    moversState,
    moversLabel + (moversState ? "ON" : "OFF") + suffix,
    manualOn
  );
  const liquidOn = !!s.liquidOn;
  // Liquid F&O master exists only on the full engines (Realtime 1 / Paper 1).
  set(
    "rtLiquidToggle",
    liquidOn,
    "Liquid F&O: " + (liquidOn ? "ON" : "OFF") + suffix,
    manualOn
  );
  set("rtNiftyTrendToggle", trendOn, "Trend Follow: " + (trendOn ? "ON" : "OFF") + suffix, manualOn);
  set("rtCommodityToggle", commOn, "Commodities: " + (commOn ? "ON" : "OFF") + suffix, manualOn);
  if (PAPER2) {
    const idxTrendOn = !!s.indexTrendOn;
    const commTrendOn = !!s.commTrendOn;
    set("rtIndexTrendToggle", idxTrendOn, "Trend Follow: " + (idxTrendOn ? "ON" : "OFF") + suffix, manualOn);
    set("rtCommTrendToggle", commTrendOn, "Trend Follow: " + (commTrendOn ? "ON" : "OFF") + suffix, manualOn);
  }
}

function filterIsBull(k) {
  if (String(k).startsWith("Bull")) return true;
  if (String(k).startsWith("Bear")) return false;
  return String(k).toUpperCase().includes("UP");
}

// AI Smart margin bar (above Running Trades): budget, locked by open trades and
// the amount still available. The engine blocks + warns the next trade whose
// required margin exceeds the available amount.
function renderMarginBar(m) {
  const bar = document.getElementById("rtMarginBar");
  if (!bar) return;
  const budget = num(m && m.budget);
  const locked = num(m && m.locked);
  const count = m && m.count != null ? num(m.count) : 0;
  const avail = m && m.available != null ? num(m.available) : Math.max(0, budget - locked);
  // The shared-margin allotment is the ONLY margin source, so the bar is shown
  // even when nothing is allotted (that is precisely why entries are blocked).
  bar.style.display = "flex";
  const availCol = avail > 0 ? "#00d4aa" : "#ef5350";
  const allotted = !!(m && m.allotted);
  bar.innerHTML =
    '<b style="color:#00d4aa;white-space:nowrap">Tab margin allotment:</b>' +
    '<b style="color:' + (allotted ? "#ffd700" : "#ef5350") + ';font-size:11px;white-space:nowrap">' +
    (allotted ? fmtMoney(budget) : "₹0 - no allotment, entries blocked") + "</b>" +
    '<span style="color:#888;white-space:nowrap">Locked by ' + count + " running trade" + (count === 1 ? "" : "s") + ': <b style="color:#ffd700">' + fmtMoney(locked) + "</b></span>" +
    '<span style="color:#888;white-space:nowrap">Available now: <b style="color:' + availCol + '">' + fmtMoney(avail) + "</b></span>" +
    '<span style="color:#666;font-size:8px">set the allotment in the Margin allotment panel above; the family pool total is in the app header</span>';
}

// Old-app "Open Chart": focus the main chart on this trade's contract and let
// the parent window's overlay poller draw the live P&L / SL / trail-SL lines on
// the candles. The Paper Trade pane runs inside an iframe, so it forwards the
// request to the parent instead of trying to chart inside the frame.
function openChartForTrade(info) {
  const payload = {
    type: "openTradeChart",
    securityId: Number(info.securityId || info.sid || 0),
    exchangeSegment: info.exchangeSegment || info.exch || "",
    instrument: info.instrument || info.inst || "",
    tradingSymbol: info.tradingSymbol || info.label || info.name || "",
    paper: !!window.__PAPER__,
    // Engine base path for the parent's chart-overlay poller, so the second
    // paper frame reads its own ledger (`/api/paper2`) instead of the first, and
    // Realtime Trading Engine 2 reads its own live ledger (`/api/realtime2`).
    paperBase: window.__PAPER__
      ? (window.__PAPER_BASE__ || "/api/paper")
      : (window.__RT_BASE__ || "/api/rt"),
  };
  if (!payload.securityId) return;
  if (window.parent && window.parent !== window) {
    try { window.parent.postMessage(payload, "*"); } catch (_) {}
  }
  if (typeof window.openTradeChart === "function") {
    try { window.openTradeChart(payload); } catch (_) {}
  }
}

// Chart routing for one running strategy: the chart the engine evaluates the
// entry on (`runMode`) and the chart the order executes on (`tradeMode`), with
// the resolved premium leg for each. `spot` uses the strategy's own instrument;
// `premium` / `both` use the engine-resolved option contract, so the Running
// Strategies view can show - and open - the exact chart each strategy runs on.
function strategyChartInfo(x, leg, mode) {
  const label =
    mode === "premium" ? "Premium" : mode === "both" ? "Spot+Premium" : mode === "futures" ? "Futures" : "Spot";
  // The server publishes the exact run leg for the mode (underlying spot when
  // run mode is spot, option premium when premium/both). Prefer it whenever it
  // is present so an option-instrument strategy does not show its own premium
  // chart while labelled "Spot".
  const useLeg = !!leg;
  const info = useLeg
    ? { securityId: leg.securityId, segment: leg.segment, instrument: leg.instrument, tradingSymbol: leg.tradingSymbol }
    : {
        securityId: x.securityId,
        segment: x.exchangeSegment,
        instrument: x.instrument,
        tradingSymbol: x.tradingSymbol || x.name || String(x.securityId || ""),
      };
  return { label, info, resolved: !!info.securityId };
}

function chartBadgeHTML(prefix, chart) {
  if (!chart || !chart.resolved) {
    return '<span style="color:#666">' + prefix + ": resolving\u2026</span>";
  }
  const i = chart.info;
  const btn =
    '<button class="btn-action" data-openchart="' + esc(String(i.securityId || "")) +
    '" data-exch="' + esc(i.segment || "") +
    '" data-inst="' + esc(i.instrument || "") +
    '" data-sym="' + esc(i.tradingSymbol || "") +
    '" style="width:auto;padding:0 5px;margin:0 0 0 4px;font-size:9px" title="Is chart ko kholo">Chart</button>';
  return (
    '<span style="color:#888">' + prefix + ":</span> " +
    '<b style="color:#8ab4ff">' + esc(chart.label) + "</b> " +
    '<span style="color:#ccc">' + esc(i.tradingSymbol || i.securityId) + "</span>" + btn
  );
}

// LTP change cell for the trade lists: absolute (₹) + percent with an explicit
// plus/minus sign, coloured by direction, so the option premium's sign (the point
// of the "+green premium" pick) is visible next to each trade. `--` when no quote.
function chgCell(chg, pct) {
  const c = num(chg);
  const p = num(pct);
  if (c === 0 && p === 0) return { txt: "--", col: "#888" };
  const col = c >= 0 ? "#00d4aa" : "#ef5350";
  const sgn = (v) => (v >= 0 ? "+" : "-");
  return { txt: `${sgn(c)}${Math.abs(c).toFixed(2)} (${sgn(p)}${Math.abs(p).toFixed(2)}%)`, col };
}

function renderRunning(s) {
  const positions = s.positions || [];
  const broker = s.brokerPositions || [];
  const settings = s.settings || {};
  const sel = s.selected || {};
  // The engine's run set (matches scan_signals()): saved strategies that are
  // enabled, OR the manually ticked run set while "Call manually selected
  // strategies" is on, OR the live AI-trader picks while it is on.
  const callManual = settings.callManual !== false;
  const aiPicks = new Set(s.aiPicks || []);
  const running = (s.strategies || []).filter((x) => x.enabled || (callManual && sel[x.id]) || aiPicks.has(x.id));
  // Scanner / synthetic strategies (Top Movers, NIFTY trend, Manual Strike) never
  // live in `s.strategies`, so they are normally surfaced only through the
  // volatile Picked-Strikes readout. That readout is empty whenever the scanner
  // pollers yield to the operator (or right after a restart), which left this
  // list blank while scanner trades were still open. Derive a running row straight
  // from each open position the saved list does not already cover, so the panel
  // always mirrors what is actually trading.
  const savedIds = new Set((s.strategies || []).map((x) => String(x.id)));
  const seenDerived = new Set();
  const derived = positions
    .filter((p) => {
      const id = String(p.strategyId || "");
      if (!id || savedIds.has(id) || seenDerived.has(id)) return false;
      seenDerived.add(id);
      return true;
    })
    .map((p) => {
      const id = String(p.strategyId);
      const symbol = String(p.tradingSymbol || "");
      const bear =
        /:PE$/i.test(id) || /-PE$/i.test(symbol) || /bear/i.test(String(p.strategyName || ""));
      const leg = {
        securityId: p.securityId,
        tradingSymbol: symbol,
        segment: p.exchangeSegment,
        instrument: p.instrument,
      };
      return {
        id,
        name: p.strategyName || symbol || id,
        category: bear ? "BEARISH" : "BULLISH",
        engineTf: "",
        securityId: p.securityId,
        exchangeSegment: p.exchangeSegment,
        instrument: p.instrument,
        tradingSymbol: symbol,
        runMode: "premium",
        tradeMode: "premium",
        runLeg: leg,
        tradeLeg: leg,
        derived: true,
      };
    });
  const modeLabel = "Real (live)";
  const armedFilters = Object.keys(settings.filters || {}).filter((k) => settings.filters[k]);
  const filtersBull = armedFilters.filter(filterIsBull).length;
  const filtersBear = armedFilters.length - filtersBull;
  const bullNames = armedFilters.filter(filterIsBull);
  const bearNames = armedFilters.filter((k) => !filterIsBull(k));
  const sideWord = (bull) => (bull ? "bullish" : "bearish");
  const sideCol = (bull) => (bull ? "#00d4aa" : "#ef5350");
  // The ticked indicator filters that gate a side (bullish names for a CE leg,
  // bearish names for a PE leg) - shared by the Running Strategies list and the
  // Picked Strikes (premium) list.
  const filtName = (bull) => (bull ? bullNames : bearNames);
  // Per-running-strategy detail line: Overall SL / Trail SL / Lot size / Lot qty.
  // Live values come from the open position; while waiting, the configured plan
  // (from settings) is shown so the operator always sees what will be applied.
  const fmtLot = (v) => (num(v) > 0 ? String(num(v)) : "auto");
  const trailLabel = (pct, pts) => {
    const a = [];
    if (num(pct) > 0) a.push(num(pct).toFixed(2) + "%");
    if (num(pts) > 0) a.push(num(pts).toFixed(2) + " pts");
    return a.length ? a.join(" + ") : "-";
  };
  const plannedDetail = () => {
    const ov = settings.manualSl ? num(settings.manualSlPct).toFixed(2) + "%" : "-";
    const linked = !!settings.trailPointsFromPct && !!settings.manualTrailSl && num(settings.manualTrailSlPct) > 0;
    const tr = linked
      ? trailLabel(num(settings.manualTrailSlPct), 0) + " (pts auto)"
      : trailLabel(
          settings.manualTrailSl ? settings.manualTrailSlPct : 0,
          settings.manualPointTrailSl ? settings.manualPointTrailSlPoints : 0
        );
    const lots = num(settings.lots) > 0 ? num(settings.lots) : 1;
    return `Overall SL ${ov} · Trail SL ${tr} · Lot size ${fmtLot(settings.lotSize)} · Lots ${lots}`;
  };
  const posDetail = (p) => {
    const ov = num(p.sl) > 0 ? num(p.sl).toFixed(2) : settings.manualSl ? num(settings.manualSlPct).toFixed(2) + "%" : "-";
    const lots = num(p.lots) > 0 ? num(p.lots) : 0;
    const sN = Array.isArray(p.slices) ? p.slices.length : 0;
    const sliceTxt = sN > 1 ? ` · ${sN} slices` : "";
    // Both the % Trail SL and the point Trail SL run app-side; with
    // "Pts = % (auto)" linked the points are the exact live equivalent of the %.
    const tr = trailLabel(p.trail, p.pointTrailLive != null ? p.pointTrailLive : p.pointTrail);
    return `Overall SL ${ov} · Trail SL ${tr} · Lot size ${fmtLot(p.lotSize)} · Qty ${num(p.qty)}${lots ? ` (${lots} lot${lots === 1 ? "" : "s"})` : ""}${sliceTxt}`;
  };
  // Indicator-filters run mode trades the scanner universe (Top Movers / NIFTY
  // trend / Commodities) with synthetic strategies that never live in
  // `s.strategies`, so surface the struck contracts here: picked option strike +
  // the ticked indicator filters that gate that side.
  const strikes = s.strikes || [];
  const manualMode = !!settings.manualStrikesEnabled;
  const scannerOn = !!(
    settings.filterMode ||
    settings.moversOn ||
    settings.niftyTrendOn ||
    settings.commodityOn ||
    settings.liquidOn ||
    manualMode
  );
  // "Running Strategies" list = the indicator-filter based strategies actually
  // running (saved enabled / manually selected / live AI-trail picks) plus any
  // row derived from an open position. It is split into its OWN sub-lists by the
  // chart each strategy is RUN on (Spot chart / Premium chart), so the operator
  // sees at a glance which chart the indicator filters evaluate on. The strikes
  // SELECTED for trade execution are shown in their own "Picked Strikes
  // (Premium)" list, separate from the strategies.
  const allRunning = running.concat(derived);

  const runStrat = document.getElementById("rtRunStrategies");
  if (runStrat) {
    const head =
      `<div style="display:flex;justify-content:space-between;gap:6px;font-size:9px;color:#888;padding:0 0 3px">` +
      `<span>${s.engineOn ? '<span style="color:#00d4aa">Engine RUNNING</span>' : '<span style="color:#ffd700">Engine OFF</span>'} · ${allRunning.length} strateg${allRunning.length === 1 ? "y" : "ies"} · ${esc(modeLabel)}</span>` +
      `<span>${s.armed ? '<span style="color:#ef5350">ARMED</span>' : '<span style="color:#666">disarmed</span>'}${armedFilters.length ? ` · filters ${filtersBull}CE/${filtersBear}PE` : ""}</span>` +
      `</div>`;
    const stratRow = (x) => {
      const bull = stratBull(x);
      const pos = positions.filter((p) => (p.strategyId && p.strategyId === x.id) || (p.strategyName && x.name && p.strategyName === x.name));
      const pnl = pos.reduce((a, p) => a + num(p.pnl), 0);
      const filt = filtName(bull);
      const names = filt.slice(0, 4).join(", ") + (filt.length > 4 ? ` +${filt.length - 4}` : "");
      const tail = pos.length
        ? `<span style="color:${num(pnl) >= 0 ? "#00d4aa" : "#ef5350"}">${pos.length} trade · ${num(pnl).toFixed(2)}</span>`
        : filt.length
        ? `<span style="color:#ffd700" title="${esc(filt.join(", "))}">waiting · ${filt.length} filter${names ? " (" + esc(names) + ")" : ""}</span>`
        : `<span style="color:#666">waiting</span>`;
      // The Run chart follows the operator's "Strategy should be run in"
      // routing: spot mode opens the underlying spot chart, premium mode opens
      // the option-premium chart. The trade (premium) chart lives in the
      // Picked Strikes list, not here.
      const runChart = strategyChartInfo(x, x.runLeg, x.runMode);
      return `<div style="font-size:9px;border:1px solid #1e1e40;border-radius:3px;padding:2px 6px;margin:2px 0">
        <div style="display:flex;justify-content:space-between;gap:6px">
          <span><b style="color:#d0d0d0">${esc(x.name || x.id)}</b> <span style="color:${sideCol(bull)}">${sideWord(bull)}</span></span>
          <span style="color:#666">${esc(x.engineTf || x.timeframe || "")} · ${tail}</span>
        </div>
        <div style="display:flex;gap:12px;flex-wrap:wrap;padding:2px 0 0 2px;font-size:8px">
          ${chartBadgeHTML("Run chart", runChart)}
        </div>
        <div style="font-size:8px;color:#8aa0c0;padding:2px 0 0 2px">${esc(pos.length ? posDetail(pos[0]) : plannedDetail())}</div></div>`;
    };
    if (allRunning.length) {
      const bucket = { spot: [], premium: [], other: [] };
      allRunning.forEach((x) => {
        const m = String(x.runMode || "").toLowerCase();
        if (m === "spot") bucket.spot.push(x);
        else if (m === "premium" || m === "both") bucket.premium.push(x);
        else bucket.other.push(x);
      });
      const groupLabel = (k, n) =>
        `<div style="font-size:9px;color:#8ab4ff;font-weight:700;margin:4px 0 2px">${k === "spot" ? "Spot chart" : k === "premium" ? "Premium chart" : "Other chart"} (${n})</div>`;
      let html = head;
      for (const k of ["spot", "premium", "other"]) {
        if (!bucket[k].length) continue;
        html += groupLabel(k, bucket[k].length) + bucket[k].map(stratRow).join("");
      }
      runStrat.innerHTML = html;
    } else {
      const needFilters = !!settings.filterMode && armedFilters.length === 0;
      const emptyMsg = needFilters
        ? 'Indicator filter mode ON hai, par koi bullish/bearish filter tick nahi. Filter tick karo, phir scanner strategies yahan dikhengi.'
        : 'Koi strategy running nahi. "Run Paper Trading" dabao, phir engine ON karo.';
      runStrat.innerHTML =
        head +
        `<div style="font-size:10px;color:#666;padding:2px 2px">${emptyMsg}</div>`;
    }
  }

  const runStrikes = document.getElementById("rtRunStrikes");
  if (runStrikes) {
    const head =
      `<div style="display:flex;justify-content:space-between;gap:6px;font-size:9px;color:#888;padding:0 0 3px">` +
      `<span>Selected strikes for execution · ${strikes.length}</span>` +
      `<span>${s.engineOn ? '<span style="color:#00d4aa">Engine RUNNING</span>' : '<span style="color:#ffd700">Engine OFF</span>'}</span>` +
      `</div>`;
    if (scannerOn && strikes.length) {
      runStrikes.innerHTML =
        head +
        strikes
          .map((r) => {
            const bull = String(r.side || "").toUpperCase() === "CE";
            const isManualRow = manualMode || String(r.source || "") === "Manual";
            const pos = positions.filter((p) => String(p.securityId) === String(r.securityId));
            const pnl = pos.reduce((a, p) => a + num(p.pnl), 0);
            const filt = filtName(bull);
            const names = filt.slice(0, 4).join(", ") + (filt.length > 4 ? ` +${filt.length - 4}` : "");
            const tail = pos.length
              ? `<span style="color:${num(pnl) >= 0 ? "#00d4aa" : "#ef5350"}">${pos.length} trade · ${num(pnl).toFixed(2)}</span>`
              : filt.length
              ? `<span style="color:#ffd700" title="${esc(filt.join(", "))}">waiting · ${filt.length} filter${names ? " (" + esc(names) + ")" : ""}</span>`
              : isManualRow
              ? `<span style="color:#888">manual strike · waiting</span>`
              : `<span style="color:#666">waiting</span>`;
            // This panel is the TRADE (premium) list only. The run chart is in
            // "Running Strategies", routed by "Strategy should be run in".
            const tradeBtn =
              '<button class="btn-action" data-openchart="' + esc(String(r.securityId || "")) +
              '" data-exch="' + esc(r.segment || r.exchangeSegment || "") + '" data-inst="' + esc(r.instrument || "") +
              '" data-sym="' + esc(r.tradingSymbol || "") + '" style="width:auto;padding:0 5px;margin:0 0 0 4px;font-size:9px" title="Trade (premium) chart">Chart</button>';
            const tradeChip =
              '<span style="color:#888">Trade:</span> <b style="color:#8ab4ff">Premium</b> <span style="color:#ccc">' + esc(r.tradingSymbol || "") + "</span>" + tradeBtn;
            return `<div style="font-size:9px;border:1px solid #1e1e40;border-radius:3px;padding:2px 6px;margin:2px 0">
              <div style="display:flex;justify-content:space-between;gap:6px">
                <span><b style="color:#d0d0d0">${esc(r.underlying || "")}</b> <span style="color:${sideCol(bull)}">${sideWord(bull)}</span> <span style="color:#666">${esc(sourceLabel(r.source))}</span></span>
                <span>${tail}</span>
              </div>
              <div style="display:flex;gap:12px;flex-wrap:wrap;padding:2px 0 0 2px;font-size:8px">${tradeChip}</div>
              <div style="font-size:8px;color:#8aa0c0;padding:2px 0 0 2px">${esc(pos.length ? posDetail(pos[0]) : plannedDetail())}</div></div>`;
          })
          .join("");
    } else {
      runStrikes.innerHTML =
        head +
        `<div style="font-size:10px;color:#666;padding:2px 2px">Koi picked strike nahi. Scanner (Top Movers / NIFTY trend / Commodity / Manual) ON hone par selected strikes yahan dikhenge.</div>`;
    }
  }

  const runTrades = document.getElementById("rtRunTrades");
  if (runTrades) {
    // De-dupe: an engine position already mirrors its Dhan position, so only
    // show broker rows the engine is not tracking (e.g. manual Dhan trades).
    const engineBrokerSec = new Set(positions.filter((p) => p.broker).map((p) => String(p.securityId)));
    const dhanOnly = broker.filter((p) => !engineBrokerSec.has(String(p.securityId)));
    let total = 0;
    const pnlCell = (v) => {
      const cls = num(v) >= 0 ? "#00d4aa" : "#ef5350";
      return `<td style="color:${cls}">${num(v) >= 0 ? "+" : "-"}${fmtMoney(Math.abs(num(v)))}</td>`;
    };
    const rows = positions
      .map((p) => {
        const pnl = num(p.pnl);
        total += pnl;
        const ch = chgCell(p.ltpChange, p.ltpChangePct);
        return `<tr>
          <td><b>${esc(p.tradingSymbol || p.securityId)}</b>${orderBadge(p.orderType, p.limitPrice)}<br><span style="font-size:8px;color:#666">REAL · ${esc(p.method || "")}${p.broker ? " · broker" : ""}</span></td>
          <td>${num(p.qty)}</td>
          <td>${num(p.entry).toFixed(2)}</td>
          <td>${num(p.ltp).toFixed(2)}</td>
          <td style="color:${ch.col};white-space:nowrap">${ch.txt}</td>
          ${pnlCell(pnl)}
          <td style="white-space:nowrap">
            <button class="btn-action" data-openchart="${esc(String(p.securityId || ""))}" data-exch="${esc(p.exchangeSegment || "")}" data-inst="${esc(p.instrument || "")}" data-sym="${esc(p.tradingSymbol || "")}" style="width:auto;padding:2px 8px;margin:0 4px 0 0;font-size:10px" title="Is trade ka chart kholo (live P&amp;L / SL / trail lines)">Chart</button>
            <button class="btn-action warn" data-runclose="${esc(p.id)}" style="width:auto;padding:2px 8px;margin:0;font-size:10px">Close</button>
          </td>
        </tr>`;
      })
      .concat(
        dhanOnly.map((p) => {
          const pnl = num(p.pnl);
          total += pnl;
          const ch = chgCell(p.ltpChange, p.ltpChangePct);
          return `<tr>
            <td><b>${esc(p.tradingSymbol || p.securityId)}</b><br><span style="font-size:8px;color:#666">DHAN · ${esc(p.exchangeSegment || "")} · ${esc(p.positionType || "")}${p.productType ? " · " + esc(p.productType) : ""}</span></td>
            <td>${num(p.netQty)}</td>
            <td>${num(p.buyAvg).toFixed(2)}</td>
            <td>${num(p.ltp).toFixed(2)}</td>
            <td style="color:${ch.col};white-space:nowrap">${ch.txt}</td>
            ${pnlCell(pnl)}
            <td></td>
          </tr>`;
        })
      )
      .join("");
    if (!rows) {
      runTrades.innerHTML = `<div style="font-size:10px;color:#666;padding:6px">Dhan me koi open position nahi hai.</div>`;
    } else {
      const totCol = total >= 0 ? "#00d4aa" : "#ef5350";
      runTrades.innerHTML =
        `<table class="account-table"><thead><tr><th>Option</th><th>Qty</th><th>Entry</th><th>LTP</th><th>LTP chg</th><th>P&amp;L</th><th></th></tr></thead><tbody>${rows}</tbody></table>` +
        `<div style="font-size:9px;color:#888;padding:3px 2px">Total gross P&amp;L: <b style="color:${totCol}">${total >= 0 ? "+" : "-"}${fmtMoney(Math.abs(total))}</b> · updated ${istTime(Date.now())}</div>`;
      runTrades.querySelectorAll("[data-runclose]").forEach((b) => {
        b.onclick = async () => {
          if (!(await uiConfirm("Close this position at market?", { danger: true }))) return;
          await API.close({ id: b.getAttribute("data-runclose") });
          refresh();
        };
      });
    }
  }

  const body = document.querySelector("#rtPos tbody");
  if (body) {
    body.innerHTML = positions
      .map((p) => {
        const pnlCls = num(p.pnl) >= 0 ? "rt-pos" : "rt-neg";
        const entryForSl = num(p.fillPrice) > 0 ? num(p.fillPrice) : num(p.entry);
        const slCell = slCellHTML(p.side, entryForSl, p.sl, p.overallSl, p.trail, p.pointTrailLive != null ? p.pointTrailLive : p.pointTrail);
        const g = [];
        if (num(p.sl) > 0) g.push("SL");
        if (num(p.tp) > 0) g.push("TP");
        if (num(p.trail) > 0) g.push("TRAIL");
        const guardTxt = g.length ? `<span style="color:#ffd700">${g.join("+")}</span>` : "--";
        const ch = chgCell(p.ltpChange, p.ltpChangePct);
        return `<tr>
          <td>${esc(p.tradingSymbol || p.securityId)}${orderBadge(p.orderType, p.limitPrice)}</td>
          <td>${num(p.qty)}</td>
          <td>${num(p.entry).toFixed(2)}</td>
          <td>${num(p.ltp).toFixed(2)}</td>
          <td style="color:${ch.col};white-space:nowrap">${ch.txt}</td>
          <td class="${pnlCls}">${num(p.pnl).toFixed(2)}</td>
          <td>${targetLabCell(p.targetLab)}</td>
          <td>${slCell}</td>
          <td>${guardTxt}</td>
          <td style="white-space:nowrap">
            <button class="btn-action" data-openchart="${esc(String(p.securityId || ""))}" data-exch="${esc(p.exchangeSegment || "")}" data-inst="${esc(p.instrument || "")}" data-sym="${esc(p.tradingSymbol || "")}" style="width:auto;padding:2px 8px;margin:0 4px 0 0;font-size:10px" title="Is trade ka chart kholo (live P&amp;L / SL / trail lines)">Chart</button>
            <button class="btn-action warn" data-close="${esc(p.id)}" style="width:auto;padding:2px 8px;margin:0;font-size:10px">Close</button>
          </td>
        </tr>`;
      })
      .join("");
    body.querySelectorAll("[data-close]").forEach((b) => {
      b.onclick = async () => {
        if (!(await uiConfirm("Close this position at market?", { danger: true }))) return;
        await API.close({ id: b.getAttribute("data-close") });
        refresh();
      };
    });
  }

  wireOpenChartButtons();
}

// Attach the per-trade / per-strike "Open Chart" handlers. Called at the end of
// renderRunning once every running-trade / strike table has been rebuilt.
function wireOpenChartButtons() {
  document.querySelectorAll("[data-openchart]").forEach((b) => {
    b.onclick = () => openChartForTrade({
      securityId: b.getAttribute("data-openchart"),
      exchangeSegment: b.getAttribute("data-exch"),
      instrument: b.getAttribute("data-inst"),
      tradingSymbol: b.getAttribute("data-sym"),
    });
  });
}

function reasonLabel(reason) {
  const r = String(reason || "").toUpperCase();
  if (r === "STOP_LOSS") return "Overall SL";
  if (r === "TRAIL_SL") return "Trail SL";
  if (r === "TRAIL_SL_PCT") return "Trail SL (%)";
  if (r === "TRAIL_SL_PTS") return "Trail SL (points)";
  if (r === "TRAIL_SL_BOTH") return "Trail SL (% + points)";
  // Historical back-compat only: the Manual Trail TP exit was removed, but
  // trades already in the saved ledger can still carry this reason.
  if (r === "TRAIL_TP") return "Trail TP";
  if (r === "TARGET") return "Target";
  return reason;
}

// "N slices" badge for a sliced entry. `slices` is the per-slice ledger the
// engine records on the position/closed row (paper: qty+price fills; live:
// qty+orderId child orders). A single (or empty) entry is not badged.
function sliceBadge(slices) {
  const list = Array.isArray(slices) ? slices : [];
  if (list.length <= 1) return "";
  const parts = list
    .map((s) => {
      const q = num(s && s.qty);
      const p = s && s.price != null ? num(s.price) : null;
      const oid = s && s.orderId ? String(s.orderId) : "";
      const tail = p != null ? "@" + p.toFixed(2) : oid ? "#" + oid : "";
      return tail ? q + tail : String(q);
    })
    .join(", ");
  return ` <span title="Sliced entry: ${list.length} child ${list[0] && list[0].price != null ? "fills" : "orders"} — ${parts}" style="font-size:9px;color:#ffd700;border:1px solid #7a5c00;border-radius:3px;padding:0 3px;white-space:nowrap">${list.length} slices</span>`;
}

// "LIMIT" badge for an entry placed as an F&O limit order (the "F&O Limit"
// checkbox). The engine records orderType = "FNO_LIMIT" + the limit price on the
// position (and, since the close path now carries them over, on the closed row).
// A MARKET entry (orderType "MARKET") is not badged.
function orderBadge(orderType, limitPrice) {
  if (String(orderType || "").toUpperCase() !== "FNO_LIMIT") return "";
  const px = num(limitPrice);
  const tail = px > 0 ? "@" + px.toFixed(2) : "";
  return ` <span title="F&O limit entry${px > 0 ? " at " + px.toFixed(2) : ""}" style="font-size:9px;color:#ffb74d;border:1px solid #7a5200;border-radius:3px;padding:0 3px;white-space:nowrap">LIMIT${tail}</span>`;
}

// Applied stop-loss shown in BOTH representations the operator asked for: the
// premium-points distance from entry to the applied SL level, and the same as a
// percentage of entry (the % -> points conversion). The trail, when armed, is
// shown too with its converted counterpart. Shared by the Running Trades and
// Closed Trades tables so the two always agree.
function slCellHTML(side, entry, sl, overallSl, trail, pointTrail) {
  const e = num(entry);
  const isBuy = String(side == null ? "BUY" : side).toUpperCase() !== "SELL";
  const lvl = num(sl) > 0 ? num(sl) : num(overallSl);
  const parts = [];
  if (e > 0 && lvl > 0) {
    const pts = isBuy ? e - lvl : lvl - e;
    const pct = (pts / e) * 100;
    const sign = pts >= 0 ? "" : "-";
    parts.push(
      `SL ${lvl.toFixed(2)} <span style="color:#ff9800">(${sign}${Math.abs(pts).toFixed(2)} pts / ${pct.toFixed(2)}%)</span>`
    );
  }
  const tp = num(trail);
  const ppt = num(pointTrail);
  if (tp > 0 || ppt > 0) {
    const t = [];
    if (tp > 0) t.push(tp.toFixed(2) + "%");
    if (ppt > 0) t.push(ppt.toFixed(2) + " pts");
    if (tp > 0 && ppt <= 0 && e > 0) t.push(((e * tp) / 100).toFixed(2) + " pts");
    if (ppt > 0 && tp <= 0 && e > 0) t.push(((ppt / e) * 100).toFixed(2) + "%");
    parts.push(`<span style="color:#b39ddb">Trail ${t.join(" / ")}</span>`);
  }
  return parts.length ? parts.join("<br>") : "-";
}

function targetLabCell(tl) {
  if (!tl || typeof tl !== "object") return "--";
  const st = String(tl.status || "");
  const col =
    st === "ACTIVE" || st === "READY" ? "#00d4aa" :
    st === "TOO_CLOSE" || st === "LOW_PROFIT" || st === "NO_MATCH" || st === "BELOW_CONF" ? "#ffb74d" :
    st === "PASSED" || st === "EXPIRED" ? "#888" : "#666";
  const bits = [`<span style="color:${col};font-weight:700">${esc(st || "--")}</span>`];
  const dir = String(tl.directionText || "");
  if (dir) bits.push(`<span style="color:#90caf9">${esc(dir)}</span>`);
  const t = num(tl.target);
  if (t > 0) bits.push("T " + t.toFixed(2));
  const d = num(tl.distance);
  const dp = num(tl.distancePct);
  if (d !== 0 || dp !== 0) {
    bits.push(`<span style="color:${d >= 0 ? "#00d4aa" : "#ef5350"};white-space:nowrap">${d >= 0 ? "+" : ""}${d.toFixed(2)} (${dp.toFixed(2)}%)</span>`);
  }
  return bits.join("<br>");
}

function closedRowHTML(c, on) {
  const charges = on ? (c.charges != null ? num(c.charges) : estimateCharges(c.entry, c.exit, c.qty, c.side, c.instrument, c.tradingSymbol)) : 0;
  const net = on && c.netPnl != null ? num(c.netPnl) : num(c.pnl);
  const cls = net >= 0 ? "rt-pos" : "rt-neg";
  const fmtT = (t) => (t ? istTime(num(t)) : "-");
  const ch = chgCell(c.ltpChange, c.ltpChangePct);
  const slCell = slCellHTML(c.side, c.entry, c.sl, c.overallSl, c.trail, c.pointTrailLive != null ? c.pointTrailLive : c.pointTrail);
  return `<tr>
        <td>${esc(c.tradingSymbol || c.securityId)}${sliceBadge(c.slices)}${orderBadge(c.orderType, c.limitPrice)}</td>
        <td>${num(c.qty)}</td>
        <td>${num(c.entry).toFixed(2)} → ${num(c.exit).toFixed(2)}</td>
        <td>${slCell}</td>
        <td style="color:${ch.col};white-space:nowrap">${ch.txt}</td>
        <td class="${cls}">${net.toFixed(2)}</td>
        <td>${on ? charges.toFixed(2) : "--"}</td>
        <td>${esc(reasonLabel(c.reason))}</td>
        <td>${targetLabCell(c.targetLab)}</td>
        <td>${esc(fmtT(c.openedAt))} → ${esc(fmtT(c.closedAt))}</td>
      </tr>`;
}

function renderClosed(s) {
  const body = document.querySelector("#rtClosed tbody");
  if (!body) return;
  // Prefer the full on-demand ledger; fall back to the snapshot's newest slice
  // before the first `/closed` fetch resolves.
  const fromCache = !!(CLOSED_CACHE && CLOSED_CACHE.length);
  const list = fromCache ? CLOSED_CACHE : ((s && s.closed) || []);
  const source = fromCache ? "c" : "s";
  const on = chargesOn();
  const cnt = document.getElementById("rtClosedCount");
  if (cnt) cnt.textContent = "(" + list.length + ")";
  const idOf = (c) => String((c && c.id) || "");
  // Newest-first + append-only ledger: a pure prepend can insert just the new
  // rows. Anything else (source switch, charges toggle, reset, re-sort, tail that
  // no longer matches) falls back to a full repaint.
  const canAppend =
    closedRenderedCount > 0 &&
    list.length > closedRenderedCount &&
    source === closedRenderedSource &&
    on === closedRenderedCharges &&
    closedTailId !== "" &&
    idOf(list[closedRenderedCount - 1]) === closedTailId;
  if (canAppend) {
    const fresh = list.slice(0, list.length - closedRenderedCount);
    body.insertAdjacentHTML("afterbegin", fresh.map((c) => closedRowHTML(c, on)).join(""));
    closedRenderedCount = list.length;
    closedTailId = idOf(list[list.length - 1]);
    return;
  }
  body.innerHTML = list.map((c) => closedRowHTML(c, on)).join("");
  closedRenderedCount = list.length;
  closedRenderedSource = source;
  closedRenderedCharges = on;
  closedTailId = list.length ? idOf(list[list.length - 1]) : "";
}

// The closed ledger and day-stats are cached client-side (the 1s poll only ships
// the newest slice). After a P&L reset the server book is empty, so drop the
// caches and force a repaint - otherwise the old rows keep showing.
function resetBookCaches() {
  CLOSED_CACHE = null;
  closedLoadedAt = 0;
  closedRenderedCount = 0;
  closedRenderedSource = "";
  closedRenderedCharges = null;
  closedTailId = "";
  DAY_STATS = null;
  dayStatsAt = 0;
  dayStatsSig = "";
}

// "Deduct Dhan charges" toggle state. Real trades settle charges broker-side
// (net == gross), so only the paper engine honours the toggle; default on so a
// saved setting is only ever disabled explicitly.
function chargesOn() {
  if (!PAPER) return false;
  const s = STATE && STATE.settings;
  return !s || s.brokerCharges !== false;
}

// Dhan-style broker charge simulation, ported 1:1 from the Python paper-trade
// engine (`static/papertrade.js`). Rates are Dhan's published retail tariffs:
//   Delivery: brokerage Rs 0, STT 0.1% (buy+sell), NSE txn 0.0030699%,
//             SEBI 0.0001%, stamp 0.015% (buy), IPFT 0.0000001%, GST 18%
//   Intraday: brokerage Rs 20 or 0.03% (lower of the two), STT 0.025% (sell),
//             NSE txn 0.0030699%, SEBI 0.0001%, stamp 0.003% (buy),
//             IPFT 0.0000001%, GST 18%
//   Options:  brokerage Rs 20 / executed order, STT 0.0625% of premium (sell),
//             NSE txn 0.03503% of premium, SEBI 0.0001%, stamp 0.003% (buy),
//             IPFT 0.0000001%, GST 18%
//   Futures:  brokerage Rs 20 / executed order, STT 0.02% of turnover (sell),
//             NSE txn 0.00173% of turnover, SEBI 0.0001%, stamp 0.003% (buy),
//             IPFT 0.0000001%, GST 18%
// Rounding follows the Dhan contract-note rule: STT + stamp duty to the nearest
// rupee, every other component to 2 dp. Mirrors the Rust engine's
// `compute_charges_for_trade()` so the Closed Trades table, paper wallet and
// Paper Stats all settle to the same figure.
const CHARGES_CONFIG = {
  delivery: { brokerageFlat: 0, brokeragePct: 0, txnPct: 0.0030699, sttBuyPct: 0.1, sttSellPct: 0.1, sebiPct: 0.0001, stampBuyPct: 0.015, stampSellPct: 0, gstPct: 18, ipftPct: 0.0000001 },
  intraday: { brokerageFlat: 20, brokeragePct: 0.03, txnPct: 0.0030699, sttBuyPct: 0, sttSellPct: 0.025, sebiPct: 0.0001, stampBuyPct: 0.003, stampSellPct: 0, gstPct: 18, ipftPct: 0.0000001 },
  options: { brokerageFlat: 20, brokeragePct: 0, txnPct: 0.03503, sttBuyPct: 0, sttSellPct: 0.0625, sebiPct: 0.0001, stampBuyPct: 0.003, stampSellPct: 0, gstPct: 18, ipftPct: 0.0000001 },
  futures: { brokerageFlat: 20, brokeragePct: 0, txnPct: 0.00173, sttBuyPct: 0, sttSellPct: 0.02, sebiPct: 0.0001, stampBuyPct: 0.003, stampSellPct: 0, gstPct: 18, ipftPct: 0.0000001 },
};

// Charge segment for a position / symbol: OPT* -> options, FUT* -> futures,
// otherwise delivery (equity / index spot). Mirrors Python `segmentFor()`.
function chargesSegment(instrument, tradingSymbol) {
  const inst = String(instrument == null ? "" : instrument).toUpperCase();
  if (inst === "OPTIDX" || inst === "OPTSTK" || inst === "OPTFUT" || inst === "OPT") return "options";
  if (inst === "FUTIDX" || inst === "FUTSTK" || inst === "FUTCOM" || inst === "FUT") return "futures";
  const nm = String(tradingSymbol == null ? "" : tradingSymbol).toUpperCase();
  if (/(?:^|[^A-Z0-9])(?:CE|PE)(?:$|[^A-Z0-9])/.test(nm)) return "options";
  return "delivery";
}

// Per-side charge line for one order (turnover = qty x price). STT + stamp
// round to the nearest rupee; all other charges to 2 decimals (Dhan rule).
function sideCharges(segment, isBuy, turnover) {
  const c = CHARGES_CONFIG[segment] || CHARGES_CONFIG.delivery;
  const t = Math.abs(num(turnover));
  const r2 = (n) => Math.round(n * 100) / 100;
  let brokerage = c.brokerageFlat;
  if (c.brokeragePct > 0) {
    const pct = (t * c.brokeragePct) / 100;
    brokerage = c.brokerageFlat > 0 ? Math.min(c.brokerageFlat, pct) : pct;
  }
  const txn = (t * c.txnPct) / 100;
  const stt = Math.round(((isBuy ? c.sttBuyPct : c.sttSellPct) * t) / 100);
  const sebi = (t * c.sebiPct) / 100;
  const stamp = Math.round(((isBuy ? c.stampBuyPct : c.stampSellPct) * t) / 100);
  const ipft = (t * c.ipftPct) / 100;
  const gst = ((brokerage + txn + sebi + ipft) * c.gstPct) / 100;
  return { brokerage: r2(brokerage), txn: r2(txn), stt: stt, sebi: r2(sebi), stamp: stamp, ipft: ipft, gst: r2(gst), total: r2(brokerage + txn + stt + sebi + stamp + ipft + gst) };
}

// Full round-trip charge estimate for a position closed at `exit`. Returns the
// round-trip total; entry leg uses `side`, exit leg the opposite side so STT
// lands on the sell leg and stamp on the buy leg.
function estimateCharges(entry, exit, qty, side, instrument, tradingSymbol) {
  const q = Math.abs(num(qty));
  const e = Math.abs(num(entry));
  const x = Math.abs(num(exit));
  if (!q || !e || !x) return 0;
  const segment = chargesSegment(instrument, tradingSymbol);
  const isLong = String(side == null ? "BUY" : side).toUpperCase() !== "SELL";
  const entryC = sideCharges(segment, isLong, e * q);
  const exitC = sideCharges(segment, !isLong, x * q);
  return Math.round((entryC.total + exitC.total) * 100) / 100;
}

function renderHoldings(s) {
  const body = document.querySelector("#rtHoldings tbody");
  if (!body) return;
  const rows = s.holdings || [];
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="5" style="color:#666;padding:6px">Dhan demat me koi holding nahi hai.</td></tr>`;
    return;
  }
  body.innerHTML = rows
    .map((h) => {
      const ltp = num(h.ltp);
      const avg = num(h.avgCostPrice);
      const qty = num(h.totalQty);
      const hasLtp = ltp > 0;
      const pnl = hasLtp ? (ltp - avg) * qty : 0;
      const cls = pnl >= 0 ? "rt-pos" : "rt-neg";
      return `<tr>
        <td><b>${esc(h.tradingSymbol || h.securityId)}</b><br><span style="font-size:8px;color:#666">${esc(h.exchange || "")}</span></td>
        <td>${qty}</td>
        <td>${avg.toFixed(2)}</td>
        <td>${hasLtp ? ltp.toFixed(2) : "--"}</td>
        <td class="${hasLtp ? cls : ""}">${hasLtp ? (pnl >= 0 ? "+" : "-") + fmtMoney(Math.abs(pnl)) : "--"}</td>
      </tr>`;
    })
    .join("");
}

// ---------------------------------------------------------------------------
// Old-app AST entry timing diagnostics.
// ---------------------------------------------------------------------------
function stratBull(s) {
  return !/bear/i.test(String((s && s.category) || ""));
}

async function loadAstSections() {
  if (astBusy) return;
  astBusy = true;
  try {
    const et = await API.entryTiming();
    AST.entryTiming = (et && et.rows) || [];
    renderEntryTiming();
  } catch (e) {
    // non-fatal: the core engine UI keeps working
  } finally {
    astBusy = false;
  }
}


function renderEntryTiming() {
  const body = document.getElementById("rtEtBody");
  const cnt = document.getElementById("rtEtCount");
  if (cnt) cnt.textContent = AST.entryTiming.length + " event(s)";
  if (!body) return;
  const rows = AST.entryTiming.slice(-60).reverse();
  body.innerHTML = rows.length
    ? rows
        .map((r) => {
          const held = r.metAt ? fmtLogTime(r.metAt) : "-";
          const placed = r.entryAt ? fmtLogTime(r.entryAt) : "-";
          const delayS = num(r.delayMs) / 1000;
          const win = num(r.ordersWindow);
          return `<div style="display:flex;gap:8px;padding:1px 2px;border-bottom:1px solid #16163a">
            <b style="color:#b39ddb">${esc(r.strategyName || r.strategyId || "")}</b>
            <span style="color:#888">held ${esc(held)} · placed ${esc(placed)}</span>
            <span style="color:#666">orders ${win}</span>
            <span style="margin-left:auto" class="${delayS > 0 ? "rt-neg" : "rt-pos"}">delay ${delayS.toFixed(1)}s</span>
          </div>`;
        })
        .join("")
    : `<div style="color:#666">No alignment episodes recorded yet.</div>`;
}


// ---------------------------------------------------------------------------
// "Why no trades?" live blockers panel. Reads the `blockers` block that the
// server attaches to every snapshot, so the panel updates at the same 1s cadence
// as the rest of the engine and needs no extra polling in the common case.
// ---------------------------------------------------------------------------
let blockerRenderSig = "";
// Latest process-wide diagnostics block for THIS tab. Kept outside STATE so a
// fresh snapshot (which overwrites STATE wholesale) cannot wipe the heartbeat
// readout between polls.
let DIAG = null;

async function loadBlockers() {
  try {
    const r = await API.blockers();
    renderBlockers(r);
  } catch (e) {
    /* keep the last snapshot view */
  }
  loadDiag();
}

// Pull the process-wide diagnostics snapshot and keep THIS tab's own engine
// block on STATE.diag. The heartbeat numbers (loop / scan / last entry) live
// there because the blocker list can only report gates that actually ran - a
// dead scan or a stalled loop produces no blocker at all.
let diagBusy = false;
let diagLastMs = 0;
// Heartbeats only need a couple of seconds of resolution, so the 1s engine poll
// piggybacks on this cheap throttle instead of adding a second request every tick.
function loadDiagThrottled() {
  const now = Date.now();
  if (now - diagLastMs < 2000) return;
  diagLastMs = now;
  loadDiag();
}
async function loadDiag() {
  if (diagBusy) return;
  diagBusy = true;
  try {
    const r = await API.diagSnapshot();
    if (!r || !Array.isArray(r.engines)) return;
    const want = realtimeEngineId();
    const block = r.engines.find((e) => e && e.engine === want) || null;
    DIAG = block;
    if (STATE) STATE.diag = block;
    if (block) {
      // Repaint the panel with the fresh heartbeat even when the 1s snapshot has
      // not landed yet (STATE still empty) or it carried no blocker rows.
      const src = STATE && STATE.blockers ? Object.assign({}, STATE.blockers) : { blockers: [] };
      if (src.engineOn == null) src.engineOn = block.engineOn;
      if (src.armed == null) src.armed = !!block.armed;
      if (src.connected == null) src.connected = !!(block.runtime && block.runtime.connected);
      if (src.feedLive == null) src.feedLive = !!(block.runtime && block.runtime.feed && block.runtime.feed.feedLive);
      const m = block.margin || {};
      if (src.marginFree == null) src.marginFree = num(m.free);
      if (src.marginBudget == null) src.marginBudget = num(m.cap);
      if (src.openPositions == null) src.openPositions = num(m.openPositions);
      renderBlockers(src);
    }
  } catch (e) {
    /* diagnostics are optional - never break the engine pane */
  } finally {
    diagBusy = false;
  }
}

function blockersPayload(src) {
  // Accepts:
  //  - /api/rt/blockers  -> { ok, at, blockers: [...], engineOn, marginBudget, ... }
  //  - the engine snapshot -> { engineOn, armed, margin: {...}, blockers: { ok, blockers: [...] } }
  //  - /api/diagnostics/snapshot engine block -> { engineOn, margin: {cap,free,...}, blockers: {...}, runtime: {...} }
  // Every path lands on one flat shape the renderer can trust.
  if (!src || typeof src !== "object") return null;
  let rows = null;
  let engineOn = src.engineOn;
  let armed = src.armed;
  let margin = src.margin || null;
  let connected = src.connected;
  let feedLive = src.feedLive;
  if (Array.isArray(src.blockers)) {
    // Raw blockers response, or the nested `blockers` object passed directly.
    rows = src.blockers;
  } else if (src.blockers && Array.isArray(src.blockers.blockers)) {
    rows = src.blockers.blockers;
  } else {
    return null;
  }
  if (rows === null) return null;

  // The raw blockers endpoint carries no engine/feed/margin fields; borrow the
  // last snapshot's so the headline does not read "engine OFF" just because that
  // particular response had no engine fields.
  const st = STATE && typeof STATE === "object" ? STATE : null;
  if (st) {
    if (engineOn == null) engineOn = st.engineOn;
    if (armed == null) armed = st.armed;
    if (!margin) margin = st.margin;
    if (connected == null) connected = st.connected;
    if (feedLive == null) feedLive = st.feedLive;
  }
  const rt = src.runtime || {};
  if (rt.connected != null && connected == null) connected = !!rt.connected;
  if (rt.feed && rt.feed.feedLive != null && feedLive == null) feedLive = !!rt.feed.feedLive;

  // Two margin shapes reach here: the engine snapshot uses
  // available/locked/budget/count, /api/diagnostics/snapshot uses
  // cap/free/locked/openPositions.
  const m = margin || {};
  const free = num(m.available != null ? m.available : m.free != null ? m.free : src.marginFree);
  const locked = num(m.locked != null ? m.locked : src.marginLocked);
  const budget = num(m.budget != null ? m.budget : m.cap != null ? m.cap : src.marginBudget);
  const open = num(m.count != null ? m.count : m.openPositions != null ? m.openPositions : src.openPositions);
  const capIsZero = m.capIsZero != null ? !!m.capIsZero : budget <= 0;

  const eng = engineOn != null ? !!engineOn : false;
  const isArmed = !!armed;
  let hl = src.headline;
  if (!hl) {
    if (!eng) hl = "Engine is OFF - turn on AI Smart Trading";
    else if (!isArmed) hl = "Engine is ON but disarmed for today (auto square-off already ran)";
    else if (capIsZero) hl = "No shared margin allotted to this tab - set an allotment";
    else if (free <= 0)
      hl = "Margin fully locked by " + open + " open position(s) (Rs " +
        Math.round(locked) + " of Rs " + Math.round(budget) + ") - square off or raise the allotment";
    else if (rows.length && rows[0].level === "warn") hl = "Blocked: " + (rows[0].detail || rows[0].code);
    else hl = "Engine is armed and evaluating - see the blocking reasons below";
  }
  return {
    ok: true,
    blockers: rows,
    headline: hl,
    engineOn: eng,
    armed: isArmed,
    connected,
    feedLive,
    marginFree: free,
    marginBudget: budget,
    marginLocked: locked,
    openPositions: open,
  };
}

function renderBlockers(src) {
  const b = blockersPayload(src) || blockersPayload(STATE);
  if (!b) return;
  const body = document.getElementById("rtBlockerBody");
  const head = document.getElementById("rtBlockerHeadline");
  const cnt = document.getElementById("rtBlockerCount");
  if (!body) return;
  const rows = Array.isArray(b.blockers) ? b.blockers : [];
  if (cnt) cnt.textContent = rows.length + " reason(s)";
  if (head) {
    // The dedicated `/api/rt/blockers` response only carries the rows, so the
    // connection/feed/margin/heartbeat bits come from the diagnostics snapshot
    // when it is available, falling back to whatever the snapshot already put
    // on the payload.
    const diag = DIAG || (STATE && STATE.diag) || null;
    const drt = (diag && diag.runtime) || {};
    const stateBits = [];
    if (b.connected === false) stateBits.push("Dhan: disconnected");
    else if (b.connected === true) stateBits.push("Dhan: connected");
    if (b.feedLive === false) stateBits.push("feed: no ticks");
    stateBits.push("engine " + (b.engineOn ? (b.armed ? "ON+armed" : "ON (disarmed)") : "OFF"));
    stateBits.push("margin Rs " + Math.round(num(b.marginFree)) + " free of " + Math.round(num(b.marginBudget)));
    stateBits.push(num(b.openPositions) + " open");
    if (num(b.marginBudget) <= 0) stateBits.push("NO ALLOTMENT");
    // Heartbeats: a dead scan records NO blocker, so without these two numbers
    // an empty panel would look like "nothing is wrong" instead of "the engine
    // never ran". Shows seconds since the last loop tick and the last scan.
    if (diag && diag.runtime) {
      const fmtAge = (v) => (v == null ? "never" : (Number(v) < 90 ? Math.round(Number(v)) + "s" : Math.round(Number(v) / 60) + "m") + " ago");
      stateBits.push("loop " + fmtAge(drt.loopAgeSec));
      stateBits.push("scan " + fmtAge(drt.scanAgeSec));
      stateBits.push("scanner " + fmtAge(drt.scannerAgeSec));
      stateBits.push("last entry " + fmtAge(drt.lastEntryAgeSec));
      if (drt.loopAgeSec != null && Number(drt.loopAgeSec) > 120)
        stateBits.push("LOOP STALLED");
      if (b.connected && drt.scanAgeSec != null && Number(drt.scanAgeSec) > 180)
        stateBits.push("SCAN STALLED");
      // The scanner pass is separate from the evaluation scan: when it is stale
      // the engine still evaluates, but its Top Movers / Liquid F&O picks are
      // frozen (typically a Dhan 429 hold). Surfacing it prevents "engine looks
      // alive yet nothing new is picked" from being a mystery.
      if (b.connected && drt.scannerAgeSec != null && Number(drt.scannerAgeSec) > 20)
        stateBits.push("SCANNER STALE");
    }
    head.innerHTML =
      `<b style="color:#ffd54f">${esc(b.headline || "No blocking reason recorded yet")}</b>` +
      `<span style="color:#777;margin-left:8px">${esc(stateBits.join(" · "))}</span>`;
  }
  // Re-render when the set/count changes, and (coarsely) as ages advance, so a
  // stale row's "Xm ago" still ticks over without repainting every second.
  const sig = rows.length + "|" + rows.map((r) => r.code + ":" + r.count + ":" + Math.floor(num(r.ageMs) / 15000)).join(",");
  if (sig === blockerRenderSig) return;
  blockerRenderSig = sig;
  const codeLabel = (code) => {
    switch (String(code || "")) {
      case "margin": return "Margin";
      case "latch": return "Repeat-entry latch";
      case "gate": return "Indicator gate";
      case "no-premium": return "No option premium";
      case "candles": return "No candles";
      case "orders-per-sec": return "Orders/sec cap";
      case "cooldown": return "Re-entry cooldown";
      case "duplicate": return "Duplicate strike";
      case "trend": return "Trend confirmation";
      case "trade-limit": return "Max trades reached";
      case "session": return "Outside trade window";
      case "fno-expiry": return "F&O expiry guard";
      case "spot-candle": return "Spot candle colour";
      case "mtf": return "Higher-TF confirm";
      case "already-running": return "Position already open";
      case "feed": return "Feed / connection";
      case "market-closed": return "Market closed";
      case "no-instruments": return "No instruments";
      case "research": return "Research / target";
      case "quantity": return "Quantity";
      case "engine-state": return "Engine state";
      case "scanner-stale": return "Scanner stale";
      case "order-error": return "Order rejected / error";
      default: return code ? String(code) : "Other";
    }
  };
  body.innerHTML = rows.length
    ? rows
        .map((r) => {
          const age = Math.round(num(r.ageMs) / 1000);
          const ageTxt = age < 60 ? age + "s ago" : Math.round(age / 60) + "m ago";
          const col = r.level === "warn" ? "#ffb300" : "#66ccff";
          const fresh = r.fresh ? "" : "opacity:.55;";
          const samples = Array.isArray(r.samples) && r.samples.length
            ? `<span style="color:#666">${esc(r.samples.join(", "))}</span>`
            : "";
          return `<div style="display:flex;gap:6px;padding:2px 0;border-bottom:1px solid #16163a;${fresh}">
            <b style="color:${col};flex:0 0 118px">${esc(codeLabel(r.code))}</b>
            <span style="color:#888;flex:0 0 62px;text-align:right">${esc(r.count)}x</span>
            <span style="color:#555;flex:0 0 56px;text-align:right">${esc(ageTxt)}</span>
            <span style="color:#ccc;white-space:normal;flex:1">${esc(r.detail || "")} ${samples}</span>
          </div>`;
        })
        .join("")
    : `<div style="color:#666">No blocking reason recorded in the last 30 minutes - either the engine is placing trades, or no signal has reached a gate yet.</div>`;
}

// ---------------------------------------------------------------------------
// Condition Log: engine skip/condition events (no option contracts, no
// tradeable instruments, order errors). Reads `logs` from the snapshot, which
// the server already returns newest-first.
// ---------------------------------------------------------------------------
function fmtLogTime(ms) {
  return istTime(num(ms));
}
async function refreshAccount() {
  const btn = el("rtRefreshAccount");
  if (btn) { btn.disabled = true; btn.textContent = "Refreshing..."; }
  try {
    const a = await API.account();
    if (STATE) {
      if (a.funds) STATE.funds = a.funds;
      if (Array.isArray(a.positions)) STATE.brokerPositions = a.positions;
      if (Array.isArray(a.holdings)) STATE.holdings = a.holdings;
      renderRunning(Object.assign({}, STATE));
      renderHoldings({ holdings: STATE.holdings || [] });
      updateBalanceReadouts();
    }
  } catch (e) {
    /* broker offline - snapshot poll will recover */
  }
  if (btn) { btn.disabled = false; btn.textContent = "Refresh Account"; }
}

async function refreshLogs() {
  try {
    const r = await API.logs();
    if (STATE && Array.isArray(r.logs)) STATE.logs = r.logs;
    renderConditionLog();
  } catch (e) {
    /* keep the last snapshot log */
  }
}

function renderConditionLog() {
  const body = document.getElementById("rtCondLogBody");
  if (!body) return;
  const lvlEl = document.getElementById("rtCondLogLevel");
  const pauseEl = document.getElementById("rtCondLogPause");
  if (pauseEl && pauseEl.checked) return;
  const lvl = (lvlEl && lvlEl.value) || "all";
  let logs = (STATE && STATE.logs) || [];
  if (!Array.isArray(logs)) logs = [];
  if (lvl === "warn") logs = logs.filter((l) => l.level === "warn" || l.level === "error");
  else if (lvl === "error") logs = logs.filter((l) => l.level === "error");
  const cnt = document.getElementById("rtCondLogCount");
  if (cnt) cnt.textContent = logs.length + " line(s)";
  // Skip the DOM rebuild while the visible lines are unchanged (same count and
  // same newest line): the poll runs every second but logs only move on events.
  const sig = logs.length + ":" + (logs.length ? num(logs[0].t) : 0) + ":" + lvl;
  if (sig === logRenderSig) return;
  logRenderSig = sig;
  const color = (l) => (l === "error" ? "#ef5350" : l === "warn" ? "#ffb300" : "#66ccff");
  body.innerHTML = logs.length
    ? logs
        .map(
          (l) => `<div style="display:flex;gap:6px;padding:1px 0;border-bottom:1px solid #16163a">
            <span style="color:#555;flex:0 0 52px">${esc(fmtLogTime(l.t))}</span>
            <b style="color:${color(l.level)};text-transform:uppercase;flex:0 0 40px">${esc(l.level || "info")}</b>
            <span style="color:#ccc;white-space:normal">${esc(l.msg || "")}</span>
          </div>`
        )
        .join("")
    : `<div style="color:#666">No condition events yet.</div>`;
}

// ---------------------------------------------------------------------------
// Live Data Pool
// ---------------------------------------------------------------------------
function applyDataPoolUi(on) {
  const body = document.getElementById("rtDataPoolBody");
  const info = document.getElementById("rtDataPoolInfo");
  const cb = document.getElementById("rtDataPool");
  if (cb && cb.checked !== !!on) cb.checked = !!on;
  if (body) body.style.display = on ? "block" : "none";
  if (info) info.textContent = on ? "ON - live candle + indicator + filter readout" : "OFF - shared pool feeds strategies only";
  if (on) loadPool();
}

async function loadPool(force) {
  const body = document.getElementById("rtDataPoolBody");
  const on = document.getElementById("rtDataPool");
  if (!body || !on || !on.checked) return;
  try {
    const p = await API.pool(force);
    renderPool(p);
  } catch (e) {
    body.textContent = "pool unavailable";
  }
}

function renderPool(p) {
  const body = document.getElementById("rtDataPoolBody");
  if (!body) return;
  const rows = (p && p.rows) || [];
  const scanner = (p && p.scanner) || [];
  const info = document.getElementById("rtDataPoolInfo");
  if (info) info.textContent = "ON - tick-native live candle + indicator + filter readout";
  const scannerHtml = scanner.length
    ? `<div style="border-top:1px solid #2d2d50;margin-top:3px;padding:3px 6px;color:#66ccff;font-weight:700;font-size:9px">Scanner instruments (${scanner.length})</div>` +
      scanner
        .map((r) => {
          const c = r.candle || null;
          const candle = c
            ? `O ${num(c.open).toFixed(2)} H ${num(c.high).toFixed(2)} L ${num(c.low).toFixed(2)} C <b style="color:#fff">${num(c.close).toFixed(2)}</b>`
            : "no candle";
          return `<div style="border-bottom:1px solid #14142e;padding:3px 6px;color:#cfcfe6">
            <b>${esc(r.tradingSymbol || r.securityId)}</b> <span style="color:${String(r.side).toUpperCase() === "PE" ? "#ef5350" : "#00d4aa"}">${esc(r.side || "")}</span>
            <span style="color:#888"> · ${esc(r.source || "")} · ${esc(r.underlying || "")} spot ${num(r.spot).toFixed(2)} (${num(r.changePct).toFixed(2)}%)</span>
            <div style="color:#9a9ac0">${candle}</div></div>`;
        })
        .join("")
    : "";
  if (!rows.length && !scanner.length) {
    body.innerHTML = `<div style="padding:6px;color:#666">No strategies or scanner instruments to read. Add a strategy below or enable a scanner.</div>`;
    return;
  }
  const stratHtml = rows
    .map((r) => {
      const rd = r.readout;
      const head = `<div style="padding:3px 6px;color:${r.enabled ? "#cfcfe6" : "#666"};border-bottom:1px solid #1c1c38">
        <b>${esc(r.name)}</b> · ${esc(r.tradingSymbol || r.securityId)} · ${esc(r.timeframe)} · ${esc(r.side)} ${r.enabled ? "" : "(disabled)"}</div>`;
      if (!rd) return `<div style="border-bottom:1px solid #14142e">${head}<div style="padding:4px 6px;color:#666">no conditions</div></div>`;
      if (rd.error) return `<div style="border-bottom:1px solid #14142e">${head}<div style="padding:4px 6px;color:#ff8888">${esc(rd.error)}</div></div>`;
      const c = rd.candle || {};
      const candle = `<div style="padding:3px 6px;color:#9a9ac0">O ${num(c.open).toFixed(2)} H ${num(c.high).toFixed(2)} L ${num(c.low).toFixed(2)} C <b style="color:#fff">${num(c.close).toFixed(2)}</b> V ${num(c.volume).toFixed(0)}</div>`;
      const conds = (rd.conditions || [])
        .map(
          (x) => `<div style="padding:1px 6px;display:flex;gap:8px;color:${x.pass ? "#7CFFB2" : "#ff8888"}">
            <span style="min-width:120px">${esc(x.indicator)}${x.source ? "[" + esc(x.source) + "]" : ""}</span>
            <span>${esc(x.op)} ${esc(x.target)}</span>
            <span>val=${x.value == null ? "--" : num(x.value).toFixed(2)}</span>
            <b>${x.pass ? "PASS" : "FAIL"}</b></div>`
        )
        .join("");
      const verdict = `<div style="padding:2px 6px;font-weight:700;color:${rd.pass ? "#7CFFB2" : "#ff8888"}">${rd.pass ? "ALL PASS - entry ready" : "WAIT"}</div>`;
      return `<div style="border-bottom:1px solid #14142e">${head}${candle}${conds}${verdict}</div>`;
    })
    .join("");
  body.innerHTML = stratHtml + scannerHtml;
}

// ---------------------------------------------------------------------------
// Templates (server-backed AST engine templates, assignable to directions)
// ---------------------------------------------------------------------------
let TPLS = {};

async function fetchTemplates() {
  try {
    const r = await API.templates();
    TPLS = (r && r.templates) || {};
  } catch (e) {
    TPLS = {};
  }
  return TPLS;
}
function templateNames(side) {
  return Object.keys(TPLS)
    .filter((n) => !side || !TPLS[n].side || TPLS[n].side === side)
    .sort();
}
function tplLabel(name) {
  const t = TPLS[name] || {};
  const bits = [];
  if (t.side) bits.push(t.side);
  if (t.origin) bits.push("from " + t.origin);
  return bits.length ? name + " (" + bits.join(" \u00b7 ") + ")" : name;
}
async function refreshTemplateList() {
  await fetchTemplates();
  const fill = (id, side) => {
    const sel = document.getElementById(id);
    if (!sel) return;
    const cur = sel.value;
    sel.innerHTML = `<option value="">-- none --</option>` +
      templateNames(side).map((n) => `<option value="${esc(n)}">${esc(tplLabel(n))}</option>`).join("");
    if (cur) sel.value = cur;
  };
  fill("rtTplOpen");
  renderTemplateAssignment();
  renderQuickRun();
  if (STATE && STATE.settings) applySettingsToDom(STATE.settings);
}

// ---------------------------------------------------------------------------
// "Assign AST Template to direction" (old-app parity): saved engine-settings
// templates bound to a direction. The assignment drives the next run start of
// that side; no assignment = manual engine settings run as usual.
// ---------------------------------------------------------------------------
function tplOptions(side, selected) {
  const names = templateNames(side);
  if (!names.length) return `<option value="">-- no saved ${side} template --</option>`;
  return (
    `<option value="">-- select ${side} template --</option>` +
    names.map((n) => `<option value="${esc(n)}"${n === selected ? " selected" : ""}>${esc(tplLabel(n))}</option>`).join("")
  );
}
function assignedTplChip(containerId, key, side, name) {
  const box = document.getElementById(containerId);
  if (!box) return;
  if (!name) {
    box.innerHTML = `<span style="color:#666">none assigned - manual engine settings run as usual</span>`;
    return;
  }
  const t = TPLS[name] || {};
  box.innerHTML =
    `<span style="display:inline-flex;align-items:center;gap:4px;background:#1a1a35;border:1px solid #2d2d50;color:#ffd700;border-radius:3px;padding:2px 6px;font-size:9px;margin:1px">` +
    `${esc(name)} (${esc(t.side || side)}${t.origin ? " \u00b7 from " + esc(t.origin) : ""})` +
    ` <button data-tpl-clear="${esc(key)}" title="Remove from ${esc(side)} assignments" style="background:none;border:none;color:#ef5350;font-weight:700;cursor:pointer;font-size:11px;padding:0 2px;line-height:1">\u00d7</button></span>`;
  box.querySelectorAll("[data-tpl-clear]").forEach((b) => {
    b.onclick = () => pushSetting({ [b.getAttribute("data-tpl-clear")]: "" });
  });
}
function setDirSelect(id, side, val) {
  const el = document.getElementById(id);
  if (!el) return;
  const sig = String(val || "") + "|" + templateNames(side).join(",");
  if (el.dataset.tplSig === sig) return;
  el.dataset.tplSig = sig;
  el.innerHTML = tplOptions(side, val || "");
}
function renderTemplateAssignment() {
  const s = (STATE && STATE.settings) || {};
  setDirSelect("rtTplMoverBull", "bullish", s.moverBullTemplate);
  setDirSelect("rtTplMoverBear", "bearish", s.moverBearTemplate);
  assignedTplChip("rtMoverBullTplList", "moverBullTemplate", "bullish", s.moverBullTemplate);
  assignedTplChip("rtMoverBearTplList", "moverBearTemplate", "bearish", s.moverBearTemplate);
}
function wireAssign(selId, btnId, key, side) {
  const btn = document.getElementById(btnId);
  if (!btn) return;
  btn.onclick = () => {
    const sel = document.getElementById(selId);
    const v = sel ? sel.value : "";
    if (!v) {
      uiAlert("Pick a saved " + side + " template to assign first");
      return;
    }
    pushSetting({ [key]: v });
  };
}

// ---------------------------------------------------------------------------
// "AST saved templates quick run" (old-app parity): pick a saved AST engine
// template and start the engine on it in one click. The server applies the
// template's saved settings, restores its ticked strategy set and turns the
// engine on (arm required to place live orders).
// ---------------------------------------------------------------------------
function renderQuickRun() {
  const sel = document.getElementById("rtAstTplRun");
  if (!sel) return;
  const cur = sel.value;
  const names = Object.keys(TPLS).sort();
  const rows = names.map((n) => {
    const t = TPLS[n] || {};
    const nc = Array.isArray(t.selected) ? t.selected.length : 0;
    return { id: n, label: tplLabel(n) + (nc ? " \u00b7 " + nc + " strat" : "") };
  });
  const sig = rows.map((r) => r.id + "~" + r.label).join("|");
  const hasVal = (v) => Array.from(sel.options).some((o) => o.value === v);
  if (sel.dataset.tplIds === sig) {
    if (cur && hasVal(cur)) sel.value = cur;
    return;
  }
  sel.dataset.tplIds = sig;
  sel.innerHTML = rows.length
    ? `<option value="">-- select template --</option>` + rows.map((r) => `<option value="${esc(r.id)}">${esc(r.label)}</option>`).join("")
    : `<option value="">-- no saved templates --</option>`;
  if (cur && hasVal(cur)) sel.value = cur;
}

async function quickRunTemplate() {
  const sel = document.getElementById("rtAstTplRun");
  const status = document.getElementById("rtAstTplRunStatus");
  const name = sel ? sel.value : "";
  if (!name) {
    if (status) status.innerHTML = `<span style="color:#ff9800">Please select a saved template from the dropdown first.</span>`;
    return;
  }
  try {
    const r = await API.template({ action: "run", name });
    if (r && r.ok) {
      // Top Movers with BOTH legs assigned: the gainer (bullish) + loser (bearish)
      // assigned templates drive the live run per direction, not the dropdown
      // template's saved gate settings (old-app parity note).
      const s = (STATE && STATE.settings) || {};
      const moversAssigned = !!s.moversOn && !!s.moverBullTemplate && !!s.moverBearTemplate;
      if (status)
        status.innerHTML =
          `<span style="color:#b39ddb;font-weight:700">RUN</span> template <b style="color:#fff">${esc(r.name)}</b>` +
          ` (${esc(r.side || "")}) started` +
          ` <span style="color:#ff9800">- arm the engine to place live orders</span>` +
          (r.restored ? ` \u00b7 <span style="color:#ffd700">${num(r.restored)} strategy(s) restored</span>` : "") +
          (moversAssigned
            ? `<br><span style="color:#00d4aa">Top Movers assigned templates (gainers + losers) drive the live run per direction.</span>`
            : "");
    } else {
      if (status) status.innerHTML = `<span style="color:#ef5350">Run failed - ${esc((r && r.error) || "see Condition Log")}.</span>`;
    }
    refresh();
    loadAstSections();
  } catch (e) {
    if (status) status.innerHTML = `<span style="color:#ef5350">Run failed - ${esc(String(e))}</span>`;
  }
}
// ---------------------------------------------------------------------------
// Catalog + polling
// ---------------------------------------------------------------------------
function fillCatalog() {
  populateAstSelects();
}

async function loadCatalog() {
  try {
    const [ind, tfs, syms] = await Promise.all([get("/api/indicators"), get("/api/timeframes"), get("/api/symbols")]);
    CATALOG.indicators = Array.isArray(ind) ? ind : [];
    CATALOG.timeframes = Array.isArray(tfs) ? tfs : [];
    const raw = Array.isArray(syms) ? syms : syms && Array.isArray(syms.symbols) ? syms.symbols : [];
    CATALOG.symbols = raw
      .map((s) => {
        if (Array.isArray(s)) {
          // [name, id, exchangeSegment, instrument, optionChainId, optionChainExch, category]
          return {
            id: num(s[1]),
            name: String(s[0] || s[1] || ""),
            exch: s[2] || "NSE_FNO",
            inst: s[3] || "OPTIDX",
          };
        }
        return {
          id: num(s.id || s.securityId || s.security_id),
          name: s.name || s.symbol || s.trading_symbol || String(s.id || ""),
          exch: s.exch || s.exchange_segment || s.exchangeSegment || "NSE_FNO",
          inst: s.inst || s.instrument || s.instrument_type || "OPTIDX",
        };
      })
      .filter((s) => s.id > 0);
  } catch (e) {
    console.warn("realtime catalog load failed", e);
  }
  fillCatalog();
}

async function refresh() {
  if (refreshInFlight) {
    // Coalesce: run one more pass right after the current response lands, so a
    // refresh requested by a button/post never races the in-flight poll.
    refreshQueued = true;
    return;
  }
  refreshInFlight = true;
  const seq = ++refreshSeq;
  try {
    const s = await API.snapshot();
    // Single-flight means only one snapshot can resolve, so this holds; kept as
    // a belt-and-braces guard against any future parallel path.
    if (seq !== refreshSeq) return;
    renderSnapshot(s);
    loadDiagThrottled();
    const conn = document.getElementById("rtConn");
    if (conn && !document.body.classList.contains("link-alarm")) {
      conn.textContent = PAPER ? "Paper engine ready" : "Engine session ready";
      conn.className = "rt-pill on";
    }
    fillCatalog();
  } catch (e) {
    // A stale request that fails after a newer one succeeded must not flip the
    // connection pill back to "unavailable".
    if (seq !== refreshSeq) return;
    console.warn("realtime refresh failed", e);
    const conn = document.getElementById("rtConn");
    if (conn) {
      conn.textContent = PAPER ? "paper engine unavailable" : "engine unavailable";
      conn.className = "rt-pill off";
    }
  } finally {
    refreshInFlight = false;
    if (refreshQueued) {
      refreshQueued = false;
      refresh();
    }
  }
}

// Full closed-trade ledger, loaded on demand (not on the 1s poll). The snapshot
// carries only the newest slice plus the total count; when the count changes we
// re-pull the full ledger once, so every trade still shows without paying the
// payload cost every second.
async function loadClosed() {
  if (closedLoading) return;
  // Coalesce: a burst of closes must not kick off a multi-MB fetch on every
  // poll. One refresh per few seconds is plenty for a read-only ledger.
  if (Date.now() - closedLoadedAt < 3000) return;
  closedLoading = true;
  closedLoadedAt = Date.now();
  try {
    const r = await API.closed();
    if (r && Array.isArray(r.closed)) {
      CLOSED_CACHE = r.closed;
      renderClosed(STATE);
    }
  } catch (e) {
    console.warn("closed ledger load failed", e);
  } finally {
    closedLoading = false;
  }
}

// Last-trading-day stats (`range=today`, anchored to the newest IST day that has
// a closed trade) feeding the engine summary strip. Re-pulled when the
// closed-trade count moves and at most every 15s otherwise, so the summary stays
// "last trading day" without hammering the stats endpoint each second.
async function loadDayStats(closedCount) {
  const sig = String(num(closedCount));
  if (dayStatsLoading) return;
  if (DAY_STATS && sig === dayStatsSig && Date.now() - dayStatsAt < 15000) return;
  dayStatsLoading = true;
  dayStatsAt = Date.now();
  try {
    const r = await API.stats("today");
    if (r && r.stats) {
      DAY_STATS = r.stats;
      dayStatsSig = sig;
    }
  } catch (e) {
    console.warn("day stats load failed", e);
  } finally {
    dayStatsLoading = false;
  }
}

function active() {
  const e = document.getElementById("tab-realtime");
  return !!e && e.classList.contains("active");
}

// ---------------------------------------------------------------------------
// Dhan link monitor
//
// One observer drives the connect/disconnect popups, the persistent red banner,
// the stale-data alarm and the chart "DISCONNECTED" veil. It runs in both the
// realtime window and the paper iframe (which pre-sets `window.__PAPER__`), and
// it polls the same `/api/feed/status` the server watchdog uses, so the UI and
// the server always agree on whether ticks are actually flowing.
//
// Server-side auto-reconnect already lives in the feed watchdog; the client only
// nudges it when a session exists, the market is open and the socket is silent.
// The access token is never persisted, so a full server/tunnel restart shows the
// red "offline" banner and asks the operator to Connect again.
// ---------------------------------------------------------------------------
let linkTimer = null;
const LINK = { state: "unknown", downSince: 0, nextRetry: 0, backoff: 0 };

function ensureLinkUi() {
  // The parent window owns the top-of-page popups/banner; the paper iframe only
  // needs its own pane-level alarm, so it never injects a second banner.
  if (!PAPER) {
    if (!document.getElementById("linkBanner")) {
      const b = document.createElement("div");
      b.id = "linkBanner";
      const body = document.querySelector("body");
      if (body && body.firstChild) body.insertBefore(b, body.firstChild);
      else if (body) body.appendChild(b);
    }
    if (!document.getElementById("linkToasts")) {
      const t = document.createElement("div");
      t.id = "linkToasts";
      document.body.appendChild(t);
    }
  }
  const cw = document.querySelector("#tab-chart .chart-wrap");
  if (cw && !document.getElementById("rtLinkVeil")) {
    const v = document.createElement("div");
    v.id = "rtLinkVeil";
    v.innerHTML =
      "<div>DISCONNECTED</div>" +
      '<div style="font-size:11px;font-weight:400;color:#c9c9e0;letter-spacing:0">' +
      "Live data paused - reconnecting automatically</div>";
    cw.appendChild(v);
  }
}

function linkToast(kind, text, ms) {
  if (PAPER) return; // parent window draws the popups
  ensureLinkUi();
  const wrap = document.getElementById("linkToasts");
  if (!wrap) return;
  const el = document.createElement("div");
  el.className = "link-toast " + (kind === "ok" ? "ok" : "err");
  el.textContent = text;
  wrap.appendChild(el);
  setTimeout(() => {
    el.style.opacity = "0";
    setTimeout(() => el.remove(), 350);
  }, ms || 4500);
}

function linkBanner(kind, text) {
  if (PAPER) return;
  ensureLinkUi();
  const b = document.getElementById("linkBanner");
  if (!b) return;
  if (!kind) {
    b.className = "";
    b.style.display = "none";
    return;
  }
  b.className = "show " + kind;
  b.textContent = text;
}

function setAlarm(on) {
  document.body.classList.toggle("link-alarm", !!on);
  const v = document.getElementById("rtLinkVeil");
  if (v) v.classList.toggle("show", !!on);
}

function clearStaleQuotes() {
  // A disconnected feed must not leave a frozen LTP looking live. The sidebar
  // rows are the only place the last pushed quote is still painted.
  if (PAPER) return;
  document
    .querySelectorAll(
      "#marketWatch .mw-ltp, #marketWatch .mw-chg, #commodityWatch .mw-ltp, #commodityWatch .mw-chg"
    )
    .forEach((el) => {
      el.textContent = "--";
      el.classList.remove("up", "down");
    });
  const sel = chartSelection();
  if (sel) delete sel.ltp;

  // RT order-placement card: drop the auto-filled LTP and broker margin readout
  // so the card cannot quote a stale price. A manually typed price is preserved.
  const pEl = document.querySelector("#rtEngMethodOpts .rtEng-price");
  if (pEl && priceAuto[activeMethod] !== false) pEl.value = "";
  const req = document.getElementById("rtEngReq");
  if (req) {
    delete req.dataset.live;
    req.textContent = "--";
  }
  const fr = document.getElementById("rtEngFreeze");
  if (fr) fr.textContent = "--";
}

function linkStatusText(state, info) {
  if (state === "offline") return (info && info.authError) || "Offline - enter Client ID + Token";
  if (state === "down") {
    const age = info && info.age != null ? " (last tick " + Math.round(info.age) + "s ago)" : "";
    return "Algo Disconnected - auto reconnecting" + age;
  }
  return null;
}

function applyLink(state, info) {
  info = info || {};
  const prev = window.__linkState || "unknown";
  window.__linkState = state;

  const wasAlarm = prev === "down" || prev === "offline";
  const isAlarm = state === "down" || state === "offline";

  // A connect is legitimately offline/down until the handshake completes; don't
  // fire the red popup, banner, veil or stale-wipe during that window.
  if (window.__connecting && state !== "live") return;

  if (state !== prev) {
    if (state === "live") {
      linkToast("ok", "Algo Connected - live Dhan feed streaming");
      linkBanner(null);
    } else if (state === "down" && !wasAlarm) {
      linkToast("err", "Algo Disconnected - auto reconnecting...", 6000);
    } else if (state === "offline" && !wasAlarm) {
      linkToast("err", "Algo Offline - Dhan not connected", 7000);
    }
  }

  if (isAlarm && !wasAlarm) {
    LINK.downSince = Date.now();
    clearStaleQuotes();
  }
  if (state === "live") {
    LINK.downSince = 0;
    LINK.backoff = 0;
    LINK.nextRetry = 0;
  }

  setAlarm(isAlarm);

  // Let the sidebar blank its panels while the link is down (and stop repainting
  // them from the server's last cached quote).
  // Paint the sidebar while the feed is streaming (live) or the session is up
  // but the exchange is shut (closed), so a closed market still shows each
  // symbol's previous close / daily change. Offline / down / connecting freeze
  // the last painted values instead.
  const paintOk = state === "live" || state === "closed";
  window.__feedDown = !paintOk;
  try {
    window.dispatchEvent(new CustomEvent("dhan-link", { detail: { state, live: paintOk, alarm: isAlarm, info } }));
  } catch (e) {
    /* CustomEvent unavailable: non-fatal */
  }

  // Pane pill (shared with refresh(); this is the authoritative value).
  const conn = document.getElementById("rtConn");
  if (conn) {
    if (state === "live") {
      conn.textContent = PAPER ? "Paper engine ready" : "Engine session ready";
      conn.className = "rt-pill on";
    } else if (state === "connecting") {
      conn.textContent = "Connecting...";
      conn.className = "rt-pill armed";
    } else if (state === "closed") {
      conn.textContent = "Market closed";
      conn.className = "rt-pill off";
    } else {
      conn.textContent = PAPER ? "Paper feed offline" : "Feed disconnected";
      conn.className = "rt-pill off";
    }
  }

  // Top-bar status + banner (main window only).
  if (!PAPER) {
    const st = document.getElementById("status");
    if (st) {
      if (state === "live") {
        st.textContent = "Connected - live feed streaming";
        st.className = "ok";
      } else if (state === "closed") {
        st.textContent = "Market closed - feed idle";
        st.className = "warn";
      } else if (state === "connecting") {
        st.textContent = "Connecting to live feed...";
        st.className = "warn";
      } else {
        st.textContent = linkStatusText(state, info);
        st.className = "error";
      }
    }
    if (state === "down") {
      const age = info.age != null ? " (last tick " + Math.round(info.age) + "s ago)" : "";
      linkBanner("err", "Algo DISCONNECTED - auto reconnecting" + age);
    } else if (state === "offline") {
      linkBanner("err", "Algo OFFLINE - Dhan not connected");
    } else if (state === "closed") {
      linkBanner("warn", "Market closed - feed idle");
    } else if (state === "connecting") {
      linkBanner("warn", "Connecting to live feed...");
    } else if (state === "live") {
      linkBanner(null);
    }
  }

  // Server-side auto-reconnect nudge: only while a session exists, the market is
  // open and the socket has gone silent. The server park cooldown is respected.
  if (state === "down" && info.connected && info.open && !info.parked && !PAPER) {
    const now = Date.now();
    if (LINK.downSince && now - LINK.downSince < 6000) return;
    if (now < LINK.nextRetry) return;
    LINK.backoff = LINK.backoff ? Math.min(LINK.backoff * 2, 60) : 12;
    LINK.nextRetry = now + LINK.backoff * 1000;
    fetch("/api/feed/restart", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    }).catch(() => {});
  }
}

// Fallback link derivation for when the server has not been rebuilt with the
// `link` field yet. Mirrors the server's `link_state` so the UI behaves the same
// in both cases. Market hours: IST 09:15-23:30, Mon-Fri (cash + MCX evening).
function istMarketOpen() {
  const now = new Date();
  const ist = new Date(now.getTime() + (330 + now.getTimezoneOffset()) * 60000);
  const day = ist.getDay();
  if (day === 0 || day === 6) return false;
  const mins = ist.getHours() * 60 + ist.getMinutes();
  return mins >= 9 * 60 + 15 && mins <= 23 * 60 + 30;
}

function deriveLink(d) {
  if (d.link) return d.link;
  if (!d.connected) return "offline";
  if (!istMarketOpen()) return "closed";
  // Any frame (trade tick OR heartbeat) within 15s counts as live, so a quiet
  // market with no trade ticks is not misread as a disconnect.
  const age = d.last_activity_age_sec != null ? d.last_activity_age_sec : d.last_tick_age_sec;
  if (age != null && age < 15) return "live";
  if (!d.feed_up && d.ws_running && d.feed_started_age_sec != null && d.feed_started_age_sec < 30) {
    return "connecting";
  }
  return "down";
}

async function linkPoll() {
  let d = null;
  let conn = null;
  try {
    const [fr, sr] = await Promise.all([
      fetch("/api/feed/status", { cache: "no-store" }),
      fetch("/api/status", { cache: "no-store" }),
    ]);
    d = await fr.json();
    try { conn = await sr.json(); } catch (e) { conn = null; }
  } catch (e) {
    d = null;
  }
  if (!d || d.status !== "success") {
    applyLink("offline", { authError: null });
    return;
  }
  if (conn && typeof conn.connected === "boolean" && typeof d.connected !== "boolean") {
    d.connected = conn.connected;
  }
  if (d.market_open == null) d.market_open = istMarketOpen();
  if (conn && conn.auth_error) d.auth_error = conn.auth_error;
  const info = {
    connected: !!d.connected,
    open: !!d.market_open,
    parked: num(d.reconnect_parked_sec) > 0,
    age: d.last_tick_age_sec,
    feedUp: !!d.feed_up,
    authError: d.auth_error || null,
  };
  applyLink(deriveLink(d), info);
}

function bootLinkMonitor() {
  if (linkTimer) return;
  ensureLinkUi();
  const kick = () => {
    if (!PAPER && document.hidden) return; // parent paused; iframe keeps its own
    linkPoll();
  };
  kick();
  linkTimer = setInterval(kick, 2000);
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) kick();
  });
}

// [accent, solid bg, edge, readable text] per section. Cycled across the engine
// rows so every section gets its own flat colour, ribbon and text hue.
const SECTION_COLORS = [
  ["#38bdf8", "#0f2433", "#1f4a63", "#e6eaf5"],
  ["#fbbf24", "#2a2314", "#57491f", "#e6eaf5"],
  ["#34d399", "#0f2a20", "#215340", "#e6eaf5"],
  ["#a78bfa", "#1b1830", "#3a3160", "#e6eaf5"],
  ["#f472b6", "#2a1626", "#552a48", "#e6eaf5"],
  ["#fb7185", "#2b1620", "#582936", "#e6eaf5"],
  ["#22d3ee", "#0e2530", "#1f4c5e", "#e6eaf5"],
  ["#a3e635", "#1f2a14", "#3c5228", "#e6eaf5"],
  ["#fb923c", "#2b1d12", "#573a22", "#e6eaf5"],
  ["#e879f9", "#271733", "#502c66", "#e6eaf5"],
  ["#2dd4bf", "#0e2a26", "#1f544b", "#e6eaf5"],
  ["#60a5fa", "#141d31", "#293a5a", "#e6eaf5"],
];

function paintSections() {
  const rt = document.getElementById("tab-realtime");
  if (!rt) return;
  const paint = (el, i) => {
    const c = SECTION_COLORS[i % SECTION_COLORS.length];
    el.style.setProperty("--sec-accent", c[0]);
    el.style.setProperty("--sec-bg", c[1]);
    el.style.setProperty("--sec-edge", c[2]);
    el.style.setProperty("--sec-text", c[3]);
  };
  rt.querySelectorAll(".rt-engine-row").forEach(paint);
  rt.querySelectorAll(".account-section").forEach((sec, i) => paint(sec, i + 3));
}

export function bootRealtime() {
  if (booted) return;
  booted = true;
  style();
  shell();
  paintSections();
  bootLinkMonitor();
  loadCatalog();
  refresh();
  timer = setInterval(() => {
    if (active()) refresh();
  }, 1000);
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden && active()) refresh();
  });
  document.addEventListener("click", (e) => {
    const t = e.target;
    if (t && t.classList && t.classList.contains("pt-gear")) {
      e.preventDefault();
      e.stopPropagation();
      openPivotTrendSettings(t.getAttribute("data-pt") === "resistance" ? "resistance" : "support");
    }
  });
}


export function realtimeTick(connected) {
  const conn = document.getElementById("rtConn");
  if (conn) {
    conn.textContent = connected ? "Connected" : "Disconnected";
    conn.className = "rt-pill " + (connected ? "on" : "off");
  }
}
