// Shared margin pools UI.
//
// TWO pools are set ONCE in the app header (below Connect / Reconnect):
//   * Paper family  -> one shared margin for Paper Trade 1/2/3 + Equity Paper
//   * Realtime family -> one shared margin for RT 1/2/3 + Equity Live
//
// Inside each engine tab, only the ALLOTMENT is configured: a percentage of the
// family total or a fixed rupee amount, plus a checkbox to activate/deactivate
// it. That allotment is a hard cap enforced by the Rust engine
// (`crate::shared_margin`). Checkbox OFF = the tab keeps its legacy margin
// behaviour.

const SHARED = "/api/margin/shared";

const ENGINE_LABEL = {
  realtime: "Realtime Trading Engine",
  realtime2: "Realtime Trading Engine 2",
  realtime3: "Realtime Trading Engine 3",
  paper: "Paper Trade",
  paper2: "Paper Trade 2",
  paper3: "Paper Trade 3",
  paper4: "Equity Indicator Based Trade",
  autoexp: "Auto Experiment",
  aitrader: "AI Trader (Paper)",
  aitraderlive: "AI Trader (Live)",
  equitypaper: "Equity Paper Engine",
  equitylive: "Realtime Equity Engine",
};

const PAPER_ENGINES = ["paper", "paper2", "paper3", "paper4", "autoexp", "aitrader", "equitypaper"];

let styleInjected = false;
let mounted = {};
let headerEl = null;
let pollTimer = null;

function css() {
  if (styleInjected) return;
  styleInjected = true;
  const s = document.createElement("style");
  s.textContent = `
    .sm-head{display:inline-flex;align-items:center;gap:6px;flex-wrap:wrap;font-size:10px;
      padding:2px 6px;background:#0d0d1e;border:1px solid #2a2a55;border-radius:4px;color:#c4c4e0}
    .sm-head b.sm-h-title{color:#c4a9ff;font-size:10px;letter-spacing:.2px}
    .sm-head .sm-h-grp{display:inline-flex;align-items:center;gap:3px;white-space:nowrap}
    .sm-head .sm-h-grp.paper b{color:#c4a9ff}
    .sm-head .sm-h-grp.real b{color:#ffb0b0}
    .sm-head input[type=number]{width:82px;background:#0a0a18;color:#e0e0ff;border:1px solid #3a3a60;
      border-radius:3px;padding:1px 4px;font-size:10px}
    .sm-head .sm-h-used{color:#888;font-size:9px}
    .sm-head .sm-h-used.over{color:#ff7b72;font-weight:700}
    .sm-head .sm-h-live{font-weight:700;font-size:9px}
    .sm-head .sm-h-src{display:inline-flex;align-items:center;gap:3px;white-space:nowrap;color:#9a86c8}
    .sm-head .sm-h-src input{accent-color:#00d4aa}
    .sm-head .sm-err{color:#ff7b72;font-size:9px}

    .sm-panel{display:flex;align-items:center;gap:8px;flex-wrap:wrap;font-size:10px;
      flex:1 1 100%;width:100%;box-sizing:border-box;
      padding:4px 8px;margin:4px 0;background:#0d0d1e;border:1px solid #2a2a55;border-radius:4px;
      color:#c4c4e0}
    .sm-panel b.sm-title{color:#c4a9ff;text-transform:uppercase;letter-spacing:.3px;font-size:10px}
    .sm-panel .sm-field{display:flex;align-items:center;gap:4px;white-space:nowrap}
    .sm-panel input[type=number]{width:78px;background:#0a0a18;color:#e0e0ff;border:1px solid #3a3a60;
      border-radius:3px;padding:1px 4px;font-size:10px}
    .sm-panel select{background:#0a0a18;color:#e0e0ff;border:1px solid #3a3a60;border-radius:3px;
      padding:1px 3px;font-size:10px}
    .sm-panel input[type=checkbox]{accent-color:#00d4aa}
    .sm-panel .sm-cap{color:#ffd700;font-weight:700}
    .sm-panel .sm-total{color:#9a86c8}
    .sm-panel .sm-used{color:#888}
    .sm-panel .sm-native{opacity:.45;filter:grayscale(.6);transition:opacity .18s,filter .18s}
    .sm-panel .sm-native input{pointer-events:none}
    .sm-panel .sm-hint{color:#666;font-size:9px}
    .sm-panel .sm-err{color:#ff7b72;font-size:9px}
  `;
  document.head.appendChild(s);
}

async function jget(url) {
  const r = await fetch(url);
  return r.json();
}

async function jpost(url, body) {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  return r.json();
}

function fmt(v) {
  return Number(v || 0).toLocaleString("en-IN", { maximumFractionDigits: 0 });
}

function ageLabel(epochSecs) {
  const secs = Math.max(0, Math.floor(Date.now() / 1000 - Number(epochSecs || 0)));
  if (secs < 60) return secs + "s ago";
  if (secs < 3600) return Math.floor(secs / 60) + "m ago";
  return Math.floor(secs / 3600) + "h ago";
}

/**
 * Mount the two pool totals in the app header, right below Connect / Reconnect.
 * Safe to call more than once; only one header panel is created.
 */
export function mountSharedMarginHeader(host) {
  if (!host) return null;
  if (headerEl) return headerEl;
  css();

  const el = document.createElement("div");
  el.className = "sm-head";
  el.innerHTML = `
    <b class="sm-h-title">Shared margin</b>
    <span class="sm-h-grp paper" title="Poori PAPER family ke liye ek hi shared margin: Paper Trade 1/2/3 + Auto Experiment + Equity Paper Engine. Har paper tab ke andar sirf iska allotment (%) ya rupee set hota hai.">
      <b>Paper 1+2+3+AE pool ₹</b>
      <input type="number" class="sm-paper-total" min="0" step="10000">
      <span class="sm-h-used sm-paper-used">--</span>
    </span>
    <span class="sm-h-grp real" title="Poori REALTIME family ke liye ek hi shared margin: Realtime 1/2/3 + Realtime Equity Engine. Default me Dhan account ka live available balance hi pool total hai (real money). Har live tab ke andar sirf iska allotment (%) ya rupee set hota hai.">
      <b>Realtime 1+2+3 pool ₹</b>
      <input type="number" class="sm-real-total" min="0" step="10000">
      <label class="sm-h-src" title="ON = Dhan account ka live available balance pool total banega (recommended, real money). OFF = neeche diya manual amount use hoga.">
        <input type="checkbox" class="sm-real-dhan"> Dhan live</label>
      <span class="sm-h-live">--</span>
      <span class="sm-h-used sm-real-used">--</span>
    </span>
    <span class="sm-err"></span>
  `;
  host.insertAdjacentElement("afterend", el);

  const $ = (c) => el.querySelector(c);
  const paperEl = $(".sm-paper-total");
  const realEl = $(".sm-real-total");
  const realDhan = $(".sm-real-dhan");
  const realLive = $(".sm-h-live");
  const paperUsed = $(".sm-paper-used");
  const realUsed = $(".sm-real-used");
  const errEl = $(".sm-err");
  let data = null;

  const paint = () => {
    if (!data) return;
    // Same guard as the per-tab panel: a focused / just-edited pool total must
    // not be repainted from the saved value by the 15-second poll.
    const ae = document.activeElement;
    if (paperEl !== ae && !paperEl._dirty) paperEl.value = Math.round(data.paperTotal || 0);
    const fromDhan = (data.realtimeSource || "dhan") !== "manual";
    if (realDhan !== ae) realDhan.checked = fromDhan;
    if (realEl !== ae && !realEl._dirty) realEl.value = Math.round(
      fromDhan ? data.realtimeLive || 0 : data.realtimeTotal || 0
    );
    realEl.disabled = fromDhan;
    realEl.title = fromDhan
      ? "Dhan live available balance se aa raha hai. Manual value ke liye 'Dhan live' OFF karo."
      : "Manual realtime pool total";
    const live = Number(data.realtimeLive || 0);
    const age = Number(data.realtimeFetchedAt || 0);
    realLive.textContent =
      age > 0
        ? "Dhan avail ₹" + fmt(live) + " (" + ageLabel(age) + ")"
        : "Dhan avail: not fetched yet";
    realLive.style.color = age <= 0 ? "#ff7b72" : live > 0 ? "#00d4aa" : "#ffb74d";
    const pp = data.paper || {};
    const rp = data.realtime || {};
    paperUsed.textContent =
      "used ₹" + fmt(pp.allotted) + " (" + Number(pp.allottedPct || 0).toFixed(0) + "%)";
    paperUsed.classList.toggle("over", Number(pp.allottedPct || 0) > 100.0001);
    realUsed.textContent =
      "used ₹" + fmt(rp.allotted) + " (" + Number(rp.allottedPct || 0).toFixed(0) + "%)";
    realUsed.classList.toggle("over", Number(rp.allottedPct || 0) > 100.0001);
  };

  const load = async () => {
    try {
      const r = await jget(SHARED);
      if (r && !r.error) {
        data = r;
        paint();
      }
    } catch (_) {}
  };

  const setTotal = async (group, value, source) => {
    errEl.textContent = "";
    try {
      const body = { group, total: Number(value) || 0 };
      if (source) body.source = source;
      const r = await jpost(SHARED + "/total", body);
      if (r && r.error) throw new Error(r.error);
      data = r;
      paint();
      for (const k of Object.keys(mounted)) {
        const n = mounted[k]._load;
        if (typeof n === "function") n();
      }
    } catch (e) {
      errEl.textContent = String(e.message || e);
    }
  };

  const setSource = async (source) => {
    errEl.textContent = "";
    try {
      const body = { group: "realtime", source };
      // Switching to manual with no saved manual amount: seed it with the last
      // known Dhan balance so the pool does not drop to zero.
      if (source === "manual" && !(Number(data && data.realtimeTotal) > 0)) {
        body.total = Number(data && data.realtimeLive) || 0;
      }
      const r = await jpost(SHARED + "/total", body);
      if (r && r.error) throw new Error(r.error);
      data = r;
      paint();
      for (const k of Object.keys(mounted)) {
        const n = mounted[k]._load;
        if (typeof n === "function") n();
      }
    } catch (e) {
      errEl.textContent = String(e.message || e);
    }
  };

  paperEl.addEventListener("input", () => {
    paperEl._dirty = true;
    paperEl.addEventListener("blur", () => { paperEl._dirty = false; }, { once: true });
  });
  realEl.addEventListener("input", () => {
    realEl._dirty = true;
    realEl.addEventListener("blur", () => { realEl._dirty = false; }, { once: true });
  });
  paperEl.addEventListener("change", () => setTotal("paper", paperEl.value));
  realEl.addEventListener("change", () => setTotal("realtime", realEl.value));
  realDhan.addEventListener("change", () => setSource(realDhan.checked ? "dhan" : "manual"));

  load();
  headerEl = el;
  el._load = load;
  startPolling();
  return el;
}

/**
 * Mount the per-tab allotment panel into `host` for `engine`.
 * Only the allotment (% or ₹) and its activate checkbox live here; the family
 * total comes from the app header.
 */
export function mountSharedMargin(host, engine) {
  if (!host || !engine) return null;
  if (mounted[engine]) return mounted[engine];
  css();

  const label = ENGINE_LABEL[engine] || engine;
  const group = PAPER_ENGINES.includes(engine) ? "paper" : "realtime";
  const el = document.createElement("div");
  el.className = "sm-panel";
  el.dataset.engine = engine;
  el.innerHTML = `
    <b class="sm-title">Margin allotment</b>
    <label class="sm-field" title="ON = is tab ko itna margin milega (hard cap). OFF = is tab ko koi margin nahi milega, saari entries block rahengi.">
      <input type="checkbox" class="sm-on"> Allot margin to ${label}</label>
    <span class="sm-allot sm-field">
      <select class="sm-mode" title="Allotment % of the family total, ya fixed rupees.">
        <option value="pct">%</option><option value="rs">₹</option>
      </select>
      <input type="number" class="sm-pct" min="0" max="100" step="1" title="Family total ka kitna percent is tab ko"> %
    </span>
    <span class="sm-allot sm-field"><input type="number" class="sm-rs" min="0" step="10000" title="Is tab ke liye fixed rupees"> ₹</span>
    <span class="sm-total">Pool: --</span>
    <span class="sm-cap">This tab cap: --</span>
    <span class="sm-used">Allotted: --</span>
    <span class="sm-err"></span>
    <span class="sm-hint">Yahi is tab ka margin hai. Pool total header me set hota hai (Connect/Reconnect ke niche).</span>
  `;
  host.insertAdjacentElement("afterbegin", el);

  const $ = (c) => el.querySelector(c);
  const onEl = $(".sm-on");
  const modeEl = $(".sm-mode");
  const pctEl = $(".sm-pct");
  const rsEl = $(".sm-rs");
  const totalEl = $(".sm-total");
  const capEl = $(".sm-cap");
  const usedEl = $(".sm-used");
  const errEl = $(".sm-err");
  let data = null;

  const paintMode = () => {
    const on = onEl.checked;
    const rs = modeEl.value === "rs";
    pctEl.disabled = !on || rs;
    rsEl.disabled = !on || !rs;
    pctEl.parentElement.classList.toggle("sm-native", rs || !on);
    rsEl.parentElement.classList.toggle("sm-native", !rs || !on);
  };

  const paint = () => {
    if (!data) return;
    const mine = (data.engines || {})[engine] || {};
    const pool = data[group] || {};
    // Never overwrite a control the operator is typing in / just edited: the
    // 15-second poll used to repaint every field from the last saved state, so a
    // typed allotment reverted to its old value before it could be saved.
    const ae = document.activeElement;
    const busy = (el) => el === ae || el._dirty;
    if (!busy(onEl)) onEl.checked = !!mine.enabled;
    if (!busy(modeEl)) modeEl.value = mine.mode === "rs" ? "rs" : "pct";
    if (!busy(pctEl)) pctEl.value = mine.pct ?? 0;
    if (!busy(rsEl)) rsEl.value = Math.round(mine.amount ?? 0);
    totalEl.textContent =
      "Pool: ₹" +
      fmt(data[group === "paper" ? "paperTotal" : "realtimeTotal"]) +
      (group === "paper" ? "" : data.realtimeSource === "manual" ? " (manual)" : " (Dhan live)");
    totalEl.title =
      group === "paper"
        ? "Paper family pool total (header me set hota hai)."
        : data.realtimeSource === "manual"
        ? "Realtime pool manual total (Dhan live OFF hai)."
        : "Realtime pool = Dhan account ka live available balance.";
    const cap = Number(mine.cap || 0);
    if (!mine.enabled) {
      capEl.textContent = "This tab cap: ₹0 - OFF, entries blocked";
      capEl.style.color = "#ff7b72";
    } else if (cap <= 0) {
      capEl.textContent = "This tab cap: ₹0 - pool empty, entries blocked";
      capEl.style.color = "#ff7b72";
    } else {
      capEl.textContent = "This tab cap: ₹" + fmt(cap);
      capEl.style.color = "#ffd700";
    }
    const over = (pool.total || 0) > 0 && (pool.allottedPct || 0) > 100.0001;
    usedEl.textContent =
      (group === "paper" ? "Paper" : "Realtime") +
      " pool allotted: ₹" + fmt(pool.allotted) + " / ₹" + fmt(pool.total) +
      " (" + Number(pool.allottedPct || 0).toFixed(0) + "%)";
    usedEl.style.color = over ? "#ff7b72" : "#888";
    paintMode();
  };

  const save = async (body) => {
    errEl.textContent = "";
    try {
      const r = await jpost(SHARED + "/allot", Object.assign({ engine }, body));
      if (r && r.error) throw new Error(r.error);
      data = r;
      paint();
      if (headerEl && typeof headerEl._load === "function") headerEl._load();
      return r;
    } catch (e) {
      errEl.textContent = String(e.message || e);
      return null;
    }
  };

  const load = async () => {
    try {
      const r = await jget(SHARED);
      if (r && !r.error) {
        data = r;
        paint();
      }
    } catch (_) {}
  };

  const touch = (el) => {
    el._dirty = true;
    el.addEventListener("blur", () => { el._dirty = false; }, { once: true });
  };
  onEl.addEventListener("change", () => {
    paintMode();
    save({ enabled: onEl.checked });
  });
  modeEl.addEventListener("change", () => {
    paintMode();
    save({ mode: modeEl.value });
  });
  pctEl.addEventListener("input", () => touch(pctEl));
  rsEl.addEventListener("input", () => touch(rsEl));
  pctEl.addEventListener("change", () => { touch(pctEl); save({ pct: Number(pctEl.value) || 0 }); });
  rsEl.addEventListener("change", () => { touch(rsEl); save({ amount: Number(rsEl.value) || 0 }); });

  load();
  startPolling();
  el._load = load;
  mounted[engine] = el;
  return el;
}

/** Resolve the engine id for the shared realtime UI (`rt181.js`). */
export function realtimeEngineId() {
  try {
    const rb = window.__RT_BASE__ || "";
    if (rb.indexOf("/api/realtime2") === 0) return "realtime2";
    if (rb.indexOf("/api/realtime3") === 0) return "realtime3";
    if (rb.indexOf("/api/aitraderlive") === 0) return "aitraderlive";
    const pb = window.__PAPER_BASE__ || "";
    if (pb.indexOf("/api/paper2") === 0) return "paper2";
    if (pb.indexOf("/api/paper3") === 0) return "paper3";
    if (pb.indexOf("/api/paper4") === 0) return "paper4";
    if (pb.indexOf("/api/autoexp") === 0) return "autoexp";
    if (pb.indexOf("/api/aitraderlive") === 0) return "aitraderlive";
    if (pb.indexOf("/api/aitrader") === 0) return "aitrader";
    return window.__PAPER__ ? "paper" : "realtime";
  } catch (_) {
    return "realtime";
  }
}

function startPolling() {
  if (pollTimer) return;
  pollTimer = setInterval(() => {
    if (headerEl && typeof headerEl._load === "function") headerEl._load();
    for (const k of Object.keys(mounted)) {
      const n = mounted[k]._load;
      if (typeof n === "function") n();
    }
  }, 15000);
}
