// Sidebar: instrument picker, MCX commodities, market watch and the shared
// live-quote engine. Talks to the Rust server (/api/symbols, /api/commodities,
// /ws) and drives the WASM chart via `api.select_symbol`. Quotes arrive only
// over the `/ws` push channel (the client declares its securities there); there
// is no REST quote polling.
//
// Behaviour mirrors the old Python app: a single quote store powers the symbol
// dropdown LTP text, the market-watch rows, the commodity rows and the chart
// header P&L, updated from one merged store.

export function bootSidebar(api) {
  const state = {
    symbols: [],
    watchlists: {},
    groups: {},
    quotes: {},
    selected: null,
    ws: null,
    wsRetry: 0,
    wsLastMsg: 0,
    watchdogTimer: null,
    pollTimer: null,
    polling: false,
    started: false,
    renderPending: false,
    lastSortAt: 0,
    lastOcAt: 0,
  };

  const $ = (id) => document.getElementById(id);
  const quoteKey = (opt) =>
    opt.getAttribute("data-exch") === "IDX_I"
      ? "IDX_I:" + opt.value
      : String(opt.value);

  // -------------------------------------------------------------------------
  // Boot: load catalog + commodities, then start the live feed.
  // -------------------------------------------------------------------------
  // Fetch JSON with a few retries so a transient network/proxy hiccup during
  // server startup cannot silently degrade the sidebar to commodities only.
  async function fetchJson(url, tries) {
    let last;
    for (let i = 0; i < (tries || 4); i++) {
      try {
        const res = await fetch(url, { cache: "no-store" });
        if (!res.ok) throw new Error(url + " HTTP " + res.status);
        return await res.json();
      } catch (e) {
        last = e;
        await new Promise((r) => setTimeout(r, 400 * (i + 1)));
      }
    }
    throw last;
  }

  // Make a catalog failure loud and recoverable instead of showing only the
  // commodities list (which looked like the indices/F&O sections vanished).
  function showCatalogError() {
    const wrap = $("marketWatch");
    if (!wrap) return;
    const n = document.createElement("div");
    n.className = "mw-row";
    n.style.cssText = "opacity:.9;cursor:pointer;background:rgba(255,120,120,.14)";
    n.innerHTML =
      '<span class="mw-name">Instrument catalog load failed - click to retry</span>';
    n.onclick = () => boot();
    wrap.prepend(n);
    console.error("catalog load failed: indices/F&O sections unavailable");
  }

  async function boot() {
    let catalogOk = false;
    try {
      const cat = await fetchJson("/api/symbols");
      state.symbols = cat.symbols || [];
      state.watchlists = cat.watchlists || {};
      catalogOk = state.symbols.length > 0;
    } catch (e) {
      console.error("symbols load failed", e);
    }
    computeGroups();
    buildSymbolOptions();
    // Kick the commodity load off without blocking the initial paint; the master
    // may still be warming, in which case it retries and refreshes in place.
    loadCommodities();
    buildWatchlist();
    renderCommodityRows();
    if (!catalogOk) showCatalogError();
    selectInitial();
    startFeed();
  }

  // Load the MCX commodity list, retrying while the server's contract master is
  // still warming. On a fresh install the master needs ~25-30s to download and
  // parse; the old code fetched once at boot, so the window before that returned
  // an empty/placeholder list which then stuck forever and every row showed `-`.
  // Now we wait for real contract ids and, when they arrive, replace the rows.
  let commodityRetry = 0;
  let commoditiesLoading = false;
  async function loadCommodities() {
    if (commoditiesLoading) return;
    commoditiesLoading = true;
    try {
      for (;;) {
        let d = null;
        try {
          d = await fetchJson("/api/commodities", 2);
        } catch (e) {
          console.error("commodities load failed", e);
        }
        const payload = (d && d.data) || {};
        const list = payload.commodities || [];
        const warming = payload.warming === true;
        if (list.length) {
          applyCommodities(list);
          return;
        }
        if (!warming || commodityRetry >= 40) {
          // Not warming (permanent failure) or we waited long enough: show the
          // empty state honestly instead of a spiral of retries.
          return;
        }
        commodityRetry += 1;
        await new Promise((r) => setTimeout(r, 1000));
      }
    } finally {
      commoditiesLoading = false;
    }
  }

  // Replace the MCX block with an authoritative list. Any MCX symbols/rows from
  // a previous (possibly placeholder or rolled-expiry) response are dropped, so
  // the picker, watchlist and feed subscription all switch to real ids at once.
  function applyCommodities(list) {
    const rows = [];
    const seen = new Set();
    for (const c of list) {
      const sid = Number(c.security_id);
      if (!sid || seen.has(sid)) continue;
      seen.add(sid);
      rows.push([c.name, sid]);
    }
    if (!rows.length) return;
    state.symbols = state.symbols.filter((s) => s[2] !== "MCX_COMM");
    for (const [name, sid] of rows) {
      state.symbols.push([name, sid, "MCX_COMM", "FUTCOM", sid, "MCX_COMM", "Commodities (MCX)"]);
    }
    state.watchlists["Commodities (MCX)"] = rows;
    computeGroups();
    buildSymbolOptions();
    buildWatchlist();
    renderCommodityRows();
    // Force a subscription resend so the new commodity ids start streaming even
    // though the socket is already open.
    if (typeof sendSubscription === "function") sendSubscription(true);
  }

  function computeGroups() {
    const groups = {};
    for (const s of state.symbols) {
      const g = s[6] || "F&O Stocks (NSE)";
      (groups[g] = groups[g] || []).push(s);
    }
    state.groups = groups;
  }

  // -------------------------------------------------------------------------
  // Instrument picker
  // -------------------------------------------------------------------------
  function buildSymbolOptions() {
    const sel = $("symbolSelect");
    sel.innerHTML = "";
    for (const [grp, items] of Object.entries(state.groups)) {
      const og = document.createElement("optgroup");
      og.label = grp;
      for (const s of items) {
        const [name, id, exch, inst, ocId, ocExch] = s;
        const opt = document.createElement("option");
        opt.value = id;
        opt.setAttribute("data-exch", exch);
        opt.setAttribute("data-inst", inst);
        opt.setAttribute("data-oc-id", ocId);
        opt.setAttribute("data-oc-exch", ocExch);
        opt.setAttribute("data-symbol-name", name);
        opt.textContent = name;
        og.appendChild(opt);
      }
      sel.appendChild(og);
    }
  }

  function filterSymbols() {
    const q = ($("symbolSearch").value || "").toLowerCase();
    const sel = $("symbolSelect");
    for (let _i = 0; _i < sel.options.length; _i++) {
      const opt = sel.options[_i];
      opt.style.display = !q || opt.textContent.toLowerCase().includes(q) ? "" : "none";
    }
  }

  function findOption(sid, exch) {
    const sel = $("symbolSelect");
    for (let _i = 0; _i < sel.options.length; _i++) {
      const opt = sel.options[_i];
      if (Number(opt.value) !== Number(sid)) continue;
      const ex = opt.getAttribute("data-exch") || "";
      if (!exch || ex === exch) return opt;
    }
    return null;
  }

  function selectInitial() {
    const opt = findOption(13, "IDX_I") || $("symbolSelect").options[0];
    if (!opt) return;
    state.selected = optionInfo(opt);
    $("chartSymbolLabel").textContent = state.selected.name;
    publishChartTarget(state.selected);
    publishSelection(state.quotes);
  }

  function optionInfo(opt) {
    return {
      id: Number(opt.value),
      exch: opt.getAttribute("data-exch") || "",
      inst: opt.getAttribute("data-inst") || "",
      name: opt.getAttribute("data-symbol-name") || opt.textContent.split("  ")[0],
    };
  }

  function onSymbolChange() {
    const sel = $("symbolSelect");
    const opt = sel.options[sel.selectedIndex];
    if (!opt) return;
    applySelection(opt);
  }

  function applySelection(opt) {
    applySelectionInfo(optionInfo(opt));
  }

  // Switch the chart to an explicit instrument descriptor. Split out from
  // `applySelection` so a watchlist row that is not present in the symbol picker
  // can still open its chart (previously those clicks silently did nothing).
  function applySelectionInfo(info) {
    state.selected = info;
    const lbl = $("chartSymbolLabel");
    if (lbl) lbl.textContent = info.name;
    const pnl = $("chartPnL");
    if (pnl) pnl.innerHTML = "";
    const tp = $("chartTradePnl");
    if (tp) tp.innerHTML = "";
    markSelectedRow(info.id, info.exch);
    publishChartTarget(info);
    api.select_symbol(info.id, info.exch, info.inst, info.name);
    activateChartTab();
  }

  // Dhan instrument type for a cash/index/commodity watchlist row. Derivative
  // rows carry their own type and are resolved through the symbol picker.
  function instForExch(exch) {
    switch ((exch || "").toUpperCase()) {
      case "IDX_I":
        return "INDEX";
      case "MCX_COMM":
        return "FUTCOM";
      case "NSE_FNO":
      case "BSE_FNO":
        return "OPTIDX";
      default:
        return "EQUITY";
    }
  }

  // Remember which instrument the main chart is showing so the trade-level
  // overlay poller (index.html) knows where to draw the live P&L / SL / trail-SL
  // lines. Also published by the option-chain chart opener.
  function publishChartTarget(info) {
    window.__chartTarget = {
      securityId: Number(info.id || 0),
      exchangeSegment: info.exch || "",
      instrument: info.inst || "",
      label: info.name || "",
    };
  }

  function markSelectedRow(sid, exch) {
    document.querySelectorAll("#marketWatch .mw-row").forEach((r) => {
      const same =
        Number(r.dataset.sid) === Number(sid) && (r.dataset.exch || "") === (exch || "");
      r.classList.toggle("sel", same);
    });
  }

  function activateChartTab() {
    // Drive the shared tab switcher so the chart button, its pane and the
    // `tabshown` event all stay in sync with a real user click.
    const btn = document.querySelector('.tab-btn[data-tab="chart"]');
    if (btn) {
      btn.click();
      return;
    }
    document.querySelectorAll(".tab-btn[data-tab]").forEach((b) =>
      b.classList.toggle("active", b.getAttribute("data-tab") === "chart")
    );
    document.querySelectorAll(".tab-content").forEach((c) =>
      c.classList.toggle("active", c.id === "tab-chart")
    );
  }

  // -------------------------------------------------------------------------
  // Market watch
  // -------------------------------------------------------------------------
  function buildWatchlist() {
    const wrap = $("marketWatch");
    wrap.innerHTML = "";
    for (const [cat, items] of Object.entries(state.watchlists)) {
      const catDiv = document.createElement("div");
      catDiv.className = "mw-cat";

      const head = document.createElement("div");
      head.className = "mw-head";
      head.innerHTML =
        '<span class="mw-caret">&#9654;</span> ' +
        cat +
        ' <span class="mw-count">' +
        items.length +
        "</span>";
      head.onclick = () => catDiv.classList.toggle("open");

      const body = document.createElement("div");
      body.className = "mw-body";

      const load = document.createElement("div");
      load.className = "mw-load";
      load.innerHTML =
        '<div class="mw-subhead">Loading quotes&#8230; <span class="mw-subcount"></span></div>';

      const bull = document.createElement("div");
      bull.className = "mw-sub";
      bull.innerHTML = '<div class="mw-subhead up">Bullish <span class="mw-subcount">0</span></div>';
      const bullBox = document.createElement("div");
      bullBox.className = "mw-bull";
      bull.appendChild(bullBox);

      const bear = document.createElement("div");
      bear.className = "mw-sub";
      bear.innerHTML = '<div class="mw-subhead down">Bearish <span class="mw-subcount">0</span></div>';
      const bearBox = document.createElement("div");
      bearBox.className = "mw-bear";
      bear.appendChild(bearBox);

      for (const [name, sid] of items) {
        const exch = commodityExch(sid);
        const row = document.createElement("div");
        row.className = "mw-row";
        row.dataset.sid = sid;
        row.dataset.name = name;
        row.dataset.exch = exch;
        row.innerHTML =
          '<span class="mw-name">' + name + '</span><span class="mw-ltp">--</span><span class="mw-chg">--</span>';
        row.onclick = () => selectWatchlistSymbol(sid, name, exch);
        load.appendChild(row);
      }
      body.appendChild(load);
      body.appendChild(bull);
      body.appendChild(bear);
      catDiv.appendChild(head);
      catDiv.appendChild(body);
      wrap.appendChild(catDiv);
    }
  }

  function commodityExch(sid) {
    const opt = findOption(sid, "MCX_COMM");
    return opt ? "MCX_COMM" : "NSE_EQ";
  }

  function renderCommodityRows() {
    const wrap = $("commodityWatch");
    const list = state.watchlists["Commodities (MCX)"] || [];
    wrap.innerHTML = "";
    if (!list.length) {
      wrap.innerHTML =
        '<div class="mw-row" style="opacity:.55;cursor:default"><span class="mw-name">No commodities</span><span class="mw-ltp">--</span><span class="mw-chg">--</span></div>';
      return;
    }
    for (const [name, sid] of list) {
      const row = document.createElement("div");
      row.className = "mw-row";
      row.dataset.sid = sid;
      row.dataset.name = name;
      row.dataset.exch = "MCX_COMM";
      row.innerHTML =
        '<span class="mw-name">' + name + '</span><span class="mw-ltp">--</span><span class="mw-chg">--</span>';
      row.onclick = () => selectWatchlistSymbol(sid, name, "MCX_COMM");
      wrap.appendChild(row);
    }
  }

  function selectWatchlistSymbol(sid, name, exch) {
    const opt = findOption(sid, exch);
    if (opt) {
      const sel = $("symbolSelect");
      sel.selectedIndex = opt.index;
      applySelection(opt);
      return;
    }
    // Watchlist-only row (or a commodity the picker does not list): open its
    // chart directly, keyed off the row's own exchange.
    applySelectionInfo({
      id: Number(sid),
      exch: exch || "NSE_EQ",
      inst: instForExch(exch),
      name: name || String(sid),
    });
  }

  function toggleAllWatchlist() {
    const cats = document.querySelectorAll("#marketWatch .mw-cat");
    const anyOpen = Array.from(cats).some((c) => c.classList.contains("open"));
    cats.forEach((c) => c.classList.toggle("open", !anyOpen));
    const btn = $("mwToggleBtn");
    if (btn) btn.textContent = anyOpen ? "Expand All" : "Collapse All";
  }

  function renderGroup(box, list) {
    const cur = Array.from(box.children);
    const same = cur.length === list.length && list.every((r, i) => r === cur[i]);
    if (same) return;
    box.replaceChildren(...list);
  }

  function updateWatchlist(qm, allowSort) {
    const cats = document.querySelectorAll("#marketWatch .mw-cat");
    for (const cat of cats) {
      const rows = Array.from(cat.querySelectorAll(".mw-row"));
      const quoted = [];
      for (const row of rows) {
        const q = qm[String(row.dataset.sid)];
        if (q && q.change_pct !== undefined) {
          row.dataset.pct = q.change_pct;
          row.querySelector(".mw-ltp").textContent = (q.ltp || 0).toFixed(2);
          const pct = q.change_pct;
          const pts = q.change || 0;
          const chgEl = row.querySelector(".mw-chg");
          chgEl.textContent =
            (pts >= 0 ? "+" : "") +
            pts.toFixed(2) +
            " (" +
            (pct >= 0 ? "+" : "") +
            pct.toFixed(2) +
            "%)";
          chgEl.className = "mw-chg " + (pct >= 0 ? "up" : "down");
          quoted.push(row);
        }
      }
      const bullBox = cat.querySelector(".mw-bull");
      const bearBox = cat.querySelector(".mw-bear");
      const load = cat.querySelector(".mw-load");
      if (allowSort) {
        const bull = quoted
          .filter((r) => parseFloat(r.dataset.pct) >= 0)
          .sort((a, b) => parseFloat(b.dataset.pct) - parseFloat(a.dataset.pct));
        const bear = quoted
          .filter((r) => parseFloat(r.dataset.pct) < 0)
          .sort((a, b) => parseFloat(b.dataset.pct) - parseFloat(a.dataset.pct));
        renderGroup(bullBox, bull);
        renderGroup(bearBox, bear);
        const bullCount = cat.querySelector(".mw-bull .mw-subcount");
        const bearCount = cat.querySelector(".mw-bear .mw-subcount");
        if (bullCount) bullCount.textContent = bull.length;
        if (bearCount) bearCount.textContent = bear.length;
      }
      if (load) {
        load.style.display = load.children.length > 1 ? "" : "none";
        const lc = load.querySelector(".mw-subcount");
        if (lc) lc.textContent = load.children.length - 1;
      }
    }
  }

  function updateCommodityWatch(qm) {
    document.querySelectorAll("#commodityWatch .mw-row[data-sid]").forEach((row) => {
      const q = qm[String(row.dataset.sid)];
      if (!q || q.change_pct === undefined) return;
      row.querySelector(".mw-ltp").textContent = (q.ltp || 0).toFixed(2);
      const pct = q.change_pct;
      const pts = q.change || 0;
      const chgEl = row.querySelector(".mw-chg");
      chgEl.textContent =
        (pts >= 0 ? "+" : "") + pts.toFixed(2) + " (" + (pct >= 0 ? "+" : "") + pct.toFixed(2) + "%)";
      chgEl.className = "mw-chg " + (pct >= 0 ? "up" : "down");
    });
  }

  // -------------------------------------------------------------------------
  // Live quote engine
  // -------------------------------------------------------------------------
  function mergeQuotes(qm) {
    if (!qm) return;
    for (const k in qm) {
      const q = qm[k];
      if (q) state.quotes[k] = q;
    }
  }

  // Expose the active chart instrument (plus its live ltp/oi/volume) so the
  // Realtime Trading Engine's Order Placement cards price the exact same symbol
  // the user is viewing, from the same live quote cache.
  function publishSelection(qm) {
    if (!state.selected) return;
    const key = state.selected.exch === "IDX_I" ? "IDX_I:" + state.selected.id : String(state.selected.id);
    const q = (qm && qm[key]) || state.quotes[key] || {};
    window.__chartSelection = {
      id: state.selected.id,
      exch: state.selected.exch,
      inst: state.selected.inst,
      name: state.selected.name,
      ltp: q.ltp || 0,
      oi: q.oi || 0,
      volume: q.volume || 0,
      changePct: q.change_pct,
    };
    window.dispatchEvent(new CustomEvent("chartselection", { detail: window.__chartSelection }));
  }

  // Coalesce the many /ws frames of a burst into one paint per animation frame,
  // capped at ~10 Hz. Without this every incoming frame ran a full render
  // (dropdown rewrite, watchlist sort/reorder, option-chain serialisation),
  // which read as a stuttering sidebar.
  let lastRenderAt = 0;
  function scheduleRender() {
    if (state.renderPending) return;
    state.renderPending = true;
    const run = () => {
      state.renderPending = false;
      const now = performance.now();
      if (now - lastRenderAt < 100) {
        setTimeout(scheduleRender, 100 - (now - lastRenderAt));
        return;
      }
      lastRenderAt = now;
      render();
    };
    if (typeof requestAnimationFrame === "function") {
      requestAnimationFrame(run);
    } else {
      setTimeout(run, 16);
    }
  }

  function render() {
    // Repaint whenever fresh quotes arrive. The live feed pushes ticks, and the
    // REST fallback (see pollRestFallback) rebroadcasts Dhan REST quotes on /ws
    // while the feed is rate-limited, so the day-change column keeps moving.
    const qm = state.quotes;
    publishSelection(qm);
    // Reorder only on a slow cadence: sorting/replacing rows on every frame made
    // the watchlist visibly jump ("ruk ruk"). Text still updates in place below.
    const now = Date.now();
    const allowSort = now - state.lastSortAt >= 1000;
    if (allowSort) state.lastSortAt = now;
    const sel = $("symbolSelect");
    if (sel && sel.options.length) {
      for (let _i = 0; _i < sel.options.length; _i++) {
      const opt = sel.options[_i];
        const q = qm[quoteKey(opt)];
        if (q && q.change_pct !== undefined) {
          const pct = q.change_pct;
          const pts = q.change || 0;
          const name = opt.getAttribute("data-symbol-name") || opt.textContent.split("  ")[0];
          opt.textContent =
            name.padEnd(14, " ") +
            "  " +
            (q.ltp || 0).toFixed(2) +
            "  " +
            (pct >= 0 ? "+" : "") +
            pct.toFixed(2) +
            "%  " +
            (pts >= 0 ? "+" : "") +
            pts.toFixed(2);
        }
      }
    }
    updateWatchlist(qm, allowSort);
    updateCommodityWatch(qm);
    updateChartPnL(qm);
    // Serialising the whole quote map for the option-chain tab is expensive; do
    // it at most ~3x/sec instead of on every paint.
    if (api.update_oc_quotes && now - state.lastOcAt >= 300) {
      state.lastOcAt = now;
      try {
        api.update_oc_quotes(JSON.stringify(qm));
      } catch (e) {
        /* option-chain tab not ready */
      }
    }
  }

  function updateChartPnL(qm) {
    const el = $("chartPnL");
    if (!el || !state.selected) return;
    const key =
      state.selected.exch === "IDX_I"
        ? "IDX_I:" + state.selected.id
        : String(state.selected.id);
    const q = qm[key];
    if (!q || q.change_pct === undefined) return;
    const pct = q.change_pct;
    const pts = q.change || 0;
    const isIdx = state.selected.inst === "INDEX";
    const col = pct >= 0 ? "#00d4aa" : "#ff4d6a";
    if (isIdx) {
      el.innerHTML =
        '<span style="color:#aaa;margin:0 4px">|</span> <b style="color:' +
        col +
        '">' +
        (pts >= 0 ? "+" : "") +
        pts.toFixed(2) +
        ' pts</b> <span style="color:' +
        col +
        '">(' +
        (pct >= 0 ? "+" : "") +
        pct.toFixed(2) +
        "%)</span>";
    } else {
      el.innerHTML =
        '<span style="color:#aaa;margin:0 4px">|</span> LTP <b style="color:#fff">' +
        (q.ltp || 0).toFixed(2) +
        '</b> <span style="color:' +
        col +
        '">' +
        (pts >= 0 ? "+" : "") +
        pts.toFixed(2) +
        " (" +
        (pct >= 0 ? "+" : "") +
        pct.toFixed(2) +
        "%)</span>";
    }
  }

  function securitiesList() {
    const sel = $("symbolSelect");
    const out = [];
    if (!sel) return out;
    for (let _i = 0; _i < sel.options.length; _i++) {
      const opt = sel.options[_i];
      out.push({
        security_id: parseInt(opt.value, 10),
        exchange_segment: opt.getAttribute("data-exch"),
      });
    }
    const cmd = state.watchlists["Commodities (MCX)"] || [];
    for (const [, sid] of cmd) {
      out.push({ security_id: sid, exchange_segment: "MCX_COMM" });
    }
    // Option strikes registered by the option-chain tab stay priced live.
    const oc = window.__ocSecurities;
    if (Array.isArray(oc)) {
      for (const s of oc) out.push(s);
    }
    return out;
  }

  // The sidebar is fed exclusively by the `/ws` push channel: the client tells
  // the server which securities it is showing and the server subscribes them on
  // the live Dhan feed. There is no REST quote polling.
  let lastSubKey = "";
  function sendSubscription(force) {
    const ws = state.ws;
    if (!ws || ws.readyState !== 1) return;
    const list = securitiesList();
    const key = list
      .map((s) => s.security_id + ":" + s.exchange_segment)
      .join("|");
    // Re-sending the identical set every 2s made the server re-subscribe the
    // whole book (and promote indices to Full mode, which stops their ticks).
    // Only announce a changed set, or an explicit forced resync.
    if (!force && key === lastSubKey) return;
    try {
      ws.send(JSON.stringify({ securities: list }));
      lastSubKey = key;
    } catch (e) {
      /* socket closing; onclose will reconnect */
    }
  }

  function connectStream() {
    try {
      const proto = location.protocol === "https:" ? "wss://" : "ws://";
      const ws = new WebSocket(proto + location.host + "/ws");
      state.ws = ws;
      ws.onopen = () => {
        state.wsRetry = 0;
        state.wsLastMsg = Date.now();
        lastSubKey = "";
        sendSubscription(true);
      };
      ws.onmessage = (ev) => {
        state.wsLastMsg = Date.now();
        try {
          const m = JSON.parse(ev.data);
          if (m.type === "quotes") {
            mergeQuotes(m.data || {});
            scheduleRender();
          }
        } catch (e) {
          /* ignore malformed frame */
        }
      };
      ws.onclose = () => {
        state.ws = null;
        const delay = Math.min(5000, 500 * Math.pow(2, state.wsRetry++));
        setTimeout(connectStream, delay);
      };
      ws.onerror = () => {
        try {
          ws.close();
        } catch (e) {}
      };
    } catch (e) {
      setTimeout(connectStream, 2000);
    }
  }

  // App-level liveness watchdog for the sidebar socket. The server echoes a
  // `{"type":"ping"}` with a pong; a half-open socket keeps accepting sends but
  // never answers, so a missing reply past the grace window means the socket is
  // dead and must be re-established (its `onclose` never fires on its own).
  const WS_PING_MS = 10000;
  const WS_STALE_MS = 22000;
  function watchdogTick() {
    const ws = state.ws;
    if (!ws || ws.readyState !== 1) return;
    if (Date.now() - state.wsLastMsg > WS_STALE_MS) {
      state.wsRetry = 0;
      try {
        ws.close();
      } catch (e) {}
      return;
    }
    try {
      ws.send(JSON.stringify({ type: "ping" }));
    } catch (e) {}
  }

  function startFeed() {
    if (state.started) return;
    state.started = true;
    connectStream();
    // The liveness watchdog pings the socket and reaps a silent (half-open) one.
    // It keeps running while the tab is hidden, and a visibility check recovers
    // the instant the user is back, without pressing Connect.
    state.watchdogTimer = setInterval(watchdogTick, WS_PING_MS);
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) watchdogTick();
    });
    // Newly added watchlist rows / option strikes are announced over the same
    // socket; a light 2s resend keeps the feed's subscription set in sync.
    state.pollTimer = setInterval(sendSubscription, 2000);
    // REST fallback for Dhan feed outages / rate-limit blocks: while the live
    // link is down, ask the server to refresh these securities from Dhan's REST
    // quote API. The server rebroadcasts them on /ws, so both the sidebar's
    // day-change column and the chart's forming candle keep updating (slower
    // than ticks, but live) instead of freezing until the feed returns.
    // Adaptive REST fallbacks: poll fast while prices move, back off hard when
    // nothing changes (closed market / holiday). Hammering Dhan on a holiday
    // tripped 429s and escalated its "user may be blocked" penalty.
    scheduleRestFallback(3000);
    scheduleIndexPoll(5000);
  }

  // The `/ws` push channel is the primary quote source. It can die while HTTP
  // still works (a proxy/preview tunnel dropping the WebSocket, or a half-open
  // socket): the header then keeps saying "Connected" from `/api/status` while
  // every sidebar/chart price freezes. Treat a closed or silent socket as a
  // fallback trigger too, not just a Dhan feed outage.
  function wsStreamStale() {
    const ws = state.ws;
    if (!ws || ws.readyState !== 1) return true;
    return Date.now() - (state.wsLastMsg || 0) > 15000;
  }

  let restDelay = 3000;
  let restSig = "";
  function scheduleRestFallback(ms) {
    clearTimeout(state.restTimer);
    state.restTimer = setTimeout(async () => {
      const changed = await pollRestFallback();
      restDelay = changed ? 3000 : Math.min(restDelay * 2, 120000);
      scheduleRestFallback(restDelay);
    }, ms);
  }

  async function pollRestFallback() {
    if (document.hidden) return true;
    // Feed healthy: nothing to do, but stay fast so an outage is picked up at once.
    if (!window.__feedDown && !wsStreamStale()) return true;
    const secs = securitiesList();
    // Include whatever the chart is showing so its forming candle also refreshes
    // from the REST broadcast, not just the watchlist rows.
    const cs = window.__chartSelection;
    if (cs && cs.id) secs.push({ security_id: cs.id, exchange_segment: cs.exch });
    if (!secs.length) return true;
    try {
      const r = await fetch("/api/quotes", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ securities: secs }),
      });
      // Apply the HTTP response directly: when the WebSocket is the thing that
      // died, waiting for the server's /ws rebroadcast would never update the
      // UI. This keeps the sidebar (and the chart's selection) live over HTTP.
      const j = await r.json();
      if (j && j.data) {
        const sig = Object.values(j.data)
          .reduce((a, q) => a + (q && q.at || 0), 0);
        const changed = sig !== restSig;
        restSig = sig;
        mergeQuotes(j.data);
        render();
        return changed;
      }
    } catch (e) {
      /* transient; back off rather than hammer a failing upstream */
    }
    return false;
  }

  // Dhan's live feed does not push IDX_I packets on every account (stocks keep
  // ticking while the indices stay frozen on their subscribe snapshot). Poll
  // just the five indices over REST on a slow cadence so the sidebar daily
  // change and the chart's index price keep moving even then. The server gate
  // keeps this from piling requests onto Dhan's rate limit.
  let indexDelay = 5000;
  let indexSig = "";
  function scheduleIndexPoll(ms) {
    clearTimeout(state.indexTimer);
    state.indexTimer = setTimeout(async () => {
      const changed = await pollIndexQuotes();
      indexDelay = changed ? 5000 : Math.min(indexDelay * 2, 60000);
      scheduleIndexPoll(indexDelay);
    }, ms);
  }

  async function pollIndexQuotes() {
    if (document.hidden) return true;
    // The broad fallback above already refreshes every row when the socket is
    // down or silent; do not double the Dhan quote load in that state.
    if (window.__feedDown || wsStreamStale()) return true;
    const secs = securitiesList().filter(
      (s) => s.exchange_segment === "IDX_I"
    );
    if (!secs.length) return true;
    try {
      const r = await fetch("/api/quotes", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ securities: secs }),
      });
      const j = await r.json();
      if (j && j.data) {
        const sig = secs
          .map((s) => (j.data["IDX_I:" + s.security_id] || {}).ltp || 0)
          .join("|");
        const changed = sig !== indexSig;
        indexSig = sig;
        mergeQuotes(j.data);
        render();
        return changed;
      }
    } catch (e) {
      /* transient; back off rather than hammer a failing upstream */
    }
    return false;
  }

  // -------------------------------------------------------------------------
  // Wiring
  // -------------------------------------------------------------------------
  function wire() {
    $("symbolSearch").addEventListener("input", filterSymbols);
    $("symbolSelect").addEventListener("change", onSymbolChange);
    $("mwToggleBtn").addEventListener("click", toggleAllWatchlist);
    $("mwRefresh").addEventListener("click", () => sendSubscription(true));
    $("cmdtyRefresh").addEventListener("click", () => sendSubscription(true));
    // Re-announce the on-screen securities the moment a Connect completes so the
    // server subscribes them to the live feed without waiting for the 2s sync.
    window.addEventListener("dhan-connected", () => sendSubscription(true));
    // The link monitor broadcasts every feed-state change. When it goes down the
    // panels are blanked immediately; a fresh /ws push repaints them once live.
    window.addEventListener("dhan-link", (ev) => {
      window.__feedDown = !(ev.detail && ev.detail.live);
    });
  }

  // Reconnect on Disconnection: every 2s read the indices' daily-change text
  // from the instrument picker. Index quotes can legitimately pause for many
  // seconds, so a stall is only acted on after a long silence, and a Connect
  // press is rate-limited. If the Dhan link is still streaming, re-announce the
  // subscription instead of tearing the feed down (the old 3s press loop
  // restarted the feed forever, which is what froze the daily change and the
  // live ticks). The top-header checkbox enables or disables the auto-press;
  // the status label always shows the live state.
  function startReconnectOnDisconnection() {
    const CHECK_MS = 2000;
    const STALL_MS = 45000;
    const MIN_PRESS_MS = 90000;
    let lastSnap = "";
    let lastAdvance = Date.now();
    let lastPress = 0;
    let checking = false;

    const statusEl = $("indicesChangeStatus");
    const toggle = $("reconnectOnDisconnect");

    const setStatus = (live) => {
      if (!statusEl) return;
      statusEl.textContent = live
        ? "Indices daily change live"
        : "Indices daily changes stopped";
      statusEl.classList.toggle("stopped", !live);
    };

    // Only the index instruments (exchange IDX_I), not stocks or commodities.
    const indicesSnapshot = () => {
      const parts = [];
      document
        .querySelectorAll('#symbolSelect option[data-exch="IDX_I"]')
        .forEach((opt) => parts.push(opt.value + "=" + opt.textContent));
      return parts.join("|");
    };

    setStatus(true);

    setInterval(() => {
      const snap = indicesSnapshot();
      if (!snap) return;
      const now = Date.now();
      const btn = $("connectBtn");

      if (snap !== lastSnap) {
        lastSnap = snap;
        lastAdvance = now;
        setStatus(true);
        return;
      }

      if (now - lastAdvance < STALL_MS) return;
      setStatus(false);
      if (toggle && !toggle.checked) return;
      if (checking) return;
      if (!btn || btn.disabled || window.__connecting) return;
      if (now - lastPress < MIN_PRESS_MS) return;
      checking = true;

      // Ask the server what the Dhan link is really doing before touching it.
      fetch("/api/feed/status")
        .then((r) => r.json())
        .then((d) => {
          const streaming =
            d &&
            d.feed_up === true &&
            d.last_tick_age_sec != null &&
            d.last_tick_age_sec < 30;
          if (streaming) {
            // Feed is alive: the index simply has not ticked, so refresh the
            // subscription without restarting the socket.
            lastAdvance = Date.now();
            sendSubscription(true);
            return;
          }
          if (!btn || btn.disabled || window.__connecting) return;
          if (now - lastPress < MIN_PRESS_MS) return;
          const connected = btn.textContent.indexOf("Connected") !== -1;
          if (connected && d && d.feed_up) {
            lastAdvance = Date.now();
            sendSubscription(true);
            return;
          }
          const cid = $("clientId");
          const tok = $("accessToken");
          if (!cid || !tok || !cid.value.trim() || !tok.value.trim()) {
            // No credentials in the form: nothing we can reconnect with.
            return;
          }
          lastPress = Date.now();
          lastAdvance = Date.now();
          console.warn("indices daily change stopped - pressing Connect");
          btn.click();
        })
        .catch(() => {})
        .finally(() => {
          checking = false;
        });
    }, CHECK_MS);
  }

  wire();
  boot();
  startReconnectOnDisconnection();
}
