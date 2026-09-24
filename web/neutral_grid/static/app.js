/* Neutral grid local console.
 *
 * Rules this file follows (spec NG-UI-001..003, AC-22/46-50):
 *  - every decimal and every id is rendered from the JSON *string* the backend sent; ids are never
 *    converted to JS numbers, and no arithmetic is done on quantities here;
 *  - engine/cell state is shown exactly as committed in the snapshot; nothing is derived from price
 *    and a stale snapshot is shown as "no fresh data", never as the last optimistic state;
 *  - DOM is built with textContent only (no innerHTML with data);
 *  - nothing is written to localStorage/sessionStorage; the CSRF token lives in memory only.
 */
(function () {
  "use strict";

  var ENGINE_STATES = {
    BOOTSTRAPPING: ["Первичная загрузка", "info"],
    RECONCILING: ["Сверка с биржей", "info"],
    NORMAL: ["Работает", "ok"],
    DEGRADED: ["Деградация", "bad"],
    PAUSED: ["Пауза", "warn"],
    RISK_BLOCKED: ["Блокировка по риску", "bad"],
    FROZEN: ["Заморожен до аудита", "bad"],
    STOPPING: ["Останавливается", "warn"],
    STOPPED: ["Остановлен", "off"],
    STOPPED_WITH_INVENTORY: ["Остановлен, позиция сохранена", "off"],
    STOP_UNCERTAIN: ["Остановка не подтверждена", "bad"],
    STALE: ["Нет свежих данных", "stale"],
    UNKNOWN: ["Нет данных", "off"]
  };
  var CELL_STATES = {
    IDLE: ["простой", "off"], QUEUED: ["в очереди", "off"], ENTRY_INTENT: ["вход: намерение", "info"],
    ENTRY_LIVE: ["вход активен", "info"], ENTRY_TERMINAL_UNKNOWN: ["вход: итог неизвестен", "bad"],
    TP_REQUIRED: ["нужен TP", "warn"], TP_INTENT: ["TP: намерение", "warn"], TP_LIVE: ["TP активен", "ok"],
    TP_TERMINAL_UNKNOWN: ["TP: итог неизвестен", "bad"], DUST: ["пыль", "warn"], SETTLING: ["выдержка", "info"],
    COMPLETE: ["цикл завершён", "off"], PAUSED: ["пауза", "warn"], BLOCKED: ["заблокирована", "bad"]
  };
  var ORDER_STATES = {
    INTENT: ["намерение", "info"], SUBMIT_UNKNOWN: ["отправка: итог неизвестен", "bad"], LIVE: ["активен", "ok"],
    CANCEL_PENDING: ["отмена запрошена", "warn"], CANCEL_UNKNOWN: ["отмена: итог неизвестен", "bad"],
    TERMINAL_UNKNOWN: ["завершение не доказано", "bad"], TERMINAL: ["завершён", "off"],
    REJECTED_UNSENT: ["не отправлен", "off"], REJECTED_ZERO_FILL: ["отклонён без исполнения", "off"]
  };
  var COMMAND_STATUS = { QUEUED: "в очереди", APPLIED: "применена", REJECTED: "отклонена", CONFLICT: "конфликт ревизий" };
  var COMMAND_NAMES = {
    start: "Старт", pause: "Пауза", resume: "Продолжить", stop: "Стоп",
    confirm_baseline: "Подтверждение baseline", baseline_audit: "Аудит / ручная сверка"
  };
  var AUDIT_ACTIONS = {
    baseline: "Позиция после ручной сделки/дрейфа: пересверить baseline (rebase)",
    ack_late_evidence: "Поздние исполнения проверены (снять заморозку LATE_EVIDENCE)",
    ack_history_conflict: "Конфликт истории проверен (HISTORY_CONFLICT)",
    ack_retention_gap: "Разрыв хранения истории: ручная сверка без сброса (RETENTION_GAP)",
    ack_risk_blocked: "Блокировка по риску проверена (RISK_BLOCKED)",
    resolve_unknown_submit: "Ордер с неизвестным итогом не попал на биржу (по CID)"
  };
  var COMMAND_HELP = {
    pause: "Новые входы и новые циклы запрещаются. Сверка с историей биржи и поддержка уже подтверждённых TP продолжаются.",
    resume: "Движок сначала проверит свежесть данных, риск и полноту сверки и только потом снова разрешит входы.",
    stop: "Новые входы запрещаются, отменяются только собственные активные ордера. Позиция и обязательства сохраняются, " +
      "автоматического закрытия нет. Итог STOPPED / STOPPED_WITH_INVENTORY будет только после доказанного завершения всех " +
      "ордеров по истории; иначе — STOP_UNCERTAIN.",
    confirm_baseline: "Подтвердите, что фактическая позиция после стабильного снимка и полного среза истории равна B. " +
      "Это делается один раз при первом bootstrap.",
    baseline_audit: "Аудит оператора после ручной сделки, дрейфа позиции или заморозки. Запись попадает в журнал " +
      "аудита; обязательства ячеек, позиция и журнал не сбрасываются, неизвестные исполнения не скрываются."
  };

  var S = {
    csrf: null, mode: null, identity: null, staleAfter: 15,
    state: null, stateReceivedAt: 0, backendDown: false,
    preview: null, tab: "overview",
    cellsCursor: null, cellsFilterKey: "",
    commandsCursor: null, auditCursor: null,
    pendingCommand: null, dialogKey: null, dialogKind: null, dialogRevs: null
  };

  // ------------------------------------------------------------------ helpers
  function $(id) { return document.getElementById(id); }
  function el(tag, attrs, children) {
    var node = document.createElement(tag);
    if (attrs) {
      Object.keys(attrs).forEach(function (k) {
        if (k === "text") node.textContent = attrs[k];
        else if (k === "cls") node.className = attrs[k];
        else node.setAttribute(k, attrs[k]);
      });
    }
    (children || []).forEach(function (c) {
      if (c === null || c === undefined) return;
      node.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
    });
    return node;
  }
  function clear(node) { while (node.firstChild) node.removeChild(node.firstChild); }
  function txt(v) {
    if (v === null || v === undefined || v === "") return "—";
    if (typeof v === "boolean") return v ? "да" : "нет";
    return String(v);
  }
  function fmtTime(sec) {
    if (typeof sec !== "number") return "—";
    try { return new Date(sec * 1000).toLocaleString("ru-RU"); } catch (e) { return String(sec); }
  }
  function fmtAge(sec) {
    if (typeof sec !== "number") return "—";
    if (sec < 60) return Math.floor(sec) + " с";
    if (sec < 3600) return Math.floor(sec / 60) + " мин " + Math.floor(sec % 60) + " с";
    return Math.floor(sec / 3600) + " ч " + Math.floor((sec % 3600) / 60) + " мин";
  }
  function newKey() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") return "ui-" + window.crypto.randomUUID();
    var bytes = new Uint8Array(16);
    window.crypto.getRandomValues(bytes);
    return "ui-" + Array.prototype.map.call(bytes, function (b) { return ("0" + b.toString(16)).slice(-2); }).join("");
  }
  function kv(dl, rows) {
    clear(dl);
    rows.forEach(function (r) {
      if (!r) return;
      dl.appendChild(el("dt", { text: r[0] }));
      var dd = el("dd", { text: txt(r[1]) });
      if (r[2]) dd.className = r[2];
      dl.appendChild(dd);
    });
  }
  function toast(message) {
    var t = $("toast");
    t.textContent = message;
    t.hidden = false;
    clearTimeout(toast._t);
    toast._t = setTimeout(function () { t.hidden = true; }, 4500);
  }
  // Drop out-of-order responses: only the latest request of a kind may render.
  var SEQ = {};
  function nextSeq(kind) { SEQ[kind] = (SEQ[kind] || 0) + 1; return SEQ[kind]; }
  function isLatest(kind, seq) { return SEQ[kind] === seq; }
  function shortCursor(v) {
    if (v === null || v === undefined || v === "") return null;
    var t = String(v);
    return t.length > 28 ? t.slice(0, 12) + "…" + t.slice(-12) : t;
  }

  function statePill(state, table) {
    var info = (table || CELL_STATES)[state] || [state || "—", "off"];
    return el("span", { cls: "pill tone-" + info[1], text: info[0] });
  }

  async function api(path, opts) {
    opts = opts || {};
    var init = { method: opts.method || "GET", credentials: "same-origin", headers: {} };
    if (opts.body !== undefined) {
      init.headers["Content-Type"] = "application/json";
      init.body = JSON.stringify(opts.body);
    }
    if (init.method !== "GET" && S.csrf) init.headers["X-CSRF-Token"] = S.csrf;
    var res;
    try {
      res = await fetch(path, init);
    } catch (e) {
      return { status: 0, data: null, network: true };
    }
    var data = null;
    try { data = await res.json(); } catch (e) { data = null; }
    if (res.status === 401 && path !== "/api/login") showLogin();
    return { status: res.status, data: data };
  }

  // ------------------------------------------------------------------ login / boot
  function showLogin() {
    S.csrf = null;
    $("tabs").hidden = true;
    $("logout").hidden = true;
    document.querySelectorAll("[role=tabpanel]").forEach(function (p) { p.hidden = true; });
    $("panel-login").hidden = false;
  }

  async function login(token) {
    var r = await api("/api/login", { method: "POST", body: { token: token } });
    if (r.status === 200 && r.data) {
      S.csrf = r.data.csrf_token;
      return true;
    }
    $("login-error").textContent = (r.data && r.data.message) || "Не удалось войти.";
    return false;
  }

  async function boot() {
    var hash = window.location.hash || "";
    var m = /^#auth=([A-Za-z0-9_\-]+)$/.exec(hash);
    if (m) {
      history.replaceState(null, "", window.location.pathname);
      await login(m[1]);
    }
    var r = await api("/api/session");
    if (r.status !== 200) { showLogin(); return; }
    S.csrf = r.data.csrf_token;
    S.mode = r.data.mode;
    S.identity = r.data.engine_identity;
    S.staleAfter = r.data.stale_after_s || 15;
    S.demoActions = r.data.demo_actions;
    $("panel-login").hidden = true;
    $("tabs").hidden = false;
    $("logout").hidden = false;
    renderIdentity();
    renderDemoActions();
    selectTab(S.tab, false);
    await refreshState();
    if (!S.timers) {
      S.timers = [setInterval(refreshState, 2000), setInterval(tickAge, 1000)];
    }
  }

  function renderIdentity() {
    var id = S.identity || {};
    $("identity").textContent = [id.connector_name, id.trading_pair, id.account_index !== undefined ? "account " + id.account_index : null, id.grid_id]
      .filter(Boolean).join(" · ");
    var chip = $("mode-chip");
    chip.hidden = false;
    chip.setAttribute("data-mode", S.mode === "demo" ? "demo" : "live");
    chip.textContent = S.mode === "demo" ? "ДЕМО · fake exchange, офлайн" : (S.mode === "attach" ? "Подключение к движку" : "LIVE");
  }

  // ------------------------------------------------------------------ tabs
  var TABS = ["overview", "preview", "cells", "lookup", "journal", "access"];
  function selectTab(name, focus) {
    S.tab = name;
    TABS.forEach(function (t) {
      var tab = $("tab-" + t), panel = $("panel-" + t), on = t === name;
      tab.setAttribute("aria-selected", on ? "true" : "false");
      tab.tabIndex = on ? 0 : -1;
      panel.hidden = !on;
    });
    if (focus) $("tab-" + name).focus();
    if (name === "preview") loadPreview();
    if (name === "cells") loadCells(true);
    if (name === "journal") { loadCommands(true); loadAudit(true); }
    if (name === "access") loadKeystore();
  }
  function wireTabs() {
    TABS.forEach(function (t, i) {
      var tab = $("tab-" + t);
      tab.addEventListener("click", function () { selectTab(t, false); });
      tab.addEventListener("keydown", function (ev) {
        var next = null;
        if (ev.key === "ArrowRight") next = TABS[(i + 1) % TABS.length];
        else if (ev.key === "ArrowLeft") next = TABS[(i - 1 + TABS.length) % TABS.length];
        else if (ev.key === "Home") next = TABS[0];
        else if (ev.key === "End") next = TABS[TABS.length - 1];
        if (next) { ev.preventDefault(); selectTab(next, true); }
      });
    });
  }

  // ------------------------------------------------------------------ state
  async function refreshState() {
    var seq = nextSeq("state");
    var r = await api("/api/state");
    if (!isLatest("state", seq)) return;
    if (r.status === 200 && r.data) {
      S.state = r.data;
      S.stateReceivedAt = Date.now();
      S.backendDown = false;
      renderState();
    } else if (r.status !== 401) {
      S.backendDown = true;
      tickAge();
    }
  }

  function currentAge() {
    var f = S.state && S.state.freshness;
    if (!f || typeof f.age_s !== "number") return null;
    return f.age_s + (Date.now() - S.stateReceivedAt) / 1000;
  }

  function tickAge() {
    var f = S.state && S.state.freshness;
    var age = currentAge();
    var stale = !f || !f.has_snapshot || age === null || age > (f.stale_after_s || S.staleAfter) || S.backendDown;
    var fresh = $("freshness");
    fresh.setAttribute("data-stale", stale ? "true" : "false");
    if (!f || !f.has_snapshot) fresh.textContent = "снимок: нет";
    else fresh.textContent = (stale ? "снимок УСТАРЕЛ: " : "снимок: ") + fmtAge(age) + " назад";
    if (S.backendDown) fresh.textContent += " · нет связи с backend";
    renderBadge(stale);
    var banner = $("stale-banner");
    if (stale && S.state) {
      banner.hidden = false;
      var last = S.state.engine && S.state.engine.last_known_state;
      banner.textContent = (S.backendDown ? "Нет связи с локальным backend. " : "") +
        (f && f.has_snapshot ? "Последний зафиксированный снимок старше " + fmtAge(f.stale_after_s || S.staleAfter) +
          ": текущее состояние движка неизвестно" + (last ? " (последнее известное: " + last + ")." : ".")
          : "Движок ещё не зафиксировал ни одного снимка состояния.");
    } else {
      banner.hidden = true;
    }
  }

  function renderBadge(stale) {
    var engine = (S.state && S.state.engine) || { display_state: "UNKNOWN" };
    var shown = engine.display_state;
    if (stale && shown !== "UNKNOWN") shown = "STALE";
    var info = ENGINE_STATES[shown] || ENGINE_STATES.UNKNOWN;
    var badge = $("state-badge");
    badge.setAttribute("data-state", shown);
    badge.setAttribute("data-tone", info[1]);
    $("state-text").textContent = info[0];
    var code = shown;
    if (shown === "STALE" && engine.last_known_state) code = "последнее: " + engine.last_known_state;
    $("state-code").textContent = code;
  }

  function renderState() {
    var st = S.state;
    var snap = st.snapshot;
    $("revisions").textContent = snap
      ? "config r" + txt(snap.config_revision) + " · engine r" + txt(snap.engine_revision) + " · v" + txt(snap.snapshot_version)
      : "config r— · engine r—";
    tickAge();
    var engine = st.engine || {};
    var summary = st.summary || {};
    // confirm_baseline only after an applied Start (the backend refuses it otherwise, 409 start_required)
    var confirmBtn = document.querySelector("[data-cmd=confirm_baseline]");
    confirmBtn.disabled = !(st.engine_started === true && (summary.baseline === null || summary.baseline === undefined));
    confirmBtn.title = confirmBtn.disabled ? "Доступно после применённого «Старта» и до подтверждения baseline" : "";
    var boot = summary.bootstrap || {};
    var next = null;
    if (st.engine_started === false) next = "Следующий шаг: проверьте превью и отправьте «Старт» (вкладка «Превью и старт»).";
    else if (st.engine_started && (summary.baseline === null || summary.baseline === undefined) && boot.ready === true)
      next = "Следующий шаг: сверка завершена — подтвердите baseline (кнопка «Подтвердить baseline»).";
    else if (st.engine_started && (summary.baseline === null || summary.baseline === undefined))
      next = "Движок сверяет позицию и историю перед подтверждением baseline: " + txt(boot.detail) + ".";
    $("engine-note").textContent = (engine.note || (ENGINE_STATES[engine.display_state] || ["", ""])[0]) +
      (next ? " " + next : "");
    var reasons = $("engine-reasons");
    clear(reasons);
    (engine.reasons || []).forEach(function (r) { reasons.appendChild(el("li", { text: typeof r === "string" ? r : JSON.stringify(r) })); });
    renderSummary(st.summary || {});
    renderCellMap(st.cell_map || []);
    renderErrors(st.errors || []);
    renderRecentCommands(st.recent_commands || []);
    renderUnmatched(st.unmatched_evidence || []);
  }

  function renderSummary(s) {
    var net = s.gauges && s.gauges.net;
    var g = $("net-gauge");
    if (net) {
      g.hidden = false;
      var range = $("net-range");
      range.style.left = net.min_pct + "%";
      range.style.width = "calc(" + net.max_pct + "% - " + net.min_pct + "%)";
      range.setAttribute("data-breach", net.breach);
      var mark = $("net-mark");
      mark.hidden = net.p_pct === undefined;
      if (net.p_pct !== undefined) mark.style.left = "calc(" + net.p_pct + "% - 1px)";
      $("net-cap-neg").textContent = "−" + txt(s.max_abs_net_position);
      $("net-cap-pos").textContent = "+" + txt(s.max_abs_net_position);
    } else {
      g.hidden = true;
    }
    var gross = s.gauges && s.gauges.gross;
    kv($("risk-kv"), [
      ["Baseline B", s.baseline === null && s.bootstrap ? "не подтверждён" : s.baseline],
      ["Эффективный baseline", s.effective_baseline],
      ["Авторитетная позиция (биржа)", s.authoritative_net],
      ["Позиция по журналу", s.ledger_net],
      ["P (журнал + baseline)", s.P],
      ["P_min … P_max", (s.P_min !== undefined || s.P_max !== undefined) ? txt(s.P_min) + " … " + txt(s.P_max) : null,
        net && net.breach === "yes" ? "bad" : ""],
      ["Лимит |net|", s.max_abs_net_position],
      ["Gross (худший)", s.gross_worst, gross && gross.breach === "yes" ? "bad" : ""],
      ["Лимит gross", s.max_gross_position],
      ["Якорь / bid / ask", s.anchor !== undefined ? txt(s.anchor) + " / " + txt(s.bid) + " / " + txt(s.ask) : null]
    ]);
    renderBlockers(s);
    var slots = s.slots || {};
    kv($("orders-kv"), [
      ["Свои активные ордера", s.owned_active],
      ["Ордера с неизвестным итогом", s.unknown_orders, s.unknown_orders && s.unknown_orders !== "0" && s.unknown_orders !== 0 ? "warn" : ""],
      ["Слоты: заняты", slots.actual],
      ["Слоты: зарезервированы", slots.reserved],
      ["Слоты: свободны", slots.free],
      ["Лимит слотов", slots.cap],
      ["Вооружено / в очереди", txt(s.armed) + " / " + txt(s.queued)],
      ["Пыль всего", s.dust_total]
    ]);
    var h = s.history || {};
    kv($("history-kv"), [
      ["Полнота", h.complete === undefined ? null : (h.complete ? "полная" : "НЕПОЛНАЯ"), h.complete === false ? "bad" : "ok"],
      ["Причина неполноты", h.incomplete_reason, h.incomplete_reason ? "warn" : ""],
      ["Отставание истории", typeof h.lag_s === "number" ? fmtAge(h.lag_s) : h.lag_s],
      ["Курсор сделок (непрозрачный)", shortCursor(h.trades_cursor)],
      ["Курсор ордеров (непрозрачный)", shortCursor(h.orders_cursor)],
      ["Последний полный скан", typeof h.last_full_scan_at === "number" ? fmtTime(h.last_full_scan_at) : h.last_full_scan_at]
    ]);
    var mg = s.margin || {};
    var rr = s.runtime_rules || {};
    kv($("margin-kv"), [
      ["Доступно USDG", mg.available],
      ["Оценка маржи", mg.required_estimate],
      ["Предупреждение", mg.warning, mg.warning ? "warn" : ""],
      ["Шаг цены", rr.tick_size],
      ["Шаг объёма", rr.size_step],
      ["Мин. объём", rr.min_base],
      ["Мин. notional", rr.min_notional],
      ["Макс. плечо", rr.max_leverage]
    ]);
  }

  function list(values) {
    if (!values) return null;
    if (Array.isArray(values)) return values.length ? values.join("; ") : "нет";
    var keys = Object.keys(values);
    return keys.length ? keys.map(function (k) { return k + ": " + values[k]; }).join("; ") : "нет";
  }
  function renderBlockers(s) {
    var banner = $("persistence-banner");
    var health = (S.state && S.state.health) || {};
    var texts = [];
    if (s.persistence_error) texts.push("Сбой записи состояния (из снимка): " + s.persistence_error +
      ". Новые submit/cancel не отправляются без зафиксированного намерения.");
    if (health.banner) texts.push(health.banner + (typeof health.age_s === "number" ? " Данные " + fmtAge(health.age_s) + " назад." : ""));
    banner.hidden = !texts.length;
    banner.textContent = texts.join(" ");
    var boot = s.bootstrap || {};
    var tp = s.tp_dispatch || {};
    kv($("blockers-kv"), [
      ["Блокеры входов", list(s.entry_blockers), s.entry_blockers && s.entry_blockers.length ? "warn" : ""],
      ["Блокеры TP", list(s.tp_blockers), s.tp_blockers && s.tp_blockers.length ? "bad" : ""],
      ["Заморозки (нужен аудит)", list(s.freezes), s.freezes && Object.keys(s.freezes).length ? "bad" : ""],
      ["Блокер допуска ячеек", s.admission_blocker],
      ["Пауза оператора", s.operator_paused],
      ["Итог остановки", s.stop_outcome],
      ["Bootstrap: готов к подтверждению", boot.ready === undefined || boot.ready === null ? (s.baseline ? "baseline подтверждён" : null) : boot.ready],
      ["Bootstrap: детали", boot.detail],
      ["Bootstrap: позиция на бирже / B из конфигурации", boot.observed_position !== undefined ?
        txt(boot.observed_position) + " / " + txt(boot.expected_initial_position) : null],
      ["TP dispatch: SLO / последняя / макс., с", s.tp_dispatch ? txt(tp.slo_s) + " / " + txt(tp.last_latency_s) + " / " + txt(tp.max_latency_s) : null],
      ["Позиция сверена", s.position_reconciled]
    ]);
    var ul = $("unknown-orders");
    clear(ul);
    var unknown = s.unknown_active_orders || [];
    ul.hidden = !unknown.length;
    unknown.forEach(function (o) {
      ul.appendChild(el("li", {}, [el("span", { cls: "meta", text: "чужой ордер" }),
        "client " + txt(o.client_order_id) + " · index " + txt(o.order_index) + " · " + txt(o.side) + " " +
        txt(o.remaining) + " @ " + txt(o.price) + " — бот его не отменяет и не присваивает"]));
    });
  }

  function renderCellMap(cells) {
    var map = $("cell-map");
    clear(map);
    var counts = {};
    cells.forEach(function (c) {
      var info = CELL_STATES[c.state] || [c.state || "?", "off"];
      counts[c.state] = (counts[c.state] || 0) + 1;
      var b = el("button", {
        type: "button", cls: "tone-" + info[1], role: "listitem",
        "data-side": c.entry_side || "", "data-blocked": c.blocker ? "yes" : "no",
        "aria-label": "Ячейка " + c.cell_id + ": " + info[0] + (c.entry_side ? ", вход " + c.entry_side : "") + (c.blocker ? ", блокер: " + c.blocker : ""),
        title: "Ячейка " + c.cell_id + " — " + info[0] + (c.blocker ? " — " + c.blocker : ""),
        text: String(c.cell_id)
      });
      b.addEventListener("click", function () { openCell(String(c.cell_id)); });
      map.appendChild(b);
    });
    var legend = $("cell-legend");
    clear(legend);
    Object.keys(counts).sort().forEach(function (state) {
      var info = CELL_STATES[state] || [state, "off"];
      legend.appendChild(el("span", { cls: "tone-" + info[1], text: info[0] + ": " + counts[state] }));
    });
    if (!cells.length) legend.appendChild(el("span", { text: "Ячеек в снимке нет." }));
  }

  function renderErrors(errors) {
    var ul = $("errors-list");
    clear(ul);
    if (!errors.length) { ul.appendChild(el("li", { text: "Ошибок нет." })); return; }
    errors.slice().reverse().forEach(function (e) {
      ul.appendChild(el("li", {}, [el("span", { cls: "meta", text: fmtTime(e.at) + " " + txt(e.code) }), txt(e.message)]));
    });
  }

  function commandLine(c) {
    return el("li", {}, [
      el("span", { cls: "meta", text: "#" + txt(c.id) + " " + fmtTime(c.created_at) }),
      (COMMAND_NAMES[c.kind] || c.kind) + " — ",
      el("strong", { text: COMMAND_STATUS[c.status] || txt(c.status) }),
      c.result ? " · " + (typeof c.result === "string" ? c.result : JSON.stringify(c.result)) : ""
    ]);
  }
  function renderRecentCommands(rows) {
    var ul = $("commands-list");
    clear(ul);
    if (!rows.length) { ul.appendChild(el("li", { text: "Команд ещё не было." })); return; }
    rows.forEach(function (c) { ul.appendChild(commandLine(c)); });
  }
  function renderUnmatched(items) {
    $("unmatched-card").hidden = !items.length;
    var ul = $("unmatched-list");
    clear(ul);
    items.forEach(function (it) { ul.appendChild(el("li", { cls: "mono", text: JSON.stringify(it) })); });
  }

  // ------------------------------------------------------------------ commands
  function currentRevs() {
    var snap = S.state && S.state.snapshot;
    return { cfg: snap ? snap.config_revision : 0, eng: snap ? snap.engine_revision : 0 };
  }

  function openCommand(kind) {
    S.dialogKind = kind;
    S.dialogKey = newKey();
    S.dialogRevs = currentRevs();
    $("cmd-title").textContent = COMMAND_NAMES[kind] || kind;
    $("cmd-desc").textContent = COMMAND_HELP[kind] || "";
    $("cmd-error").textContent = "";
    $("cmd-submit").textContent = "Отправить в очередь";
    var st = S.state || {};
    var age = currentAge();
    kv($("cmd-context"), [
      ["Состояние (по снимку)", st.engine ? st.engine.display_state : null],
      ["Возраст снимка", age === null ? null : fmtAge(age)],
      ["Ожидаемые ревизии", "config r" + txt(S.dialogRevs.cfg) + " · engine r" + txt(S.dialogRevs.eng)]
    ]);
    var fields = $("cmd-fields");
    clear(fields);
    if (kind === "confirm_baseline") {
      fields.appendChild(el("label", { for: "f-baseline", text: "expected_initial_position (B), со знаком" }));
      var inp = el("input", { id: "f-baseline", inputmode: "decimal", spellcheck: "false" });
      fields.appendChild(inp);
      var boot = ((S.state && S.state.summary) || {}).bootstrap || {};
      fields.appendChild(el("p", { cls: "hint", text: "Из конфигурации: B = " + txt(boot.expected_initial_position) +
        "; позиция на бирже по снимку: " + txt(boot.observed_position) + "; готовность: " +
        (boot.ready === true ? "да" : txt(boot.detail)) + ". Введите B вручную." }));
      fields.appendChild(el("label", { cls: "check" }, [el("input", { type: "checkbox", id: "f-confirm" }),
        "Подтверждаю: фактическая позиция равна B."]));
    } else if (kind === "baseline_audit") {
      fields.appendChild(el("label", { for: "f-action", text: "Что проверено" }));
      var sel = el("select", { id: "f-action" });
      Object.keys(AUDIT_ACTIONS).forEach(function (a) { sel.appendChild(el("option", { value: a, text: AUDIT_ACTIONS[a] })); });
      fields.appendChild(sel);
      var obsLabel = el("label", { for: "f-observed", text: "Наблюдаемая позиция на бирже, со знаком (для rebase)" });
      var obs = el("input", { id: "f-observed", inputmode: "decimal", spellcheck: "false" });
      var cidLabel = el("label", { for: "f-cid", text: "Client order ID (для «не попал на биржу»)" });
      var cid = el("input", { id: "f-cid", inputmode: "numeric", spellcheck: "false", maxlength: "20" });
      [obsLabel, obs, cidLabel, cid].forEach(function (n) { fields.appendChild(n); });
      var sync = function () {
        obsLabel.hidden = obs.hidden = sel.value !== "baseline";
        cidLabel.hidden = cid.hidden = sel.value !== "resolve_unknown_submit";
      };
      sel.addEventListener("change", sync);
      sync();
      fields.appendChild(el("label", { for: "f-note", text: "Что именно проверено (обязательно)" }));
      fields.appendChild(el("input", { id: "f-note", maxlength: "500" }));
      fields.appendChild(el("label", { cls: "check" }, [el("input", { type: "checkbox", id: "f-ack" }),
        "Подтверждаю: аудит проведён по истории биржи и не меняет обязательства ячеек."]));
    } else {
      fields.appendChild(el("label", { for: "f-reason", text: "Комментарий (необязательно)" }));
      fields.appendChild(el("input", { id: "f-reason", maxlength: "500" }));
    }
    $("cmd-dialog").showModal();
  }

  function commandPayload(kind) {
    if (kind === "confirm_baseline") {
      return { expected_initial_position: $("f-baseline").value.trim(), confirm: $("f-confirm").checked };
    }
    if (kind === "baseline_audit") {
      var payload = { action: $("f-action").value, note: $("f-note").value.trim(), acknowledge: $("f-ack").checked };
      if (payload.action === "baseline") payload.observed_position = $("f-observed").value.trim();
      if (payload.action === "resolve_unknown_submit") payload.cid = $("f-cid").value.trim();
      return payload;
    }
    var reason = $("f-reason") ? $("f-reason").value.trim() : "";
    return reason ? { reason: reason } : {};
  }

  async function sendCommand(kind, payload, revs, key, errorNode) {
    var body = {
      kind: kind, idempotency_key: key,
      expected_config_revision: revs.cfg, expected_engine_revision: revs.eng, payload: payload
    };
    var r = await api("/api/commands", { method: "POST", body: body });
    if (r.network) {
      errorNode.textContent = "Нет ответа от backend. Повтор отправит ту же команду с тем же ключом — дубликата не будет.";
      return { retry: true };
    }
    if (r.status === 202 || r.status === 200) {
      trackCommand(r.data.command, r.status === 200);
      return { ok: true };
    }
    var msg = (r.data && r.data.message) || ("Ошибка " + r.status);
    if (r.status === 409 && r.data) {
      var cur = r.data.current;
      if (cur) msg += " Текущие ревизии: config r" + txt(cur.config_revision) + ", engine r" + txt(cur.engine_revision) +
        (cur.engine_state ? ", состояние " + cur.engine_state : "") + ".";
      if (r.data.command) msg += " Существующая команда #" + txt(r.data.command.id) + " (" + (COMMAND_STATUS[r.data.command.status] || r.data.command.status) + ").";
      if (r.data.engine_identity) msg += " Движок: " + txt(r.data.engine_identity.grid_id) + ".";
      if (r.data.preview) { S.preview = r.data.preview; renderPreview(); }
      await refreshState();
      return { conflict: true, message: msg, code: r.data.error };
    }
    if (r.data && r.data.errors) msg += " " + r.data.errors.join(" ");
    errorNode.textContent = msg;
    return { error: true };
  }

  function trackCommand(row, replay) {
    S.pendingCommand = row;
    renderPending(replay);
    pollCommand(row.id, 0);
  }
  function renderPending(replay) {
    var c = S.pendingCommand;
    var box = $("pending-command");
    clear(box);
    if (!c) return;
    box.appendChild(el("span", { text: "Команда #" + txt(c.id) + " «" + (COMMAND_NAMES[c.kind] || c.kind) + "»: " }));
    box.appendChild(el("span", { cls: "status", text: COMMAND_STATUS[c.status] || txt(c.status) }));
    if (replay) box.appendChild(el("span", { text: " (повтор: возвращена та же команда)" }));
    if (c.result) box.appendChild(el("div", { cls: "hint", text: "Результат движка: " + (typeof c.result === "string" ? c.result : JSON.stringify(c.result)) }));
  }
  async function pollCommand(id, attempt) {
    if (!S.pendingCommand || String(S.pendingCommand.id) !== String(id) || attempt > 120) return;
    var r = await api("/api/commands/" + encodeURIComponent(String(id)));
    if (r.status === 200 && r.data) {
      S.pendingCommand = r.data.command;
      renderPending(false);
      if (r.data.command.status !== "QUEUED") { refreshState(); return; }
    }
    setTimeout(function () { pollCommand(id, attempt + 1); }, 1500);
  }

  function wireCommands() {
    document.querySelectorAll("[data-cmd]").forEach(function (b) {
      b.addEventListener("click", function () { openCommand(b.getAttribute("data-cmd")); });
    });
    $("cmd-cancel").addEventListener("click", function () { $("cmd-dialog").close(); });
    $("cmd-submit").addEventListener("click", async function () {
      var btn = $("cmd-submit");
      btn.disabled = true;
      $("cmd-error").textContent = "";
      var res = await sendCommand(S.dialogKind, commandPayload(S.dialogKind), S.dialogRevs, S.dialogKey, $("cmd-error"));
      btn.disabled = false;
      if (res.ok) { $("cmd-dialog").close(); toast("Команда записана в очередь движка."); }
      else if (res.retry) btn.textContent = "Повторить (тот же ключ)";
      else if (res.conflict) {
        // never auto-applied: the operator re-reads the fresh state and confirms again with a brand-new key
        openCommand(S.dialogKind);
        $("cmd-error").textContent = "Команда НЕ поставлена в очередь (409). " + res.message +
          " Проверьте актуальные данные выше и подтвердите заново.";
      }
    });
  }

  // ------------------------------------------------------------------ preview / start
  async function loadPreview(baseline) {
    var path = "/api/preview" + (baseline !== undefined ? "?baseline=" + encodeURIComponent(baseline) : "");
    var seq = nextSeq("preview");
    var r = await api(path);
    if (!isLatest("preview", seq)) return null;
    if (r.status === 200) { S.preview = r.data; renderPreview(); return r.data; }
    if (r.data && r.data.message) toast(r.data.message);
    return null;
  }

  function statCard(label, value, sub, bad) {
    return el("article", { cls: "card stat" }, [
      el("span", { cls: "label", text: label }),
      el("span", { cls: "big" + (bad ? " bad" : ""), text: txt(value) }),
      sub ? el("span", { cls: "hint", text: sub }) : null
    ]);
  }

  function renderPreview() {
    var p = S.preview;
    if (!p) return;
    var errs = $("preview-errors");
    clear(errs);
    errs.hidden = !(p.errors && p.errors.length);
    if (p.errors && p.errors.length) {
      errs.appendChild(el("strong", { text: "Ошибки проверки — старт невозможен:" }));
      var ul = el("ul");
      p.errors.forEach(function (e) { ul.appendChild(el("li", { text: e })); });
      errs.appendChild(ul);
    }
    var warns = $("preview-warnings");
    clear(warns);
    warns.hidden = !(p.warnings && p.warnings.length);
    if (p.warnings && p.warnings.length) {
      var wl = el("ul");
      p.warnings.forEach(function (w) { wl.appendChild(el("li", { text: w })); });
      warns.appendChild(wl);
    }
    var g = p.grid || {}, sides = p.sides || {}, adm = p.admission || {}, reach = p.reachable || {}, gross = p.gross || {};
    var lev = p.leverage || {}, notional = p.notional || {}, floors = p.floors || {}, base = p.baseline || {};
    var cards = $("preview-cards");
    clear(cards);
    cards.appendChild(statCard("Границы / ячейки", txt(g.boundaries) + " / " + txt(g.cells), "N+1 фиксированных цен, N ячеек"));
    cards.appendChild(statCard("Первые стороны BUY / SELL", sides.known ? sides.buy + " / " + sides.sell : "—",
      "по оценке якоря " + txt(p.anchor && p.anchor.anchor_estimate)));
    cards.appendChild(statCard("Вооружено / в очереди", txt(adm.armed) + " / " + txt(adm.queued),
      "слотов на ячейку: " + txt(adm.slots_per_cell_min) + (adm.slots_per_cell_max !== adm.slots_per_cell_min ? "–" + txt(adm.slots_per_cell_max) : "")));
    cards.appendChild(statCard("Слоты: заняты / резерв / свободны", txt(adm.slots_actual) + " / " + txt(adm.slots_reserved) + " / " + txt(adm.slots_free),
      "эффективный лимит " + txt(adm.effective_cap) + (adm.venue_cap ? " (площадка " + adm.venue_cap + ")" : "")));
    cards.appendChild(statCard("Baseline B", base.signed, base.source === "missing" ? "не задан — введите при старте" : "источник: " + base.source));
    cards.appendChild(statCard("Достижимые P_min … P_max", txt(reach.P_min) + " … " + txt(reach.P_max),
      "лимит |net| ±" + txt(reach.net_cap) + " при B=" + txt(reach.baseline_used), reach.within_cap === false));
    cards.appendChild(statCard("Gross худший / лимит", txt(gross.worst) + " / " + txt(gross.cap), null, gross.within_cap === false));
    cards.appendChild(statCard("Плечо", txt(lev.configured) + "x", "максимум площадки: " + txt(lev.venue_max)));
    cards.appendChild(statCard("Notional / маржа (оценка)", txt(notional.gross_notional_estimate) + " / " + txt(notional.margin_estimate),
      notional.warning || ("доступно " + txt(notional.available_collateral) + " USDG")));
    cards.appendChild(statCard("Минимумы площадки", "шаг " + txt(floors.size_step) + " · мин " + txt(floors.min_base),
      "tick " + txt(floors.tick_size) + " · мин notional " + txt(floors.min_notional) + " · мин TP " + txt(floors.min_valid_tp_qty_min)));
    cards.appendChild(statCard("Свежесть / выдержка", txt(floors.freshness_s) + " с / " + txt(floors.settlement_delay_s) + " с × " + txt(floors.settlement_scans),
      "перекрытие истории " + txt(floors.history_overlap_s) + " с, опрос " + txt(floors.poll_interval_s) + " с"));

    var prices = $("preview-prices");
    clear(prices);
    var list = g.prices || [];
    var anchor = p.anchor && p.anchor.anchor_estimate;
    $("prices-caption").textContent = list.length ? list.length + " цен; якорь (оценка) " + txt(anchor) +
      ". Цены после старта не меняются." : "Сетка не построена.";
    list.forEach(function (price, i) {
      var cls = "";
      if (sides.known && i < list.length - 1) cls = i < sides.buy ? "buy" : "sell";
      prices.appendChild(el("span", { cls: cls, title: "P[" + i + "]", text: price }));
    });
    var cfg = p.config || {};
    kv($("preview-config"), Object.keys(cfg).map(function (k) { return [k, cfg[k]]; }));
    var engineActive = !!(S.state && S.state.engine_started === true && S.state.engine &&
      ["BOOTSTRAPPING", "RECONCILING", "NORMAL", "DEGRADED", "PAUSED", "RISK_BLOCKED", "FROZEN", "STOPPING"]
        .indexOf(S.state.engine.last_known_state) >= 0);
    $("start-open").disabled = !p.can_start;
    $("start-hint").textContent = !p.can_start ? "Старт недоступен: исправьте ошибки проверки." :
      (engineActive ? "Движок уже работает: повторный старт вернёт существующую команду/движок, второй движок не создаётся." :
        "Старт потребует отдельного подтверждения baseline и риска.");
  }

  function openStart() {
    var p = S.preview;
    if (!p) return;
    $("start-mode-note").textContent = p.mode === "demo"
      ? "Демо-режим: ордера уходят только в офлайн fake exchange."
      : "LIVE: движок будет работать с реальной биржей от имени выбранного профиля ключей.";
    kv($("start-summary"), [
      ["Сетка", txt(p.grid && p.grid.boundaries) + " цен / " + txt(p.grid && p.grid.cells) + " ячеек"],
      ["BUY / SELL (оценка)", p.sides && p.sides.known ? p.sides.buy + " / " + p.sides.sell : null],
      ["Вооружено / очередь", txt(p.admission && p.admission.armed) + " / " + txt(p.admission && p.admission.queued)],
      ["Лимиты net / gross", txt(p.reachable && p.reachable.net_cap) + " / " + txt(p.gross && p.gross.cap)],
      ["Плечо", txt(p.leverage && p.leverage.configured) + "x"],
      ["Ревизии превью", "config r" + p.config_revision + " · engine r" + p.engine_revision],
      ["preview_id", p.preview_id]
    ]);
    $("start-baseline").value = "";
    $("start-baseline-help").textContent = "В конфигурации B = " + txt(p.baseline && p.baseline.signed) +
      ". Введите то же значение вручную: это подтверждение, что фактическая позиция на бирже равна B. Бот не " +
      "покупает и не закрывает её. После сверки движок попросит подтвердить B ещё раз по снимку биржи.";
    $("start-ack-baseline").checked = false;
    $("start-ack-risk").checked = false;
    $("start-error").textContent = "";
    S.startKey = newKey();
    updateStartButton();
    $("start-dialog").showModal();
  }
  function updateStartButton() {
    $("start-submit").disabled = !($("start-ack-baseline").checked && $("start-ack-risk").checked && $("start-baseline").value.trim());
  }
  function wireStart() {
    $("start-open").addEventListener("click", openStart);
    $("preview-refresh").addEventListener("click", function () { loadPreview(); });
    $("start-cancel").addEventListener("click", function () { $("start-dialog").close(); });
    ["start-ack-baseline", "start-ack-risk", "start-baseline"].forEach(function (id) {
      $(id).addEventListener("input", updateStartButton);
      $(id).addEventListener("change", updateStartButton);
    });
    $("start-baseline").addEventListener("input", function () { S.startKey = newKey(); });
    $("start-submit").addEventListener("click", async function () {
      var p = S.preview;
      var btn = $("start-submit");
      btn.disabled = true;
      $("start-error").textContent = "";
      var payload = {
        expected_initial_position: $("start-baseline").value.trim(),
        baseline_acknowledged: $("start-ack-baseline").checked,
        risk_acknowledged: $("start-ack-risk").checked,
        preview_id: p.preview_id
      };
      var res = await sendCommand("start", payload, { cfg: p.config_revision, eng: p.engine_revision }, S.startKey, $("start-error"));
      btn.disabled = false;
      if (res.ok) { $("start-dialog").close(); selectTab("overview", true); toast("Старт записан в очередь движка."); }
      else if (res.retry) btn.textContent = "Повторить (тот же ключ)";
      else if (res.conflict) {
        S.startKey = newKey();
        $("start-ack-baseline").checked = false;
        $("start-ack-risk").checked = false;
        updateStartButton();
        $("start-error").textContent = "Старт НЕ поставлен в очередь (409). " + res.message +
          (res.code === "stale_preview" || res.code === "stale_revision"
            ? " Превью обновлено; проверьте его и подтвердите заново." : "");
        if (S.preview) openStartSummaryOnly();
      }
    });
  }
  function openStartSummaryOnly() {
    var p = S.preview;
    kv($("start-summary"), [
      ["Сетка", txt(p.grid && p.grid.boundaries) + " цен / " + txt(p.grid && p.grid.cells) + " ячеек"],
      ["Ревизии превью", "config r" + p.config_revision + " · engine r" + p.engine_revision],
      ["preview_id", p.preview_id]
    ]);
  }

  // ------------------------------------------------------------------ cells
  function legCell(leg) {
    if (!leg) return el("span", { text: "—" });
    return el("span", { cls: "leg" }, [
      el("span", { cls: "mono", text: txt(leg.requested) + " / " + txt(leg.filled) + " / " + txt(leg.remaining) }), " ",
      statePill(leg.state, ORDER_STATES)
    ]);
  }
  function td(label, children) { return el("td", { "data-label": label }, children); }

  function renderCellRow(c) {
    var entry = c.entry || null;
    var tps = c.tp_children || [];
    var legs = (entry ? [entry] : []).concat(tps);
    var live = legs.filter(function (l) { return l.state === "LIVE"; }).length;
    var unknown = legs.filter(function (l) { return /UNKNOWN/.test(String(l.state || "")); }).length;
    var ids = el("div", { cls: "ids" });
    legs.forEach(function (l, i) {
      ids.appendChild(el("span", { cls: "mono", text: (i === 0 && entry ? "E " : "TP ") + txt(l.cid) + " / " + txt(l.exchange_id) }));
    });
    var tpBox = el("div", {});
    if (!tps.length) tpBox.appendChild(el("span", { text: "—" }));
    tps.forEach(function (t) {
      var line = legCell(t);
      if (t.expiry_at) line.appendChild(el("span", { cls: "sub", text: " GTT до " + fmtTime(t.expiry_at) }));
      tpBox.appendChild(line);
    });
    var ob = c.obligation || {};
    var tr = el("tr", { id: "cell-row-" + c.cell_id }, [
      td("Ячейка", [el("strong", { cls: "mono", text: String(c.cell_id) })]),
      td("Цены", [el("span", { cls: "mono", text: txt(c.low) + " – " + txt(c.high) })]),
      td("Вход", [el("span", { cls: "side-" + c.entry_side, text: txt(c.entry_side) })]),
      td("Поколение", [txt(c.generation)]),
      td("Состояние", [statePill(c.state),
        c.state_flags && c.state_flags.length > 1 ? el("div", { cls: "sub", text: c.state_flags.join(" + ") }) : null,
        c.late_evidence ? el("div", { cls: "sub", text: "поздние исполнения!" }) : null,
        c.armed !== undefined ? el("div", { cls: "sub", text: (c.armed ? "вооружена" : (c.queued ? "в очереди" : "не вооружена")) +
          (c.reserved_slots !== undefined ? " · слотов " + c.reserved_slots : "") }) : null]),
      td("Вход: запр./исп./ост.", [legCell(entry)]),
      td("TP: запр./исп./ост.", [tpBox]),
      td("Живые ноги", [txt(live) + (unknown ? " · неизв.: " + unknown : "")]),
      td("ID client / exchange", [ids]),
      td("Обязательство", [el("span", { cls: "mono", text: "E " + txt(ob.E) + " · X " + txt(ob.X) + " · TP " + txt(ob.live_tp) + " · резерв " + txt(ob.reserved_unassigned) })]),
      td("Пыль", [el("span", { cls: "mono", text: txt(ob.dust) })]),
      td("Блокер", [txt(c.blocker)]),
      td("Очередь", [typeof c.queue_age_s === "number" ? fmtAge(c.queue_age_s) : txt(c.queue_age_s)])
    ]);
    return tr;
  }

  async function loadCells(reset, focusCell) {
    var active = $("cells-active").checked ? "1" : "";
    var state = $("cells-state").value;
    var key = active + "|" + state;
    if (reset || key !== S.cellsFilterKey) { S.cellsCursor = null; S.cellsFilterKey = key; clear($("cells-body")); }
    var q = "?limit=60" + (active ? "&active=1" : "") + (state ? "&state=" + encodeURIComponent(state) : "") +
      (S.cellsCursor ? "&after=" + encodeURIComponent(S.cellsCursor) : "");
    var seq = nextSeq("cells");
    var r = await api("/api/cells" + q);
    if (r.status !== 200 || !isLatest("cells", seq)) return;
    var body = $("cells-body");
    r.data.cells.forEach(function (c) { body.appendChild(renderCellRow(c)); });
    S.cellsCursor = r.data.next_cursor;
    $("cells-more").hidden = !r.data.next_cursor;
    $("cells-count").textContent = "Показано " + body.children.length + " из " + r.data.total_matching;
    if (!body.children.length) body.appendChild(el("tr", {}, [el("td", { colspan: "13", text: "Нет ячеек по фильтру." })]));
    if (focusCell) {
      var row = $("cell-row-" + focusCell);
      if (row) { row.classList.add("highlight"); row.scrollIntoView({ block: "center" }); row.tabIndex = -1; row.focus(); }
    }
  }
  async function openCell(cellId) {
    $("cells-active").checked = false;
    $("cells-state").value = "";
    selectTab("cells", false);
    S.cellsCursor = null;
    clear($("cells-body"));
    // load pages until the row is present (ids compared as strings)
    for (var i = 0; i < 10; i++) {
      await loadCells(false, null);
      if ($("cell-row-" + cellId) || !S.cellsCursor) break;
    }
    var row = $("cell-row-" + cellId);
    if (row) { row.classList.add("highlight"); row.tabIndex = -1; row.focus(); row.scrollIntoView({ block: "center" }); }
  }
  function wireCells() {
    var sel = $("cells-state");
    Object.keys(CELL_STATES).forEach(function (s) { sel.appendChild(el("option", { value: s, text: CELL_STATES[s][0] + " (" + s + ")" })); });
    $("cells-active").addEventListener("change", function () { loadCells(true); });
    sel.addEventListener("change", function () { loadCells(true); });
    $("cell-filters").addEventListener("submit", function (ev) { ev.preventDefault(); });
    $("cells-more").addEventListener("click", function () { loadCells(false); });
  }

  // ------------------------------------------------------------------ lookup
  function objectDl(obj) {
    var dl = el("dl", { cls: "kv" });
    kv(dl, Object.keys(obj || {}).map(function (k) {
      var v = obj[k];
      return [k, v !== null && typeof v === "object" ? JSON.stringify(v) : v];
    }));
    return dl;
  }
  function wireLookup() {
    $("lookup-form").addEventListener("submit", async function (ev) {
      ev.preventDefault();
      var id = $("lookup-id").value.trim();
      var out = $("lookup-result");
      clear(out);
      var seq = nextSeq("lookup");
      var r = await api("/api/lookup?id=" + encodeURIComponent(id));
      if (!isLatest("lookup", seq)) return;
      if (r.status !== 200) { out.appendChild(el("p", { cls: "form-error", text: (r.data && r.data.message) || "Ошибка поиска" })); return; }
      var d = r.data;
      var total = d.snapshot_matches.length + d.orders.length + d.trades.length;
      out.appendChild(el("p", { text: "ID " + d.id + ": найдено " + total + " (сравнение строк, без преобразования в число)." }));
      d.snapshot_matches.forEach(function (m) {
        var title = m.source === "snapshot_leg" ? "Нога " + m.role + " ячейки " + m.cell_id + " (поколение " + txt(m.generation) + ")" : "Несопоставленные данные";
        out.appendChild(el("article", { cls: "card" }, [el("h3", { text: title }), objectDl(m.leg || m.evidence)]));
      });
      d.orders.forEach(function (o) { out.appendChild(el("article", { cls: "card" }, [el("h3", { text: "Ордер (журнал)" }), objectDl(o)])); });
      d.trades.forEach(function (t) { out.appendChild(el("article", { cls: "card" }, [el("h3", { text: "Сделка (история)" }), objectDl(t)])); });
    });
  }

  // ------------------------------------------------------------------ journal
  async function loadCommands(reset) {
    if (reset) { S.commandsCursor = null; clear($("journal-commands").tBodies[0]); }
    var seq = nextSeq("commands");
    var r = await api("/api/commands?limit=50" + (S.commandsCursor ? "&before=" + encodeURIComponent(S.commandsCursor) : ""));
    if (r.status !== 200 || !isLatest("commands", seq)) return;
    var body = $("journal-commands").tBodies[0];
    r.data.commands.forEach(function (c) {
      body.appendChild(el("tr", {}, [
        td("ID", [el("span", { cls: "mono", text: txt(c.id) })]),
        td("Команда", [COMMAND_NAMES[c.kind] || txt(c.kind)]),
        td("Статус", [COMMAND_STATUS[c.status] || txt(c.status)]),
        td("Ревизии", [el("span", { cls: "mono", text: "c" + txt(c.expected_config_revision) + " e" + txt(c.expected_engine_revision) })]),
        td("Создана", [fmtTime(c.created_at)]),
        td("Применена", [fmtTime(c.applied_at)]),
        td("Результат", [c.result ? (typeof c.result === "string" ? c.result : JSON.stringify(c.result)) : "—"])
      ]));
    });
    S.commandsCursor = r.data.next_cursor;
    $("journal-commands-more").hidden = !r.data.next_cursor;
  }
  async function loadAudit(reset) {
    if (reset) { S.auditCursor = null; clear($("journal-audit").tBodies[0]); }
    var seq = nextSeq("audit");
    var r = await api("/api/audit?limit=50" + (S.auditCursor ? "&before=" + encodeURIComponent(S.auditCursor) : ""));
    if (r.status !== 200 || !isLatest("audit", seq)) return;
    var body = $("journal-audit").tBodies[0];
    r.data.events.forEach(function (e) {
      body.appendChild(el("tr", {}, [
        td("ID", [el("span", { cls: "mono", text: txt(e.id) })]),
        td("Время", [fmtTime(e.at)]),
        td("Событие", [txt(e.kind || e.event)]),
        td("Детали", [el("span", { cls: "mono", text: e.detail ? (typeof e.detail === "string" ? e.detail : JSON.stringify(e.detail)) : "—" })])
      ]));
    });
    S.auditCursor = r.data.next_cursor;
    $("journal-audit-more").hidden = !r.data.next_cursor;
  }
  function wireJournal() {
    $("journal-commands-more").addEventListener("click", function () { loadCommands(false); });
    $("journal-audit-more").addEventListener("click", function () { loadAudit(false); });
  }

  // ------------------------------------------------------------------ keystore
  async function loadKeystore() {
    var r = await api("/api/keystore");
    if (r.status !== 200) return;
    renderKeystore(r.data.status, r.data.profiles);
  }
  function renderKeystore(status, profiles) {
    kv($("keystore-status"), [
      ["Режим", status.demo ? "демо (ключи не используются)" : "рабочий"],
      ["Keystore создан", status.keystore_exists],
      ["Выбранный профиль", status.selected_profile],
      ["Разблокирован", status.unlocked, status.unlocked ? "ok" : ""],
      ["Формат API-ключа", { valid: "корректный (80 hex)", invalid: "НЕКОРРЕКТНЫЙ", unknown: "не проверен" }[status.api_key_format],
        status.api_key_format === "invalid" ? "bad" : ""],
      ["Блокировка попыток", status.locked_out ? "да, подождите" : "нет"]
    ]);
    if (profiles) {
      S.profiles = profiles;
      var sel = $("profile-select");
      clear(sel);
      profiles.forEach(function (p) {
        var o = el("option", { value: p.name, text: p.name });
        if (p.name === status.selected_profile) o.selected = true;
        sel.appendChild(o);
      });
      if (!profiles.length) sel.appendChild(el("option", { value: "", text: "профилей нет" }));
      renderProfileFields();
    }
  }
  function renderProfileFields() {
    var name = $("profile-select").value;
    var p = (S.profiles || []).filter(function (x) { return x.name === name; })[0];
    kv($("profile-fields"), p ? Object.keys(p.fields).map(function (k) { return [k, p.fields[k]]; }) : []);
  }
  function wireKeystore() {
    $("profile-select").addEventListener("change", renderProfileFields);
    $("profile-form").addEventListener("submit", async function (ev) {
      ev.preventDefault();
      var r = await api("/api/keystore/select", { method: "POST", body: { profile: $("profile-select").value } });
      if (r.status === 200) { renderKeystore(r.data.status, null); toast("Профиль выбран."); }
      else toast((r.data && r.data.message) || "Ошибка выбора профиля");
    });
    $("unlock-form").addEventListener("submit", async function (ev) {
      ev.preventDefault();
      var input = $("unlock-password");
      var password = input.value;
      input.value = "";  // never keep it in the DOM
      $("unlock-error").textContent = "";
      var r = await api("/api/keystore/unlock", { method: "POST", body: { password: password } });
      password = null;
      if (r.status === 200) { renderKeystore(r.data.status, null); toast("Keystore разблокирован на backend."); }
      else $("unlock-error").textContent = (r.data && r.data.message) || "Ошибка разблокировки";
    });
  }

  // ------------------------------------------------------------------ demo
  function renderDemoActions() {
    var card = $("demo-card");
    var box = $("demo-actions");
    clear(box);
    card.hidden = !(S.mode === "demo" && S.demoActions);
    if (card.hidden) return;
    Object.keys(S.demoActions).forEach(function (name) {
      var b = el("button", { type: "button", cls: "btn", text: S.demoActions[name] });
      b.addEventListener("click", async function () {
        b.disabled = true;
        var r = await api("/api/demo/action", { method: "POST", body: { action: name } });
        b.disabled = false;
        $("demo-result").textContent = r.status === 200 ? "Симулятор: " + (r.data.result && r.data.result.message ? r.data.result.message : name)
          : ((r.data && r.data.message) || "Ошибка");
        refreshState();
      });
      box.appendChild(b);
    });
  }

  // ------------------------------------------------------------------ init
  document.addEventListener("DOMContentLoaded", function () {
    wireTabs();
    wireCommands();
    wireStart();
    wireCells();
    wireLookup();
    wireJournal();
    wireKeystore();
    $("login-form").addEventListener("submit", async function (ev) {
      ev.preventDefault();
      var token = $("login-token").value;
      $("login-token").value = "";
      if (await login(token)) boot();
    });
    $("logout").addEventListener("click", async function () {
      await api("/api/logout", { method: "POST", body: {} });
      showLogin();
    });
    boot();
  });
})();
