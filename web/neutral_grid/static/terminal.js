/* Read-only terminal view. Exact monetary and identity strings stay in the DOM unchanged.
 * Number conversion is confined to chartData() and is never returned to command code. */
(function () {
  "use strict";

  function byId(id) { return document.getElementById(id); }
  function exact(value) { return value === null || value === undefined || value === "" ? "—" : String(value); }
  function clear(node) { while (node && node.firstChild) node.removeChild(node.firstChild); }
  function finite(value) {
    var number = Number(value);
    return Number.isFinite(number) ? number : null;
  }
  function chartData(rows) {
    return (Array.isArray(rows) ? rows : []).slice(-500).map(function (row) {
      var time = finite(row.t === undefined ? row.time : row.t), open = finite(row.open), high = finite(row.high);
      var low = finite(row.low), close = finite(row.close);
      if (time === null || open === null || high === null || low === null || close === null) return null;
      return { time: Math.floor(time), open: open, high: high, low: low, close: close };
    }).filter(Boolean).sort(function (a, b) { return a.time - b.time; });
  }
  function sameRevision(state, terminal) {
    var current = state && state.snapshot;
    var received = terminal && terminal.snapshot;
    if (!current || !received) return !current && (!received || received.config_revision === null);
    return String(current.snapshot_version) === String(received.snapshot_version) &&
      String(current.config_revision) === String(received.config_revision) &&
      String(current.engine_revision) === String(received.engine_revision);
  }
  function setText(id, value) { var node = byId(id); if (node) node.textContent = exact(value); }
  function td(label, value, className) {
    var cell = document.createElement("td");
    cell.setAttribute("data-label", label);
    cell.textContent = exact(value);
    if (className) cell.className = className;
    return cell;
  }
  function timeText(seconds) {
    if (typeof seconds !== "number" || !Number.isFinite(seconds)) return "—";
    try { return new Date(seconds * 1000).toLocaleString("ru-RU"); } catch (error) { return String(seconds); }
  }
  function roleText(role) {
    if (role === "ENTRY") return "Вход (ENTRY)";
    if (role === "TP") return "Тейк-профит (TP)";
    return exact(role);
  }

  function create(options) {
    var request = options.request;
    var chart = null, candles = null, resize = null, priceLines = [], currentPreview = null;
    var lastState = null, lastData = null, chartInterval = null, chartHasData = false, fitCount = 0, sequence = 0;

    function selectActivity(name, focus) {
      ["orders", "fills"].forEach(function (item) {
        var tab = byId("activity-" + item + "-tab"), panel = byId("activity-" + item + "-panel");
        var active = item === name;
        tab.setAttribute("aria-selected", active ? "true" : "false");
        tab.tabIndex = active ? 0 : -1;
        panel.hidden = !active;
      });
      if (focus) byId("activity-" + name + "-tab").focus();
    }
    ["orders", "fills"].forEach(function (name, index) {
      var tab = byId("activity-" + name + "-tab");
      tab.addEventListener("click", function () { selectActivity(name, false); });
      tab.addEventListener("keydown", function (event) {
        var next = null;
        if (event.key === "ArrowRight" || event.key === "ArrowLeft") next = index === 0 ? "fills" : "orders";
        else if (event.key === "Home") next = "orders";
        else if (event.key === "End") next = "fills";
        if (next) { event.preventDefault(); selectActivity(next, true); }
      });
    });
    selectActivity("orders", false);
    byId("chart-interval").addEventListener("change", function () { if (lastState) refresh(lastState); });

    function chartColors() {
      var style = getComputedStyle(document.documentElement);
      return {
        background: style.getPropertyValue("--surface").trim(), text: style.getPropertyValue("--muted").trim(),
        border: style.getPropertyValue("--border").trim(), buy: style.getPropertyValue("--buy").trim(),
        sell: style.getPropertyValue("--sell").trim(), accent: style.getPropertyValue("--accent").trim(),
        warn: style.getPropertyValue("--warn").trim()
      };
    }
    function ensureChart() {
      if (chart) return true;
      var target = byId("terminal-chart"), library = window.LightweightCharts;
      if (!target || !library || typeof library.createChart !== "function") return false;
      var colors = chartColors();
      chart = library.createChart(target, {
        autoSize: true,
        layout: { background: { type: "solid", color: colors.background }, textColor: colors.text,
          attributionLogo: true },
        grid: { vertLines: { color: colors.border }, horzLines: { color: colors.border } },
        rightPriceScale: { borderColor: colors.border },
        timeScale: { borderColor: colors.border, timeVisible: true, secondsVisible: false },
        crosshair: { mode: library.CrosshairMode ? library.CrosshairMode.Normal : 0 },
        localization: { locale: "ru-RU" }
      });
      candles = chart.addSeries(library.CandlestickSeries, {
        upColor: colors.buy, downColor: colors.sell, wickUpColor: colors.buy, wickDownColor: colors.sell,
        borderVisible: false, priceLineVisible: false, lastValueVisible: true
      });
      if (window.ResizeObserver) {
        resize = new ResizeObserver(function () { /* autoSize resizes the canvas and preserves the visible range */ });
        resize.observe(target);
      }
      return true;
    }
    function clearPriceLines() {
      if (!candles) return;
      priceLines.forEach(function (line) { try { candles.removePriceLine(line); } catch (error) { /* removed */ } });
      priceLines = [];
    }
    function addPriceLine(priceText, color, style, title, seen) {
      var price = finite(priceText), key = title + ":" + priceText;
      if (price === null || seen[key] || priceLines.length >= 240) return false;
      seen[key] = true;
      priceLines.push(candles.createPriceLine({ price: price, color: color, lineWidth: 1,
        lineStyle: style, axisLabelVisible: false, title: title }));
      return true;
    }
    function renderChart(market, grid, orders, includeGrid) {
      var rows = chartData(market && market.candles), empty = byId("chart-empty");
      if (!rows.length) {
        if (candles) candles.setData([]);
        chartHasData = false;
        clearPriceLines();
        empty.hidden = false;
        empty.textContent = market && market.unavailable_reason
          ? "История цены недоступна: " + market.unavailable_reason : "История цены пока пуста.";
        return;
      }
      if (!ensureChart()) {
        empty.hidden = false;
        empty.textContent = "Библиотека графика недоступна. Табличные данные продолжают обновляться.";
        return;
      }
      empty.hidden = true;
      var shouldFit = !chartHasData || market.interval !== chartInterval;
      candles.setData(rows);
      clearPriceLines();
      var boundaryCount = 0, entryCount = 0, tpCount = 0;
      if (includeGrid) {
        var colors = chartColors(), seen = {};
        (grid.levels || []).forEach(function (level) {
          if (addPriceLine(level.low, colors.border, 1, "граница", seen)) boundaryCount += 1;
          if (addPriceLine(level.high, colors.border, 1, "граница", seen)) boundaryCount += 1;
        });
        ((orders && orders.rows) || []).forEach(function (order) {
          var role = order.role === "TP" ? "TP" : "ENTRY";
          var uncertain = /(UNKNOWN|INTENT|SUBMIT|CANCEL)/.test(String(order.state || ""));
          var color = uncertain ? colors.warn : (order.side === "SELL" ? colors.sell :
            (order.side === "BUY" ? colors.buy : colors.accent));
          var style = uncertain ? 1 : (role === "TP" ? 2 : 0);
          var title = role + " · " + exact(order.state);
          if (addPriceLine(order.price, color, style, title, seen)) {
            if (role === "TP") tpCount += 1; else entryCount += 1;
          }
        });
      }
      var target = byId("terminal-chart");
      target.dataset.boundaryLines = String(boundaryCount);
      target.dataset.entryLines = String(entryCount);
      target.dataset.tpLines = String(tpCount);
      if (shouldFit) {
        chart.timeScale().fitContent();
        byId("terminal-chart").dataset.fitCount = String(++fitCount);
      }
      chartHasData = true;
      chartInterval = market.interval;
    }

    function sourceText(market) {
      var source = market && market.source;
      if (source === "lighter_robinhood_public_rest") return "Источник свечей: Robinhood Lighter, публичная история сделок";
      if (source === "demo_fixture") return "Источник свечей: детерминированная офлайн-симуляция";
      return "Источник свечей: недоступен";
    }
    function renderSummary(data, matched) {
      var grid = data.grid || {}, position = data.position || {}, orders = data.orders || {};
      if (!matched) {
        ["terminal-price-value", "terminal-position-value", "terminal-orders-value", "terminal-grid-value"].forEach(function (id) { setText(id, "—"); });
        setText("terminal-spread", "Снимок терминала обновляется");
        setText("terminal-position-detail", "Ожидается совпадение ревизий");
        setText("terminal-orders-detail", "Ожидается совпадение ревизий");
        setText("terminal-grid-detail", "Ожидается совпадение ревизий");
        return;
      }
      setText("terminal-price-value", (grid.bid === null || grid.bid === undefined) && (grid.ask === null || grid.ask === undefined)
        ? "—" : exact(grid.bid) + " / " + exact(grid.ask));
      setText("terminal-spread", "якорь " + exact(grid.anchor));
      setText("terminal-position-value", position.authoritative_net);
      setText("terminal-position-detail", "baseline " + exact(position.baseline) + " · P " + exact(position.P));
      setText("terminal-orders-value", Array.isArray(orders.rows) ? String(orders.rows.length) : "0");
      setText("terminal-orders-detail", "зафиксированный снимок v" +
        exact(orders.as_of_snapshot_version || (data.snapshot && data.snapshot.snapshot_version)));
      var levels = Array.isArray(grid.levels) ? grid.levels : [];
      var previewMatches = currentPreview && data.snapshot &&
        String(currentPreview.config_revision) === String(data.snapshot.config_revision);
      var config = previewMatches && currentPreview.config || {}, leverage = previewMatches && currentPreview.leverage || {};
      var hasConfig = config.lower_price !== null && config.lower_price !== undefined &&
        config.upper_price !== null && config.upper_price !== undefined;
      setText("terminal-grid-value", hasConfig ? exact(config.lower_price) + " — " + exact(config.upper_price) :
        (levels.length ? exact(levels[0].low) + " — " + exact(levels[levels.length - 1].high) : "—"));
      setText("terminal-grid-detail", hasConfig
        ? exact(config.cell_count) + " ячеек · по " + exact(config.order_amount_base) + " LIT · " + exact(leverage.configured) + "x"
        : (levels.length ? levels.length + " ячеек из снимка" : "Сетка ещё не зафиксирована"));
    }
    function renderOrders(orders) {
      var rows = Array.isArray(orders.rows) ? orders.rows : [], body = byId("activity-orders-body");
      clear(body);
      rows.forEach(function (row) {
        var tr = document.createElement("tr"), sideClass = row.side === "BUY" ? "side-BUY" : (row.side === "SELL" ? "side-SELL" : "");
        tr.appendChild(td("Сторона", row.side, sideClass));
        tr.appendChild(td("Роль / ячейка", roleText(row.role) + " · " + exact(row.cell_id) + " · g" + exact(row.generation)));
        tr.appendChild(td("Цена", row.price));
        tr.appendChild(td("Запрошено", row.requested));
        tr.appendChild(td("Исполнено", row.filled));
        tr.appendChild(td("Остаток", row.remaining));
        tr.appendChild(td("Состояние", row.state));
        tr.appendChild(td("Client / exchange ID", exact(row.cid) + " / " + exact(row.exchange_id || row.exchange_order_id)));
        body.appendChild(tr);
      });
      byId("activity-orders-count").textContent = String(rows.length);
      byId("activity-orders-empty").hidden = rows.length > 0;
      byId("activity-orders-wrap").hidden = rows.length === 0;
    }
    function renderFills(fills) {
      var rows = Array.isArray(fills.rows) ? fills.rows : [], body = byId("activity-fills-body");
      clear(body);
      rows.forEach(function (row) {
        var tr = document.createElement("tr"), sideClass = row.side === "BUY" ? "side-BUY" : (row.side === "SELL" ? "side-SELL" : "");
        tr.appendChild(td("Время", timeText(row.trade_at)));
        tr.appendChild(td("Сторона", row.side, sideClass));
        tr.appendChild(td("Роль / ячейка", roleText(row.role) + " · " + exact(row.cell_id) + " · g" + exact(row.generation)));
        tr.appendChild(td("Цена", row.price));
        tr.appendChild(td("Объём", row.size));
        tr.appendChild(td("Trade ID", row.trade_id));
        tr.appendChild(td("Client / exchange ID", exact(row.cid) + " / " + exact(row.exchange_order_id)));
        body.appendChild(tr);
      });
      byId("activity-fills-count").textContent = String(rows.length);
      byId("activity-fills-empty").hidden = rows.length > 0;
      byId("activity-fills-wrap").hidden = rows.length === 0;
    }
    function clearLedger() {
      renderOrders({ rows: [] }); renderFills({ rows: [] });
      setText("activity-revision", "Снимок терминала обновляется");
      setText("activity-note", "Данные скрыты до совпадения ревизий снимка состояния и терминала.");
    }
    function render(data, state) {
      lastData = data;
      var market = data.market || {}, grid = data.grid || {}, matched = sameRevision(state, data);
      renderChart(market, grid, data.orders || {}, matched);
      setText("chart-source", sourceText(market));
      var status = byId("terminal-status"), freshness = data.snapshot && data.snapshot.freshness || data.freshness || {};
      if (!matched) {
        status.dataset.state = "mismatch"; status.dataset.tone = "warn";
        status.textContent = "Снимок терминала обновляется; данные разных ревизий не смешиваются.";
      } else if (freshness.stale) {
        status.dataset.state = "stale"; status.dataset.tone = "warn";
        status.textContent = "Свечи могут быть свежими, но снимок движка устарел. Торговое состояние неизвестно.";
      } else if (market.source === "unavailable") {
        status.dataset.state = "unavailable"; status.dataset.tone = "bad";
        status.textContent = "История цены недоступна. Состояние движка и журнал показаны отдельно.";
      } else {
        status.dataset.state = "ready"; status.dataset.tone = "ok";
        status.textContent = "Свечи — цены подтверждённых сделок; bid / ask показаны отдельно.";
      }
      renderSummary(data, matched);
      if (!matched) clearLedger();
      else {
        renderOrders(data.orders || {}); renderFills(data.fills || {});
        setText("activity-revision", "Снимок v" + exact(data.snapshot && data.snapshot.snapshot_version));
        var warnings = [];
        if (data.orders && data.orders.truncated) warnings.push("список ордеров ограничен");
        if (data.fills && data.fills.truncated) warnings.push("список исполнений ограничен");
        setText("activity-note", "Заявки из журнала сетки; исполнения подтверждены историей биржи." +
          (warnings.length ? " " + warnings.join("; ") + "." : ""));
      }
    }
    async function refresh(state) {
      lastState = state;
      var mine = ++sequence;
      var interval = byId("chart-interval").value;
      var version = state && state.snapshot && state.snapshot.snapshot_version;
      var path = "/api/terminal?interval=" + encodeURIComponent(interval) + "&candle_limit=200&fill_limit=100";
      if (version !== null && version !== undefined) path += "&snapshot_version=" + encodeURIComponent(String(version));
      var result = await request(path);
      if (mine !== sequence) return;
      if (result.status === 200 && result.data) { render(result.data, state); return; }
      var status = byId("terminal-status");
      status.dataset.state = "error"; status.dataset.tone = "bad";
      status.textContent = result.network ? "Нет связи с локальным backend." : "Терминальные данные недоступны: ошибка " + result.status + ".";
      setText("chart-source", "Источник свечей: недоступен");
      renderSummary({}, false); clearLedger();
      renderChart({ candles: [], unavailable_reason: "ответ backend не получен" }, {}, {}, false);
    }
    function setPreview(preview) {
      currentPreview = preview || null;
      if (lastData) renderSummary(lastData, sameRevision(lastState, lastData));
    }
    return { refresh: refresh, setPreview: setPreview, destroy: function () {
      sequence += 1; if (resize) resize.disconnect(); if (chart) chart.remove(); chart = candles = null;
    } };
  }

  window.NeutralGridTerminal = { create: create, chartData: chartData, sameRevision: sameRevision };
}());
