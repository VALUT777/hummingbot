"""AC-48/AC-49/AC-50 in a real headless browser (Chrome over DevTools; skipped when no browser binary).

Covers what only a browser can prove: the page renders committed state honestly, ids beyond 2**53 survive
into the DOM, nothing lands in localStorage/sessionStorage/readable cookies, the token leaves the URL,
keyboard-only operation works, text contrast meets WCAG AA in light and dark schemes, and the 375 px mobile
layout has no horizontal page scroll.
"""
from __future__ import annotations

import time
from decimal import Decimal

import pytest
from ngweb_cdp import CONTRAST_JS, Browser, find_chrome
from ngweb_fakes import ACCESS_TOKEN, sample_rules, sample_snapshot
from web.neutral_grid.terminal import DemoCandleProvider, TerminalService

CHROME = find_chrome()
pytestmark = pytest.mark.skipif(CHROME is None, reason="no Chrome/Chromium binary for headless browser tests")

HUGE = str((1 << 63) + 5)
TABS = ["overview", "preview", "cells", "lookup", "journal", "access"]
CONTRAST_SELECTORS = [".brand-sub", "#state-text", ".fresh", ".hint", ".kv dt", ".kv dd", ".btn", ".btn-primary",
                      ".btn-danger", "[role=tab][aria-selected=true]", "[role=tab][aria-selected=false]",
                      ".tone-ok", ".tone-warn", ".tone-off", ".log li", ".chip-mode"]


async def _open(tmp_path, web, scheme=None):
    browser = await Browser.launch(CHROME, tmp_path / "profile")
    page = await browser.new_page()
    if scheme:
        await page.color_scheme(scheme)  # before load: no colour transition while measuring
    await page.navigate(f"{web.origin}/#auth={ACCESS_TOKEN}")
    await page.wait_for("document.getElementById('tabs') && !document.getElementById('tabs').hidden")
    return browser, page


def _snapshot_with_engine_fields(state="NORMAL"):
    snap = sample_snapshot(state)
    snap["cells"][0]["state_flags"] = ["ENTRY_LIVE", "TP_LIVE"]
    snap["summary"].update({"entry_blockers": [], "tp_blockers": [], "freezes": {}, "persistence_error": None,
                            "bootstrap": {"ready": None, "detail": None, "observed_position": "-2",
                                          "expected_initial_position": "0"}})
    return snap


@pytest.mark.asyncio
async def test_browser_truthful_state_security_and_keyboard(make_web, tmp_path):
    web = await make_web(_snapshot_with_engine_fields("NORMAL"), stale_after_s=5)
    web.gateway.live_clock = True  # a running engine commits fresh snapshots; Chrome start-up time must not matter
    browser, page = await _open(tmp_path, web)
    try:
        await page.eval("new Promise(r => setTimeout(r, 6000))")  # slower than stale_after_s, like a cold Chrome
        # token left the URL, nothing persisted client-side, the session cookie is not script-readable
        assert await page.eval("location.hash") == ""
        assert await page.eval("localStorage.length + sessionStorage.length") == 0
        assert await page.eval("document.cookie") == ""
        await page.wait_for("document.getElementById('state-text').textContent === 'Работает'")
        assert await page.eval("document.getElementById('state-badge').dataset.state") == "NORMAL"
        assert await page.eval("document.getElementById('terminal-start').hidden") is True
        assert await page.eval("document.querySelector('[data-cmd=confirm_baseline]').hidden") is True

        # exact ids beyond 2**53 in the DOM (cells table and lookup)
        await page.eval("document.getElementById('tab-cells').click()")
        await page.wait_for(f"document.getElementById('cells-body').textContent.includes('{HUGE}')")
        await page.eval("document.getElementById('tab-lookup').click()")
        await page.eval(f"document.getElementById('lookup-id').value = '{HUGE}';"
                        "document.getElementById('lookup-form').requestSubmit()")
        await page.wait_for("document.getElementById('lookup-result').textContent.includes('ENTRY ячейки 21')")

        # keyboard-only: tabs with arrows, then a Pause command through the dialog
        await page.eval("document.getElementById('tab-lookup').focus()")
        await page.key("Home")
        assert await page.eval("document.activeElement.id") == "tab-overview"
        await page.key("ArrowRight")
        assert await page.eval("document.querySelector('[role=tab][aria-selected=true]').id") == "tab-preview"
        await page.key("ArrowLeft")
        await page.key("Tab")
        assert await page.eval("document.activeElement.dataset.cmd") == "pause"
        await page.key("Enter")
        await page.wait_for("document.getElementById('cmd-dialog').open")
        await page.type("проверка клавиатуры")
        await page.key("Tab")   # -> Cancel
        await page.key("Tab")   # -> Submit
        assert await page.eval("document.activeElement.id") == "cmd-submit"
        await page.key("Enter")
        await page.wait_for("!document.getElementById('cmd-dialog').open")
        assert [c["kind"] for c in web.gateway.commands] == ["pause"]
        assert web.gateway.commands[0]["payload"] == {"reason": "проверка клавиатуры"}
        await page.wait_for("document.getElementById('pending-command').textContent.includes('в очереди')")
        # still the committed NORMAL: queueing a pause is not a pause
        assert await page.eval("document.getElementById('state-badge').dataset.state") == "NORMAL"
        web.gateway.apply(web.gateway.commands[0]["id"], "APPLIED", {"paused": True})
        web.gateway.snapshot.update(engine_state="PAUSED", engine_revision=2, committed_at=time.time())
        await page.wait_for("document.getElementById('pending-command').textContent.includes('применена')")
        await page.wait_for("document.getElementById('state-badge').dataset.state === 'PAUSED'")

        # STOP_UNCERTAIN is shown as such, never as STOPPED
        web.gateway.snapshot.update(engine_state="STOP_UNCERTAIN", committed_at=time.time(),
                                    reasons=["cancel outcome unknown"])
        await page.wait_for("document.getElementById('state-badge').dataset.state === 'STOP_UNCERTAIN'")
        assert await page.eval("document.getElementById('state-text').textContent") == "Остановка не подтверждена"

        # stale snapshot: the engine stops committing -> no current state claimed, last known kept separately
        web.gateway.live_clock = False
        web.gateway.snapshot["committed_at"] = time.time() - 60
        await page.wait_for("document.getElementById('state-badge').dataset.state === 'STALE'")
        assert "STOP_UNCERTAIN" in await page.eval("document.getElementById('state-code').textContent")
        assert await page.eval("!document.getElementById('stale-banner').hidden")
        # persistence failure the engine could not commit: shown from the uncommitted health channel
        web.ctx.health_provider = lambda: {"source": "health_file", "known": True,
                                           "persistence_error": "database or disk is full", "at": time.time()}
        await page.eval("document.getElementById('tab-overview').click()")
        await page.wait_for("document.getElementById('persistence-banner').textContent.includes('disk is full')")
        assert "не зафиксировано" in await page.eval("document.getElementById('persistence-banner').textContent")
        assert await page.eval("localStorage.length + sessionStorage.length") == 0
    finally:
        await browser.close()


@pytest.mark.asyncio
async def test_browser_terminal_hierarchy_and_activity_tabs(make_web, tmp_path):
    """The primary view is a terminal: summary, chart/risk workspace, then exact order and fill activity."""
    web = await make_web(_snapshot_with_engine_fields("NORMAL"))
    web.gateway.live_clock = True
    browser, page = await _open(tmp_path, web)
    try:
        await page.wait_for("document.getElementById('terminal-status').dataset.state !== 'loading'")
        order = await page.eval("[...document.querySelectorAll('#panel-overview [data-terminal-region]')]"
                                ".map(node => node.dataset.terminalRegion)")
        assert order[:4] == ["summary", "workspace", "activity", "diagnostics"]
        assert await page.eval("document.getElementById('terminal-chart').getAttribute('aria-label')")
        assert "TradingView" in await page.eval("document.getElementById('chart-attribution').textContent")

        # A second, keyboard-operable tablist changes only the under-chart activity surface.
        await page.eval("document.getElementById('activity-orders-tab').focus()")
        await page.key("ArrowRight")
        assert await page.eval("document.activeElement.id") == "activity-fills-tab"
        assert await page.eval("document.getElementById('activity-fills-tab').getAttribute('aria-selected')") == "true"
        assert await page.eval("document.getElementById('activity-orders-panel').hidden") is True
        assert await page.eval("document.getElementById('activity-fills-panel').hidden") is False
        assert await page.eval("NeutralGridTerminal.sameRevision("
                               "{snapshot:{config_revision:1,engine_revision:2,snapshot_version:10}},"
                               "{snapshot:{config_revision:1,engine_revision:2,snapshot_version:'9'}})") is False
    finally:
        await browser.close()


@pytest.mark.asyncio
async def test_browser_terminal_primary_start_uses_existing_confirmation(make_web, tmp_path):
    web = await make_web(_snapshot_with_engine_fields("STOPPED"))
    web.gateway.live_clock = True
    web.ctx.terminal = TerminalService(web.gateway, DemoCandleProvider(), web.ctx.identity)
    browser, page = await _open(tmp_path, web)
    try:
        await page.wait_for("!document.getElementById('terminal-start').hidden")
        assert "Продолжить сетку" in await page.eval("document.getElementById('terminal-start').textContent")
        await page.eval("document.getElementById('terminal-start').click()")
        await page.wait_for("document.getElementById('start-dialog').open")
        assert await page.eval("document.getElementById('tab-preview').getAttribute('aria-selected')") == "true"
        assert "Продолжить сохранённую сетку" in await page.eval("document.getElementById('start-title').textContent")
    finally:
        await browser.close()


@pytest.mark.asyncio
async def test_browser_terminal_renders_candles_and_exact_order_strings(make_web, tmp_path):
    snap = _snapshot_with_engine_fields("NORMAL")
    snap["snapshot_version"] = 1842
    snap["summary"].update({"anchor": "5.400000000000000001", "bid": "5.399999999999999999",
                            "ask": "5.400000000000000002"})
    snap["cells"][0]["entry"].update(price="5.381800000000000001", requested="10.000000000000000001")
    web = await make_web(snap)
    web.gateway.live_clock = True
    fill = {"dedupe_key": "trade:precise", "trade_id": "9007199254741999", "cid": "281474976710600",
            "exchange_order_id": HUGE, "grid_id": "ng-test", "cell_id": "21", "generation": 2,
            "role": "ENTRY", "side": "BUY", "size": "3.000000000000000001",
            "price": "5.351200000000000001", "trade_at": 1790250102.234, "late": False}
    web.gateway.terminal_read = lambda limit, snapshot_version=None: (web.gateway.latest_snapshot(), [fill], False)
    web.ctx.terminal = TerminalService(web.gateway, DemoCandleProvider(), web.ctx.identity)
    browser, page = await _open(tmp_path, web)
    try:
        await page.wait_for("document.getElementById('terminal-status').dataset.state === 'ready'")
        await page.wait_for("document.getElementById('chart-empty').hidden")
        assert await page.eval("document.querySelectorAll('#terminal-chart canvas').length") > 0
        assert await page.eval("document.getElementById('terminal-price-value').textContent") == \
            "5.399999999999999999 / 5.400000000000000002"
        assert "якорь 5.400000000000000001" in await page.eval("document.getElementById('terminal-spread').textContent")
        orders = await page.eval("document.getElementById('activity-orders-body').textContent")
        assert "10.000000000000000001" in orders
        assert "281474976710600" in orders and HUGE in orders
        await page.eval("document.getElementById('activity-fills-tab').click()")
        fills = await page.eval("document.getElementById('activity-fills-body').textContent")
        assert "9007199254741999" in fills and "3.000000000000000001" in fills
        assert "5.351200000000000001" in fills and HUGE in fills

        fit_count = await page.eval("document.getElementById('terminal-chart').dataset.fitCount")
        await page.eval("new Promise(r => setTimeout(r, 2500))")
        assert await page.eval("document.getElementById('terminal-chart').dataset.fitCount") == fit_count

        await page.eval("{ const s = document.getElementById('chart-interval'); s.value = '15m';"
                        "s.dispatchEvent(new Event('change')); }")
        await page.wait_for(f"document.getElementById('terminal-chart').dataset.fitCount !== '{fit_count}'")
        assert await page.eval("document.getElementById('chart-interval').value") == "15m"

        # A new configuration revision refreshes the exact preview-backed size/leverage metric once.
        original_preview = web.ctx.build_preview
        revision_two_attempts = 0

        async def flaky_preview(**kwargs):
            nonlocal revision_two_attempts
            if kwargs.get("config_revision") == 2:
                revision_two_attempts += 1
                if revision_two_attempts == 1:
                    raise RuntimeError("transient preview read")
            return await original_preview(**kwargs)

        web.ctx.build_preview = flaky_preview
        web.gateway.snapshot.update(snapshot_version=1843, config_revision=2, engine_revision=2)
        await page.wait_for("document.getElementById('revisions').textContent.includes('config r2')")
        await page.wait_for("document.getElementById('terminal-grid-detail').textContent.includes('по 10 LIT')"
                            " && document.getElementById('terminal-grid-detail').textContent.includes('5x')")
        assert revision_two_attempts >= 2
    finally:
        await browser.close()


@pytest.mark.asyncio
async def test_browser_chart_overlays_only_actual_order_rows(make_web, tmp_path):
    snap = _snapshot_with_engine_fields("NORMAL")
    snap.update(snapshot_version=21, config_revision=1, engine_revision=1)
    cell = snap["cells"][0]
    cell.update(entry_price="5.3818", tp_price="5.4000")  # planned levels are not live orders
    cell["entry"].update(price="5.3818", state="LIVE")
    cell["tp_children"] = []
    web = await make_web(snap)
    web.gateway.live_clock = True
    web.ctx.terminal = TerminalService(web.gateway, DemoCandleProvider(), web.ctx.identity)
    browser, page = await _open(tmp_path, web)
    try:
        await page.wait_for("document.getElementById('terminal-chart').dataset.entryLines === '1'")
        assert await page.eval("document.getElementById('terminal-chart').dataset.tpLines") == "0"
        assert int(await page.eval("document.getElementById('terminal-chart').dataset.boundaryLines")) > 0

        # Once a real TP child exists in the committed snapshot, exactly one TP overlay appears.
        web.gateway.snapshot["cells"][0]["tp_children"] = [{
            "cid": "281474976710699", "exchange_id": HUGE, "price": "5.4000",
            "requested": "3", "filled": "1", "remaining": "2", "state": "LIVE", "expiry_at": None,
            "side": "SELL",
        }]
        web.gateway.snapshot.update(snapshot_version=22, engine_revision=2)
        await page.wait_for("document.getElementById('terminal-chart').dataset.tpLines === '1'")
        assert await page.eval("document.getElementById('terminal-chart').dataset.entryLines") == "1"
    finally:
        await browser.close()


@pytest.mark.asyncio
async def test_browser_fresh_market_never_masks_stale_engine_snapshot(make_web, tmp_path):
    snap = _snapshot_with_engine_fields("NORMAL")
    snap["committed_at"] = time.time() - 120
    web = await make_web(snap, stale_after_s=5)
    web.ctx.terminal = TerminalService(web.gateway, DemoCandleProvider(), web.ctx.identity)
    browser, page = await _open(tmp_path, web)
    try:
        await page.wait_for("document.getElementById('terminal-status').dataset.state === 'stale'")
        assert await page.eval("document.getElementById('chart-empty').hidden") is True
        assert "снимок движка устарел" in await page.eval("document.getElementById('terminal-status').textContent")
        assert await page.eval("document.getElementById('state-badge').dataset.state") == "STALE"
    finally:
        await browser.close()


@pytest.mark.asyncio
async def test_browser_attach_shows_bound_identity_and_hides_detached_keystore_controls(make_web, tmp_path):
    web = await make_web(_snapshot_with_engine_fields("BOOTSTRAPPING"), mode="attach")
    browser, page = await _open(tmp_path, web)
    try:
        await page.eval("document.getElementById('tab-access').click()")
        await page.wait_for("document.getElementById('profile-card').hidden && "
                            "document.getElementById('unlock-card').hidden")
        assert not await page.eval("document.getElementById('credentials-status-card').hidden")
        assert await page.eval("document.getElementById('profile-form').offsetParent === null")
        assert await page.eval("document.getElementById('keystore-status').offsetParent !== null")
        credentials = await page.eval("document.getElementById('keystore-status').textContent")
        assert "lighter_perpetual_robinhood" in credentials
        assert "Аккаунт" in credentials and "7" in credentials
        assert "аккаунт 7" in await page.eval("document.getElementById('identity').textContent")

        await page.eval("document.getElementById('tab-preview').click()")
        await page.wait_for("document.getElementById('preview-prices').children.length > 0 && "
                            "!document.getElementById('start-open').disabled")
        await page.eval("document.getElementById('start-open').click()")
        await page.wait_for("document.getElementById('start-dialog').open")
        start_note = await page.eval("document.getElementById('start-mode-note').textContent")
        assert "уже запущенному движку" in start_note
        assert "аккаунт 7" in start_note and "LIT-USDG" in start_note
        assert "выбранного профиля" not in start_note
    finally:
        await browser.close()


@pytest.mark.asyncio
async def test_browser_attach_refreshes_identity_from_new_committed_snapshot(make_web, tmp_path):
    web = await make_web(_snapshot_with_engine_fields("BOOTSTRAPPING"), mode="attach")
    current = {"grid_id": None, "config_error": "engine_config отсутствует"}
    web.ctx.identity_provider = lambda: dict(current)
    browser, page = await _open(tmp_path, web)
    try:
        initial_identity = await page.eval("document.getElementById('identity').textContent")
        assert initial_identity == "Привязка неизвестна"
        current.clear()
        current.update({"grid_id": "ng-engine-grid", "connector_name": "lighter_perpetual_robinhood",
                        "trading_pair": "LIT-USDG", "account_index": "7"})
        web.gateway.snapshot.update(committed_at=time.time(), engine_revision=2)
        # Open Preview immediately, before the periodic /api/state refresh.  The confirmation must use the
        # identity bound into this exact validated preview rather than the older session identity.
        await page.eval("document.getElementById('tab-preview').click()")
        await page.wait_for("document.getElementById('preview-prices').children.length > 0 && "
                            "!document.getElementById('start-open').disabled")
        await page.eval("document.getElementById('start-open').click()")
        await page.wait_for("document.getElementById('start-dialog').open")
        note = await page.eval("document.getElementById('start-mode-note').textContent")
        assert "аккаунт 7" in note and "LIT-USDG" in note
        await page.wait_for("document.getElementById('identity').textContent.includes('lighter_perpetual_robinhood')")
    finally:
        await browser.close()


@pytest.mark.asyncio
async def test_browser_external_close_flow_and_stopped_start_wording(make_web, tmp_path):
    snap = _snapshot_with_engine_fields("STOPPED_WITH_INVENTORY")
    snap["summary"]["authoritative_net"] = "0"
    snap["summary"]["external_close_candidate"] = {
        "proof_id": "a" * 64, "blockers": [], "observed_position": "0",
        "cycle": {"grid_id": "ng-test", "cell_id": "11", "generation": "2", "E": "10", "X": "0",
                  "external_settled": "0", "proposed_settlement": "10", "open_after": "0"},
        "trades": [{"inbox_id": "154", "side": "SELL", "quantity": "4.38", "price": "5.25"},
                   {"inbox_id": "155", "side": "SELL", "quantity": "5.62", "price": "5.26"}],
        "terminal_order": {"inbox_id": "156", "side": "SELL", "reduce_only": True, "final": True,
                           "filled": "10"},
    }
    web = await make_web(snap)
    web.gateway.live_clock = True
    browser, page = await _open(tmp_path, web)
    try:
        await page.wait_for("document.getElementById('state-badge').dataset.state === 'STOPPED_WITH_INVENTORY'")
        assert await page.eval("document.querySelector('[data-cmd=resume]').disabled") is True
        assert await page.eval("document.getElementById('terminal-start').hidden") is False
        assert "Продолжить сетку" in await page.eval("document.getElementById('terminal-start').textContent")
        await page.eval("document.getElementById('external-close-open').click()")
        await page.wait_for("document.getElementById('cmd-dialog').open")
        assert await page.eval("document.getElementById('f-action').value") == "settle_external_close"
        acknowledgement = await page.eval("document.getElementById('f-ack-text').textContent")
        assert "внешнее закрытие" in acknowledgement and "fills, CID, P&L и история сохранятся" in acknowledgement
        assert "не меняет обязательства" not in acknowledgement
        evidence = await page.eval("document.getElementById('f-external-close').textContent")
        assert all(text in evidence for text in ("E=10", "X биржа=0", "S вручную=10", "4.38", "5.62",
                                                 "reduce-only=да", "бот останется остановлен")), evidence
        await page.eval("document.getElementById('f-note').value='ручное закрытие сверено';"
                        "document.getElementById('f-ack').checked=true;"
                        "document.getElementById('f-confirmation').value="
                        "'SETTLE EXTERNAL CLOSE ng-test AT FLAT 0'")
        await page.eval("document.getElementById('cmd-submit').click()")
        await page.wait_for("!document.getElementById('cmd-dialog').open")
        [row] = web.gateway.commands
        assert row["payload"]["proof_id"] == "a" * 64

        await page.eval("document.getElementById('tab-preview').click()")
        await page.wait_for("document.getElementById('preview-prices').children.length > 0 && "
                            "!document.getElementById('start-open').disabled")
        await page.eval("document.getElementById('start-open').click()")
        await page.wait_for("document.getElementById('start-dialog').open")
        assert "Продолжить сохранённую сетку" in await page.eval("document.getElementById('start-title').textContent")
        wording = await page.eval("document.getElementById('start-ack-baseline-text').textContent")
        assert "исходный baseline" in wording and "позиция на бирже равна" not in wording
    finally:
        await browser.close()


@pytest.mark.asyncio
async def test_browser_external_close_dialog_never_submits_an_unseen_replacement_proof(make_web, tmp_path):
    snap = _snapshot_with_engine_fields("STOPPED")
    candidate = {
        "proof_id": "a" * 64, "blockers": [], "observed_position": "0",
        "cycle": {"grid_id": "ng-test", "cell_id": "11", "generation": "2", "E": "10", "X": "0",
                  "external_settled": "0", "proposed_settlement": "10", "open_after": "0"},
        "trades": [{"inbox_id": "154", "side": "SELL", "quantity": "4.38", "price": "5.25"},
                   {"inbox_id": "155", "side": "SELL", "quantity": "5.62", "price": "5.26"}],
        "terminal_order": {"inbox_id": "156", "side": "SELL", "reduce_only": True, "final": True,
                           "filled": "10"},
    }
    snap["summary"]["external_close_candidate"] = candidate
    web = await make_web(snap)
    web.gateway.live_clock = True
    browser, page = await _open(tmp_path, web)
    try:
        await page.eval("document.getElementById('external-close-open').click()")
        await page.wait_for("document.getElementById('cmd-dialog').open")
        assert await page.eval("document.getElementById('f-action').value") == "settle_external_close"
        assert "4.38" in await page.eval("document.getElementById('f-external-close').textContent")

        replacement = dict(candidate, proof_id="b" * 64,
                           trades=[dict(candidate["trades"][0], quantity="4"),
                                   dict(candidate["trades"][1], quantity="6")])
        web.gateway.snapshot["summary"]["external_close_candidate"] = replacement
        await page.eval("new Promise(r => setTimeout(r, 3000))")  # state poll updates S.state behind the open dialog
        await page.eval("document.getElementById('f-note').value='проверено';"
                        "document.getElementById('f-ack').checked=true;"
                        "document.getElementById('f-confirmation').value="
                        "'SETTLE EXTERNAL CLOSE ng-test AT FLAT 0';"
                        "document.getElementById('cmd-submit').click()")
        await page.wait_for("document.getElementById('cmd-error').textContent.includes('409')")
        assert web.gateway.commands == []
        refreshed = await page.eval("document.getElementById('f-external-close').textContent")
        assert "SELL 4 LIT" in refreshed and "SELL 6 LIT" in refreshed
    finally:
        await browser.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("scheme", ["light", "dark"])
async def test_browser_contrast_and_mobile_layout(make_web, tmp_path, scheme):
    web = await make_web(_snapshot_with_engine_fields("PAUSED"))
    web.gateway.live_clock = True
    browser, page = await _open(tmp_path, web, scheme)
    try:
        assert await page.eval(f"matchMedia('(prefers-color-scheme: {scheme})').matches") is True
        await page.wait_for("document.getElementById('state-badge').dataset.state === 'PAUSED'")
        ratios = await page.eval(CONTRAST_JS.replace("SELECTORS", repr(CONTRAST_SELECTORS)))
        assert len(ratios) >= 12, ratios
        low = {k: v for k, v in ratios.items() if v < 4.5}
        assert not low, f"WCAG AA contrast below 4.5:1 in {scheme}: {low}"
        await page.viewport(375, 812, mobile=True)
        for tab in TABS:
            await page.eval(f"document.getElementById('tab-{tab}').click()")
            await page.wait_for(f"!document.getElementById('panel-{tab}').hidden")
            await page.eval("new Promise(r => setTimeout(r, 300))")
            width = await page.eval("document.documentElement.scrollWidth")
            assert width <= 375, f"horizontal page scroll on tab {tab}: {width}px"
        await page.eval("document.getElementById('tab-cells').click()")
        await page.wait_for("document.querySelectorAll('#cells-body tr').length > 0")
        assert await page.eval("getComputedStyle(document.querySelector('#cells-body tr')).display") == "block"
        await page.screenshot(tmp_path / f"cells-mobile-{scheme}.png")
    finally:
        await browser.close()


async def _submit_dialog_until_queued(page, dialog_id, submit_id, error_id, attempts=6):
    """Submit an open command dialog; a 409 (engine moved on) re-opens it with fresh revisions -> resubmit."""
    for _ in range(attempts):
        await page.eval(f"document.getElementById('{submit_id}').click()")
        await page.wait_for(f"!document.getElementById('{dialog_id}').open || "
                            f"document.getElementById('{error_id}').textContent.length > 0", timeout=20)
        if not await page.eval(f"document.getElementById('{dialog_id}').open"):
            return
        assert "409" in await page.eval(f"document.getElementById('{error_id}').textContent")
    raise AssertionError("command kept conflicting")


async def _demo_click(page, prefix):
    await page.eval("[...document.querySelectorAll('#demo-actions button')]"
                    f".find(b => b.textContent.startsWith('{prefix}')).click()")
    await page.wait_for("document.getElementById('demo-result').textContent.startsWith('Симулятор')")


@pytest.mark.asyncio
@pytest.mark.timeout(240)
async def test_browser_flow_on_offline_demo_engine(tmp_path):
    """AC-50: the real engine + FakeExchange through the UI only (dialogs, demo buttons, tabs)."""
    import types

    from aiohttp.test_utils import TestClient, TestServer

    from web.neutral_grid import runtime
    from web.neutral_grid.server import create_app

    args = types.SimpleNamespace(data_dir=tmp_path / "demo", host="127.0.0.1", stale_after=15.0, allowed_host=[])
    bundle = await runtime.build_demo(args)
    client = TestClient(TestServer(create_app(bundle.context), host="127.0.0.1"))
    await client.start_server()
    browser = await Browser.launch(CHROME, tmp_path / "profile")
    try:
        page = await browser.new_page()
        await page.navigate(f"http://127.0.0.1:{client.port}/#auth={bundle.context.access.token}")
        await page.wait_for("document.getElementById('state-badge').dataset.state === 'BOOTSTRAPPING'")
        assert "Старт" in await page.eval("document.getElementById('engine-note').textContent")
        # Start through the confirmation dialog, keyboard only
        await page.eval("document.getElementById('tab-preview').click()")
        await page.wait_for("document.getElementById('preview-cards').textContent.includes('56 / 55')")
        await page.eval("document.getElementById('start-open').click()")
        await page.wait_for("document.getElementById('start-dialog').open")
        await page.eval("document.getElementById('start-baseline').focus()")
        await page.type("0")
        for _ in range(2):
            await page.key("Tab")
            await page.key(" ")
        assert await page.eval("document.getElementById('start-submit').disabled") is False
        await _submit_dialog_until_queued(page, "start-dialog", "start-submit", "start-error")
        await page.wait_for("document.getElementById('pending-command').textContent.includes('применена')", 30)
        # confirm the baseline once the engine says the bootstrap cut is ready
        await page.wait_for("document.getElementById('engine-note').textContent.includes('подтвердите baseline')", 60)
        await page.eval("document.querySelector('[data-cmd=confirm_baseline]').click()")
        await page.eval("document.getElementById('f-baseline').value = '0';"
                        "document.getElementById('f-confirm').checked = true")
        await _submit_dialog_until_queued(page, "cmd-dialog", "cmd-submit", "cmd-error")
        await page.wait_for("document.getElementById('state-badge').dataset.state === 'NORMAL'", 60)
        # partial entry 3 + 3 -> entry still live AND TP 6 live, visible in the cells table
        await _demo_click(page, "Частично исполнить ближайший вход")
        await page.eval("new Promise(r => setTimeout(r, 1500))")
        await _demo_click(page, "Частично исполнить ближайший вход")
        await page.eval("document.getElementById('tab-cells').click()")
        await page.eval("const s = document.getElementById('cells-state'); s.value = 'TP_LIVE';"
                        "s.dispatchEvent(new Event('change'))")
        await page.wait_for("(() => { const t = document.getElementById('cells-body').textContent;"
                            " if (!t.includes('ENTRY_LIVE + TP_LIVE')) { const s = document.getElementById('cells-state');"
                            " s.dispatchEvent(new Event('change')); return false; } return t.includes('6 / 0 / 6'); })()",
                            60, interval=1.0)
        # dust: visible as a DUST cell with the exact remainder
        await page.eval("document.getElementById('tab-overview').click()")
        await _demo_click(page, "Пыль")
        await page.wait_for("document.getElementById('cell-legend').textContent.includes('пыль')", 90)
        # pause / resume
        await page.eval("document.querySelector('[data-cmd=pause]').click()")
        await _submit_dialog_until_queued(page, "cmd-dialog", "cmd-submit", "cmd-error")
        await page.wait_for("document.getElementById('state-badge').dataset.state === 'PAUSED'", 30)
        await page.eval("document.querySelector('[data-cmd=resume]').click()")
        await _submit_dialog_until_queued(page, "cmd-dialog", "cmd-submit", "cmd-error")
        await page.wait_for("document.getElementById('pending-command').textContent.includes('Продолжить')"
                            " && !document.getElementById('pending-command').textContent.includes('в очереди')", 30)
        # history lag becomes visible in the history card
        await _demo_click(page, "Задержка истории")
        await _demo_click(page, "Частично исполнить ближайший вход")
        # >= 10 s: only the injected 30 s lag gets there (normal poll coalescing stays well below)
        await page.wait_for("(() => { const dd = [...document.querySelectorAll('#history-kv dd')][2];"
                            " return dd && /^([1-9][0-9]+ с|[0-9]+ мин)/.test(dd.textContent); })()", 30)
        await _demo_click(page, "Убрать задержку")
        # stop with cancels that never prove terminal -> STOP_UNCERTAIN, never STOPPED
        await _demo_click(page, "Отмены")
        await page.eval("document.querySelector('[data-cmd=stop]').click()")
        await _submit_dialog_until_queued(page, "cmd-dialog", "cmd-submit", "cmd-error")
        await page.wait_for("document.getElementById('state-badge').dataset.state === 'STOP_UNCERTAIN'", 60)
        assert await page.eval("document.getElementById('state-text').textContent") == "Остановка не подтверждена"
        # mobile table with real data: no horizontal page scroll
        await page.viewport(375, 812, mobile=True)
        await page.eval("document.getElementById('tab-cells').click()")
        await page.wait_for("document.querySelectorAll('#cells-body tr').length > 0")
        assert await page.eval("document.documentElement.scrollWidth") <= 375
        assert await page.eval("localStorage.length + sessionStorage.length") == 0
        await page.screenshot(tmp_path / "demo-mobile.png")
    finally:
        await browser.close()
        await client.close()
        await bundle.close()


@pytest.mark.asyncio
async def test_browser_409_is_reissued_by_one_operator_click_never_automatically(make_web, tmp_path):
    """Orchestrator (c): a stale revision gives 409 + fresh state; inputs survive, one click re-issues with the
    fresh revisions, and nothing is resubmitted on its own."""
    web = await make_web(_snapshot_with_engine_fields("NORMAL"))
    web.gateway.live_clock = True
    browser, page = await _open(tmp_path, web)
    try:
        await page.wait_for("document.getElementById('state-badge').dataset.state === 'NORMAL'")
        await page.eval("document.querySelector('[data-cmd=stop]').click()")
        await page.wait_for("document.getElementById('cmd-dialog').open")
        await page.eval("document.getElementById('f-reason').value = 'плановая остановка'")
        web.gateway.snapshot["engine_revision"] = 2  # the engine commits a real transition meanwhile
        await page.eval("document.getElementById('cmd-submit').click()")
        await page.wait_for("document.getElementById('cmd-error').textContent.includes('409')")
        assert await page.eval("document.getElementById('cmd-dialog').open") is True
        assert await page.eval("document.getElementById('f-reason').value") == "плановая остановка"
        assert "engine r2" in await page.eval("document.getElementById('cmd-context').textContent")
        await page.eval("new Promise(r => setTimeout(r, 3000))")
        assert web.gateway.commands == []  # never re-sent automatically
        await page.eval("document.getElementById('cmd-submit').click()")  # the single operator click
        await page.wait_for("!document.getElementById('cmd-dialog').open")
        [row] = web.gateway.commands
        assert row["kind"] == "stop" and row["expected_engine_revision"] == 2
        assert row["payload"] == {"reason": "плановая остановка"}

        # Start: a revision-only conflict keeps B and both acknowledgements -> one click with the fresh preview
        web.gateway.snapshot.update(engine_state="STOPPED", engine_revision=3)
        web.gateway.commands.clear()
        await page.eval("document.getElementById('tab-preview').click()")
        await page.wait_for("document.getElementById('preview-cards').textContent.includes('56 / 55')")
        await page.eval("document.getElementById('start-open').click()")
        await page.wait_for("document.getElementById('start-dialog').open")
        await page.eval("document.getElementById('start-baseline').value = '0';"
                        "document.getElementById('start-ack-baseline').checked = true;"
                        "document.getElementById('start-ack-risk').checked = true;"
                        "document.getElementById('start-baseline').dispatchEvent(new Event('input'))")
        web.gateway.snapshot["engine_revision"] = 4
        await page.eval("document.getElementById('start-submit').click()")
        await page.wait_for("document.getElementById('start-error').textContent.includes('409')")
        assert await page.eval("document.getElementById('start-ack-risk').checked") is True
        assert await page.eval("document.getElementById('start-baseline').value") == "0"
        assert web.gateway.commands == []
        await page.eval("document.getElementById('start-submit').click()")
        await page.wait_for("!document.getElementById('start-dialog').open")
        [start] = web.gateway.commands
        assert start["kind"] == "start" and start["expected_engine_revision"] == 4
        # material change (runtime rules) behind the conflict: the risk acknowledgement must be redone
        web.gateway.snapshot.update(engine_state="STOPPED", engine_revision=5)
        web.gateway.commands.clear()
        await page.eval("document.getElementById('tab-overview').click(); document.getElementById('tab-preview').click()")
        await page.wait_for("document.getElementById('preview-cards').textContent.includes('56 / 55')")
        await page.eval("document.getElementById('start-open').click()")
        await page.wait_for("document.getElementById('start-dialog').open")
        await page.eval("document.getElementById('start-baseline').value = '0';"
                        "document.getElementById('start-ack-baseline').checked = true;"
                        "document.getElementById('start-ack-risk').checked = true;"
                        "document.getElementById('start-baseline').dispatchEvent(new Event('input'))")
        web.market["rules"] = sample_rules(min_base=Decimal("4"))
        web.gateway.snapshot["engine_revision"] = 6
        await page.eval("document.getElementById('start-submit').click()")
        await page.wait_for("document.getElementById('start-error').textContent.includes('409')")
        assert await page.eval("document.getElementById('start-ack-risk').checked") is False
        assert await page.eval("document.getElementById('start-submit').disabled") is True
        assert web.gateway.commands == []
    finally:
        await browser.close()


@pytest.mark.asyncio
async def test_browser_round3_redaction_and_extended_audits(make_web, tmp_path):
    """C1: no auth token in the DOM; D2-17: extended audits offered only when applicable, with typed phrase."""
    from test_ngweb_round3 import SECRET, _cid_frozen_snapshot, _leaky_snapshot

    leaky = _leaky_snapshot()
    web = await make_web(leaky)
    web.gateway.live_clock = True
    browser, page = await _open(tmp_path, web)
    try:
        await page.wait_for("document.getElementById('state-badge').dataset.state === 'DEGRADED'")
        for tab in TABS[:3]:
            await page.eval(f"document.getElementById('tab-{tab}').click()")
            await page.eval("new Promise(r => setTimeout(r, 400))")
            assert SECRET not in await page.eval("document.body.innerText")
        # no CID freeze and unpublished mutation blockers: extended actions are not offered
        await page.eval("document.getElementById('tab-overview').click();"
                        "document.querySelector('[data-cmd=baseline_audit]').click()")
        await page.wait_for("document.getElementById('cmd-dialog').open")
        options = await page.eval("[...document.querySelectorAll('#f-action option')].map(o => o.value)")
        assert "retire_colliding_cid" not in options and "migrate_grid" not in options
        await page.eval("document.getElementById('cmd-dialog').close()")
        # CID freeze published: retire offered, CID prefilled, exact phrase required and shown
        web.gateway.snapshot = _cid_frozen_snapshot()
        await page.wait_for("document.getElementById('state-badge').dataset.state === 'FROZEN'")
        await page.eval("document.querySelector('[data-cmd=baseline_audit]').click()")
        await page.wait_for("document.getElementById('cmd-dialog').open")
        options = await page.eval("[...document.querySelectorAll('#f-action option')].map(o => o.value)")
        assert "retire_colliding_cid" in options and "migrate_grid" not in options
        await page.eval("{ const s = document.getElementById('f-action'); s.value = 'retire_colliding_cid';"
                        "s.dispatchEvent(new Event('change')); }")
        assert await page.eval("document.getElementById('f-cid').value") == "281474976710655"
        assert "СПИСАТЬ CID 281474976710655" in await page.eval(
            "document.getElementById('f-confirmation-hint').textContent")
        await page.eval("document.getElementById('f-note').value = 'чужой ордер';"
                        "document.getElementById('f-ack').checked = true;"
                        "document.getElementById('f-confirmation').value = 'СПИСАТЬ CID 281474976710655';"
                        "document.getElementById('cmd-submit').click()")
        await page.wait_for("!document.getElementById('cmd-dialog').open")
        [row] = web.gateway.commands
        assert row["payload"]["action"] == "retire_colliding_cid" and row["payload"]["cid"] == "281474976710655"
        web.gateway.apply(row["id"], "APPLIED", {"retired_cid": "281474976710655", "cleared": "CID_ALLOCATION"})
        await page.wait_for("document.getElementById('pending-command').textContent.includes('retired_cid')")
        # quiescent grid published: migrate offered with its own phrase
        snap = _cid_frozen_snapshot()
        snap["summary"]["grid_mutation_blockers"] = []
        web.gateway.snapshot = snap
        await page.eval("new Promise(r => setTimeout(r, 2500))")
        await page.eval("document.querySelector('[data-cmd=baseline_audit]').click()")
        await page.wait_for("document.getElementById('cmd-dialog').open")
        options = await page.eval("[...document.querySelectorAll('#f-action option')].map(o => o.value)")
        assert "migrate_grid" in options
        await page.eval("{ const s = document.getElementById('f-action'); s.value = 'migrate_grid';"
                        "s.dispatchEvent(new Event('change')); }")
        assert "МИГРАЦИЯ СЕТКИ ng-test" in await page.eval("document.getElementById('f-confirmation-hint').textContent")
    finally:
        await browser.close()


@pytest.mark.asyncio
async def test_browser_history_conflict_ack_bound_to_viewed_set(make_web, tmp_path):
    """M1: conflict versions rendered exactly; the ack sends the viewed set id + explicit pick; a changed set
    shows the fresh set and needs a new operator click (never re-sent)."""
    from test_ngweb_conflicts import FP_X, FP_Y, FP_Z, ORDER_ID, SET_1, SET_2, TRADE_ID, conflict_snapshot

    web = await make_web(conflict_snapshot())
    web.gateway.live_clock = True
    browser, page = await _open(tmp_path, web)
    try:
        await page.wait_for("!document.getElementById('conflicts-card').hidden")
        card = await page.eval("document.getElementById('conflicts-card').textContent")
        assert TRADE_ID in card and ORDER_ID in card and SET_1 in card and "зафиксирована" in card
        assert "конфликт журнала" in card and "ячейка 21" in card
        await page.eval("document.querySelector('[data-cmd=baseline_audit]').click()")
        await page.wait_for("document.getElementById('cmd-dialog').open")
        await page.eval("{ const s = document.getElementById('f-action'); s.value = 'ack_history_conflict';"
                        "s.dispatchEvent(new Event('change')); }")
        radios = await page.eval("[...document.querySelectorAll('#f-conflicts input[type=radio]')].map(r => r.value)")
        assert radios == [FP_X, FP_Y]  # only the key without a committed version needs a pick
        assert f"ПРИНЯТЬ НАБОР {SET_1}" in await page.eval(
            "document.getElementById('f-confirmation-hint').textContent")
        await page.eval(f"document.getElementById('acc-1-{FP_Y}').checked = true;"
                        "document.getElementById('f-note').value = 'сверено';"
                        "document.getElementById('f-ack').checked = true;"
                        f"document.getElementById('f-confirmation').value = 'ПРИНЯТЬ НАБОР {SET_1}'")
        # a new contradiction appears before the click
        web.gateway.snapshot = conflict_snapshot(set_id=SET_2, extra_version=True)
        await page.eval("document.getElementById('cmd-submit').click()")
        await page.wait_for("document.getElementById('cmd-error').textContent.includes('409')")
        assert web.gateway.commands == []
        assert SET_2 in await page.eval("document.getElementById('f-conflicts').textContent")
        assert await page.eval("document.querySelectorAll('#f-conflicts input[type=radio]:checked').length") == 0
        assert await page.eval("document.getElementById('f-confirmation').value") == ""
        await page.eval("new Promise(r => setTimeout(r, 2500))")
        assert web.gateway.commands == []  # never re-sent on its own
        await page.eval(f"document.getElementById('acc-1-{FP_Z}').checked = true;"
                        f"document.getElementById('f-confirmation').value = 'ПРИНЯТЬ НАБОР {SET_2}';"
                        "document.getElementById('cmd-submit').click()")
        await page.wait_for("!document.getElementById('cmd-dialog').open")
        [row] = web.gateway.commands
        assert row["payload"]["conflict_set_id"] == SET_2
        assert row["payload"]["accepted"] == {f"order:{ORDER_ID}": FP_Z}
        # engine-side refusal is shown as such
        web.gateway.apply(row["id"], "REJECTED", {"error": "CONFLICT_SET_CHANGED"})
        await page.wait_for("document.getElementById('pending-command').textContent.includes('аудит НЕ выполнен')")
        # drill-down shows the conflict versions for a trade id beyond 2**53
        await page.eval("document.getElementById('tab-lookup').click();"
                        f"document.getElementById('lookup-id').value = '{TRADE_ID}';"
                        "document.getElementById('lookup-form').requestSubmit()")
        await page.wait_for("document.getElementById('lookup-result').textContent.includes('Конфликт истории')")
        assert TRADE_ID in await page.eval("document.getElementById('lookup-result').textContent")
    finally:
        await browser.close()
