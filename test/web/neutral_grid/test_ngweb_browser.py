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
