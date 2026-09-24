"""AC-48/AC-49/AC-50 in a real headless browser (Chrome over DevTools; skipped when no browser binary).

Covers what only a browser can prove: the page renders committed state honestly, ids beyond 2**53 survive
into the DOM, nothing lands in localStorage/sessionStorage/readable cookies, the token leaves the URL,
keyboard-only operation works, text contrast meets WCAG AA in light and dark schemes, and the 375 px mobile
layout has no horizontal page scroll.
"""
from __future__ import annotations

import time

import pytest
from conftest import ACCESS_TOKEN
from ngweb_cdp import CONTRAST_JS, Browser, find_chrome
from ngweb_fakes import sample_snapshot

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
    browser, page = await _open(tmp_path, web)
    try:
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

        # stale snapshot: no current state claimed, last known kept separately
        web.gateway.snapshot["committed_at"] = time.time() - 60
        await page.wait_for("document.getElementById('state-badge').dataset.state === 'STALE'")
        assert "STOP_UNCERTAIN" in await page.eval("document.getElementById('state-code').textContent")
        assert await page.eval("!document.getElementById('stale-banner').hidden")
        assert await page.eval("localStorage.length + sessionStorage.length") == 0
    finally:
        await browser.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("scheme", ["light", "dark"])
async def test_browser_contrast_and_mobile_layout(make_web, tmp_path, scheme):
    web = await make_web(_snapshot_with_engine_fields("PAUSED"))
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
