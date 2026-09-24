"""AC-46 / NG-UI-002: config preview before start (sample 5–6 USDG, N=55, Q=10, 5x, caps 1000, cap 120)."""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from ngweb_fakes import sample_config, sample_rules

STATIC = Path(__file__).resolve().parents[3] / "web" / "neutral_grid" / "static"


async def _preview(web, query=""):
    await web.login()
    resp = await web.get("/api/preview" + query)
    assert resp.status == 200, await resp.text()
    return await resp.json()


@pytest.mark.asyncio
async def test_sample_preview_counts_range_caps_floors(make_web):
    web = await make_web(None)
    p = await _preview(web, "?baseline=0")
    assert p["grid"]["boundaries"] == 56 and p["grid"]["cells"] == 55
    assert p["grid"]["prices"][0] == "5.0000" and p["grid"]["prices"][-1] == "6.0000"
    assert all(isinstance(x, str) for x in p["grid"]["prices"])
    assert p["anchor"]["anchor_estimate"] == "5.4"
    assert p["sides"] == {"buy": 22, "sell": 33, "known": True}
    adm = p["admission"]
    # Q=10, min base 5 -> 1 entry + 2 TP slots per cell; cap 120 arms 40 of 55 (NG-RISK-005 sample)
    assert (adm["slots_per_cell_min"], adm["slots_per_cell_max"]) == (3, 3)
    assert (adm["armed"], adm["queued"]) == (40, 15)
    assert (adm["slots_actual"], adm["slots_reserved"], adm["slots_free"], adm["effective_cap"]) == (0, 120, 0, 120)
    assert p["baseline"] == {"value": "0", "signed": "+0", "source": "operator_input", "confirmed_in_ledger": False}
    assert p["reachable"] == {"P_min": "-330", "P_max": "220", "net_cap": "1000", "within_cap": True,
                              "baseline_used": "0"}
    assert p["gross"] == {"worst": "550", "cap": "1000", "within_cap": True}
    assert p["leverage"] == {"configured": "5", "venue_max": "10"}
    n = p["notional"]
    assert n["mark"] == "5.4" and n["gross_notional_estimate"] == "2970.0"
    assert n["net_notional_estimate"] == "1782.0" and n["margin_estimate"] == "356.40"
    assert n["available_collateral"] == "3000" and n["warning"] is None and n["blocks_exposure"] is False
    f = p["floors"]
    assert (f["tick_size"], f["size_step"], f["min_base"], f["min_notional"]) == ("0.0001", "0.1", "5", "10")
    assert f["min_valid_tp_qty_min"] == "5.0" and f["min_valid_tp_qty_max"] == "5.0"
    assert (f["freshness_s"], f["settlement_delay_s"], f["settlement_scans"]) == ("10", "5", 2)
    assert p["errors"] == [] and p["can_start"] is True
    assert p["live_confirmation_required"] is True
    assert any("40 из 55" in w for w in p["warnings"])


@pytest.mark.asyncio
async def test_baseline_330_shifts_reachable_range(make_web):
    web = await make_web(None)
    p = await _preview(web, "?baseline=330")
    assert (p["reachable"]["P_min"], p["reachable"]["P_max"]) == ("0", "550")
    assert p["baseline"]["signed"] == "+330"
    short = await (await web.get("/api/preview?baseline=-120")).json()
    assert (short["reachable"]["P_min"], short["reachable"]["P_max"]) == ("-450", "100")
    assert short["baseline"]["signed"] == "-120"


@pytest.mark.asyncio
async def test_missing_baseline_is_input_not_error(make_web):
    web = await make_web(None)
    p = await _preview(web)
    assert p["baseline"]["source"] == "missing"
    assert p["errors"] == [] and p["can_start"] is True
    assert p["reachable"]["baseline_used"] == "0"
    assert any("expected_initial_position" in w for w in p["warnings"])
    bad = await web.get("/api/preview?baseline=abc")
    assert bad.status == 400


@pytest.mark.asyncio
async def test_validation_errors_are_shown_and_block_start(make_web):
    web = await make_web(None, config=sample_config(lower_price=Decimal("5.00005")))
    p = await _preview(web, "?baseline=0")
    assert p["can_start"] is False
    assert any(e.startswith("Сетка:") and "tick" in e for e in p["errors"])

    web2 = await make_web(None, config=sample_config(order_amount_base=Decimal("10.05")))
    p2 = await _preview(web2, "?baseline=0")
    assert any(e.startswith("Размер ордера Q") for e in p2["errors"])

    web3 = await make_web(None, config=sample_config(leverage=Decimal("20")))
    p3 = await _preview(web3, "?baseline=0")
    assert any(e.startswith("Плечо") for e in p3["errors"])

    web4 = await make_web(None, config=sample_config(order_amount_base=Decimal("4")))  # full Q below min 5
    p4 = await _preview(web4, "?baseline=0")
    assert p4["can_start"] is False and p4["errors"]


@pytest.mark.asyncio
async def test_unknown_rules_and_margin_block(make_web):
    web = await make_web(None)
    web.market["rules"] = None
    p = await _preview(web, "?baseline=0")
    assert p["can_start"] is False
    assert any("RULES_UNKNOWN" in e for e in p["errors"])
    web.market["rules"] = sample_rules()
    web.market["available"] = None
    p = await (await web.get("/api/preview?baseline=0")).json()
    assert p["notional"]["blocks_exposure"] is True
    assert any("Маржа неизвестна" in w for w in p["warnings"])
    web.market["available"] = Decimal("100")
    p = await (await web.get("/api/preview?baseline=0")).json()
    assert p["notional"]["blocks_exposure"] is False and "MARGIN_SHORTFALL" in p["notional"]["warning"]


@pytest.mark.asyncio
async def test_live_mode_requires_enabled_config(make_web):
    web = await make_web(None, config=sample_config(enabled=False), mode="attach")
    p = await _preview(web, "?baseline=0")
    assert p["can_start"] is False
    assert any(e.startswith("enabled=false") for e in p["errors"])


@pytest.mark.asyncio
async def test_venue_cap_limits_admission(make_web):
    web = await make_web(None, config=sample_config(max_active_orders=60))
    p = await _preview(web, "?baseline=0")
    assert (p["admission"]["armed"], p["admission"]["queued"]) == (20, 35)
    web.market["rules"] = sample_rules(max_active_orders_venue=30)
    p = await (await web.get("/api/preview?baseline=0")).json()
    assert p["admission"]["venue_cap"] == 30
    assert any(e.startswith("Лимит ордеров") for e in p["errors"])


def test_ui_is_russian_responsive_and_has_preview_fields():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    css = (STATIC / "app.css").read_text(encoding="utf-8")
    js = (STATIC / "app.js").read_text(encoding="utf-8")
    assert '<html lang="ru">' in html and 'name="viewport"' in html
    for label in ("Превью и старт", "Подтверждение старта", "expected_initial_position", "Ячейки", "Журнал"):
        assert label in html
    for label in ("Границы / ячейки", "Первые стороны BUY / SELL", "Вооружено / в очереди",
                  "Слоты: заняты / резерв / свободны", "Достижимые P_min … P_max", "Gross худший / лимит",
                  "Плечо", "Notional / маржа (оценка)", "Минимумы площадки"):
        assert label in js
    assert "@media (max-width: 700px)" in css and "prefers-color-scheme: dark" in css
    assert ":focus-visible" in css
