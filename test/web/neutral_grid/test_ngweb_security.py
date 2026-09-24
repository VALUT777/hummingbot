"""AC-49 / NG-UI-001: loopback default, session, CSRF, Origin/Host, no secret readback or leakage."""
from __future__ import annotations

import logging
import re
import secrets
from pathlib import Path

import pytest
from ngweb_fakes import ACCESS_TOKEN, sample_snapshot

from web.neutral_grid.keystore import KeystoreService
from web.neutral_grid.security import BindRefused, check_bind, is_loopback_host

STATIC = Path(__file__).resolve().parents[3] / "web" / "neutral_grid" / "static"


def test_default_bind_is_loopback_and_public_bind_refused():
    assert check_bind("127.0.0.1", allow_non_loopback=False) is None
    assert check_bind("::1", allow_non_loopback=False) is None
    assert check_bind("localhost", allow_non_loopback=False) is None
    for host in ("0.0.0.0", "192.168.1.10", "::", "example.com"):
        assert not is_loopback_host(host)
        with pytest.raises(BindRefused):
            check_bind(host, allow_non_loopback=False)
    warning = check_bind("0.0.0.0", allow_non_loopback=True)
    assert "SSH" in warning and "TLS" in warning


@pytest.mark.asyncio
async def test_no_session_is_401(make_web):
    web = await make_web(sample_snapshot())
    for path in ("/api/state", "/api/preview", "/api/cells", "/api/commands", "/api/keystore", "/api/audit",
                 "/api/lookup?id=1", "/api/session"):
        resp = await web.get(path)
        assert resp.status == 401, path
    resp = await web.post("/api/commands", {"kind": "pause"}, csrf=False)
    assert resp.status == 401
    assert web.gateway.enqueue_calls == 0


@pytest.mark.asyncio
async def test_login_requires_access_token_and_sets_strict_httponly_cookie(make_web):
    web = await make_web(sample_snapshot())
    bad = await web.post("/api/login", {"token": "nope"}, csrf=False)
    assert bad.status == 401
    ok = await web.post("/api/login", {"token": ACCESS_TOKEN}, csrf=False)
    assert ok.status == 200
    cookie = ok.headers["Set-Cookie"]
    assert "HttpOnly" in cookie and "SameSite=Strict" in cookie and "Path=/" in cookie
    assert "Max-Age" not in cookie and "expires" not in cookie.lower()  # session cookie only
    body = await ok.json()
    assert set(body) == {"csrf_token", "mode"}
    assert ACCESS_TOKEN not in await ok.text()


@pytest.mark.asyncio
async def test_login_bruteforce_lockout(make_web):
    web = await make_web(sample_snapshot())
    for _ in range(10):
        await web.post("/api/login", {"token": "wrong"}, csrf=False)
    resp = await web.post("/api/login", {"token": ACCESS_TOKEN}, csrf=False)
    assert resp.status == 429


@pytest.mark.asyncio
async def test_missing_or_invalid_csrf_is_403(make_web):
    web = await make_web(sample_snapshot())
    await web.login()
    body = {"kind": "pause", "idempotency_key": "k" * 20, "expected_config_revision": 1,
            "expected_engine_revision": 1, "payload": {}}
    missing = await web.post("/api/commands", body, csrf=False)
    assert missing.status == 403 and (await missing.json())["error"] == "csrf"
    wrong = await web.post("/api/commands", body, csrf=False, headers={"X-CSRF-Token": "forged"})
    assert wrong.status == 403
    for path, payload in (("/api/keystore/unlock", {"password": "x"}), ("/api/keystore/select", {"profile": "p"}),
                          ("/api/logout", {})):
        resp = await web.post(path, payload, csrf=False)
        assert resp.status == 403, path
    assert web.gateway.enqueue_calls == 0


@pytest.mark.asyncio
async def test_bad_or_missing_origin_is_403(make_web):
    web = await make_web(sample_snapshot())
    await web.login()
    body = {"kind": "pause", "idempotency_key": "k" * 20, "expected_config_revision": 1,
            "expected_engine_revision": 1, "payload": {}}
    for origin in ("http://evil.example", "null", f"http://127.0.0.1:{web.client.port + 1}",
                   f"https://127.0.0.1:{web.client.port}", None):
        resp = await web.post("/api/commands", body, origin=origin)
        assert resp.status == 403, origin
        assert (await resp.json())["error"] == "bad_origin"
    # login itself is also Origin-protected
    resp = await web.post("/api/login", {"token": ACCESS_TOKEN}, csrf=False, origin="http://evil.example")
    assert resp.status == 403
    assert web.gateway.enqueue_calls == 0


@pytest.mark.asyncio
async def test_localhost_origin_alias_is_accepted(make_web):
    web = await make_web(sample_snapshot())
    await web.login()
    resp = await web.command("pause", "k" * 20)
    assert resp.status == 202
    resp = await web.post("/api/commands", {"kind": "pause", "idempotency_key": "j" * 20,
                                            "expected_config_revision": 1, "expected_engine_revision": 1},
                          origin=f"http://localhost:{web.client.port}")
    assert resp.status == 202


@pytest.mark.asyncio
async def test_foreign_host_header_is_403_dns_rebinding(make_web):
    web = await make_web(sample_snapshot())
    resp = await web.get("/", headers={"Host": f"attacker.example:{web.client.port}"})
    assert resp.status == 403
    resp = await web.get("/api/health", headers={"Host": "attacker.example"})
    assert resp.status == 403
    ok = await web.get("/api/health")
    assert ok.status == 200


@pytest.mark.asyncio
async def test_cross_site_get_and_non_json_post_rejected(make_web):
    web = await make_web(sample_snapshot())
    await web.login()
    resp = await web.get("/api/state", headers={"Sec-Fetch-Site": "cross-site"})
    assert resp.status == 403
    form = await web.post("/api/commands", None, raw="kind=pause", content_type="application/x-www-form-urlencoded")
    assert form.status == 415
    assert web.gateway.enqueue_calls == 0


@pytest.mark.asyncio
async def test_security_headers_and_no_store(make_web):
    web = await make_web(sample_snapshot())
    await web.login()
    page = await web.get("/")
    assert page.status == 200
    csp = page.headers["Content-Security-Policy"]
    assert "default-src 'self'" in csp and "script-src 'self'" in csp and "frame-ancestors 'none'" in csp
    assert page.headers["X-Frame-Options"] == "DENY"
    api = await web.get("/api/state")
    assert api.headers["Cache-Control"] == "no-store"
    assert api.headers["X-Content-Type-Options"] == "nosniff"


@pytest.mark.asyncio
async def test_no_secret_readback_routes(make_web):
    web = await make_web(sample_snapshot())
    routes = [(r.method, r.resource.canonical) for r in web.client.server.app.router.routes()]
    for method, path in routes:
        assert not re.search(r"password|secret|private|api_key|token", path, re.I), path
    keystore_gets = [p for m, p in routes if m == "GET" and "keystore" in p]
    assert keystore_gets == ["/api/keystore"]


def _write_keystore(tmp_path, monkeypatch, password: str, api_key: str):
    """Create a throwaway keystore with the setup wizard's own writer (native Hummingbot encryption)."""
    from bin.lighter_robinhood_setup import write_encrypted_credentials
    from hummingbot.client.config import config_crypt, config_helpers, security
    from hummingbot.client.config.config_crypt import ETHKeyFileSecretManger, store_password_verification
    from hummingbot.client.config.security import Security

    connectors = tmp_path / "connectors"
    connectors.mkdir()
    verification = tmp_path / ".password_verification"
    monkeypatch.setattr(config_crypt, "PASSWORD_VERIFICATION_PATH", verification)
    monkeypatch.setattr(security, "PASSWORD_VERIFICATION_PATH", verification)
    monkeypatch.setattr(config_helpers, "CONNECTORS_CONF_DIR_PATH", connectors)
    monkeypatch.setattr(security, "update_connector_hb_config", lambda _cfg: None)
    monkeypatch.setattr(Security, "secrets_manager", None)
    monkeypatch.setattr(Security, "_secure_configs", {})
    manager = ETHKeyFileSecretManger(password)
    store_password_verification(manager)
    Security.secrets_manager = manager
    write_encrypted_credentials(connectors / "lighter_perpetual_robinhood.yml", account_index=7, api_key_index=3,
                                api_private_key=api_key)
    Security.secrets_manager = None
    Security._secure_configs.clear()
    return connectors


@pytest.mark.asyncio
async def test_keystore_unlock_masked_and_secrets_never_leak(make_web, tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    password = "pw-" + secrets.token_hex(12)
    api_key = secrets.token_hex(40)  # 80 hex: API private key format (not a real key)
    _write_keystore(tmp_path, monkeypatch, password, api_key)
    keystore = KeystoreService(demo=False)
    web = await make_web(sample_snapshot(), keystore=keystore, mode="live")
    await web.login()

    listing = await (await web.get("/api/keystore")).json()
    assert listing["status"]["unlocked"] is False
    profile = next(p for p in listing["profiles"] if p["name"] == "lighter_perpetual_robinhood")
    assert profile["fields"]["lighter_perpetual_robinhood_api_private_key"] == "задано"
    assert set(profile["fields"].values()) <= {"задано", "нет"}

    no_profile = await web.post("/api/keystore/unlock", {"password": password})
    assert no_profile.status == 422
    sel = await web.post("/api/keystore/select", {"profile": "lighter_perpetual_robinhood"})
    assert sel.status == 200
    wrong = await web.post("/api/keystore/unlock", {"password": password + "x"})
    assert wrong.status == 422
    ok = await web.post("/api/keystore/unlock", {"password": password})
    assert ok.status == 200, await ok.text()
    status = (await ok.json())["status"]
    assert status["unlocked"] is True and status["api_key_format"] == "valid"
    after = await web.get("/api/keystore")
    assert (await after.json())["status"]["unlocked"] is True
    await web.get("/api/state")
    await web.get("/api/session")

    from hummingbot.client.config.security import Security
    assert Security.api_keys("lighter_perpetual_robinhood")["lighter_perpetual_robinhood_api_private_key"] == api_key
    everything = "\n".join(web.responses) + "\n" + caplog.text
    for secret in (password, api_key, "0x" + api_key, ACCESS_TOKEN):
        assert secret not in everything
    assert "Keystore unlocked" in caplog.text


@pytest.mark.asyncio
async def test_keystore_is_never_created_from_web(make_web, tmp_path, monkeypatch):
    from hummingbot.client.config import config_crypt, config_helpers, security
    verification = tmp_path / ".password_verification"
    monkeypatch.setattr(config_crypt, "PASSWORD_VERIFICATION_PATH", verification)
    monkeypatch.setattr(security, "PASSWORD_VERIFICATION_PATH", verification)
    (tmp_path / "connectors").mkdir()
    (tmp_path / "connectors" / "lighter_perpetual_robinhood.yml").write_text("connector: lighter_perpetual_robinhood\n")
    monkeypatch.setattr(config_helpers, "CONNECTORS_CONF_DIR_PATH", tmp_path / "connectors")
    web = await make_web(sample_snapshot(), keystore=KeystoreService(demo=False), mode="live")
    await web.login()
    await web.post("/api/keystore/select", {"profile": "lighter_perpetual_robinhood"})
    resp = await web.post("/api/keystore/unlock", {"password": "first-password"})
    assert resp.status == 422
    assert "не создан" in (await resp.json())["message"]
    assert not verification.exists()


def test_static_assets_are_local_and_js_never_stores_or_numbers_ids():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    js = (STATIC / "app.js").read_text(encoding="utf-8")
    css = (STATIC / "app.css").read_text(encoding="utf-8")
    for text in (html, js, css):
        assert not re.search(r"https?://", text), "no external/CDN URLs"
        assert "@import" not in text
    assert re.findall(r"<script[^>]*src=\"([^\"]+)\"", html) == ["/static/app.js"]
    assert "<script>" not in html and "style=" not in html  # CSP: no inline script/style
    assert not re.search(r"(localStorage|sessionStorage|indexedDB|document\.cookie)\s*[.\[(]", js)
    assert not re.search(r"\b(Number|parseInt|parseFloat)\s*\(", js)
    assert ".innerHTML" not in js and "insertAdjacentHTML" not in js
    assert 'lang="ru"' in html
    assert "от имени выбранного профиля ключей" not in js
    assert "уже запущенному движку" in js
    assert "--unlock-tty" not in html


@pytest.mark.asyncio
async def test_demo_mode_never_touches_keystore(make_web, monkeypatch):
    from hummingbot.client.config.security import Security

    def forbidden(*_a, **_k):
        raise AssertionError("demo must not read or unlock the keystore")
    monkeypatch.setattr(Security, "login", forbidden)
    monkeypatch.setattr(Security, "new_password_required", forbidden)
    web = await make_web(sample_snapshot(), mode="demo")
    await web.login()
    body = await (await web.get("/api/keystore")).json()
    assert body["profiles"] == [] and body["status"]["demo"] is True and body["status"]["keystore_exists"] is None
    for path, payload in (("/api/keystore/select", {"profile": "lighter_perpetual_robinhood"}),
                          ("/api/keystore/unlock", {"password": "whatever"})):
        resp = await web.post(path, payload)
        assert resp.status == 422 and "Демо" in (await resp.json())["message"]
