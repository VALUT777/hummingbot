"""Launcher: loopback default, no secrets/profile controls, attach mode serves committed state."""
from __future__ import annotations

import asyncio
import importlib.util
import io
import logging
import socket
import sys
import time
from pathlib import Path

import aiohttp
import pytest

from web.neutral_grid.security import BindRefused

ROOT = Path(__file__).resolve().parents[3]
LAUNCHER_PATH = ROOT / "bin" / "lighter_robinhood_neutral_grid_web.py"


def _load_launcher():
    spec = importlib.util.spec_from_file_location("ngweb_launcher", LAUNCHER_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["ngweb_launcher"] = module
    spec.loader.exec_module(module)
    return module


launcher = _load_launcher()


def test_defaults_bind_loopback_only():
    args = launcher.parse_args(["--demo-fake-exchange"])
    assert args.host == "127.0.0.1" and args.port == 8787 and args.bind_warning is None
    with pytest.raises(BindRefused):
        launcher.parse_args(["--demo-fake-exchange", "--host", "0.0.0.0"])
    with pytest.raises(BindRefused):
        launcher.parse_args(["--demo-fake-exchange", "--host", "192.168.1.5"])
    allowed = launcher.parse_args(["--demo-fake-exchange", "--host", "0.0.0.0", "--allow-non-loopback-bind"])
    assert "SSH" in allowed.bind_warning
    with pytest.raises(launcher.LaunchRefused):
        launcher.parse_args(["--demo-fake-exchange", "--allowed-host", "box.local"])


def test_main_refuses_public_bind_with_exit_code(capsys):
    assert launcher.main(["--demo-fake-exchange", "--host", "0.0.0.0"]) == 2
    assert "loopback" in capsys.readouterr().err


@pytest.mark.parametrize("argv", [
    ["--demo-fake-exchange", "--password", "hunter2-secret"],
    ["--demo-fake-exchange", "--password=hunter2-secret"],
    ["--demo-fake-exchange", "--api-key", "hunter2-secret"],
    ["--demo-fake-exchange", "--private_key=hunter2-secret"],
    ["--demo-fake-exchange", "--keystore-passwd", "hunter2-secret"],
])
def test_secrets_are_never_accepted_in_argv(argv, capsys):
    with pytest.raises(launcher.LaunchRefused):
        launcher.parse_args(argv)
    assert launcher.main(argv) == 2
    captured = capsys.readouterr()
    assert "hunter2-secret" not in captured.out + captured.err


def test_argparse_errors_do_not_echo_values(capsys):
    with pytest.raises(SystemExit):
        launcher.parse_args(["--demo-fake-exchange", "hunter2-typed-by-mistake"])
    captured = capsys.readouterr()
    assert "hunter2-typed-by-mistake" not in captured.out + captured.err


def test_launcher_source_has_no_secret_options():
    source = LAUNCHER_PATH.read_text(encoding="utf-8")
    options = [line.lower() for line in source.splitlines() if "add_argument(" in line]
    forbidden = ("password", "secret", "api-key", "private")
    assert options and not any(word in line for line in options for word in forbidden)


def test_launcher_has_no_detached_keystore_controls(capsys):
    for option in ("--unlock-tty", "--profile"):
        with pytest.raises(SystemExit):
            launcher.parse_args(["--attach-db", "engine.sqlite3", option, "unused"])
        assert "unused" not in capsys.readouterr().err
    source = LAUNCHER_PATH.read_text(encoding="utf-8")
    assert 'add_argument("--unlock-tty"' not in source
    assert 'add_argument("--profile"' not in source


def test_login_url_uses_fragment_not_query():
    url = launcher.login_url("127.0.0.1", 8787, "tok")
    assert url == "http://127.0.0.1:8787/#auth=tok"
    assert launcher.login_url("0.0.0.0", 1, "t").startswith("http://127.0.0.1:1/")


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.mark.asyncio
async def test_attach_mode_serves_store_and_never_logs_token(tmp_path, caplog):
    from hummingbot.strategy_v2.executors.neutral_grid_executor.store import EngineIdentity, NeutralGridStore

    caplog.set_level(logging.DEBUG)
    identity = EngineIdentity(connector_name="lighter_perpetual_robinhood", connector_domain="lighter_perpetual_robinhood",
                              account_index=7, trading_pair="LIT-USDG")
    db = tmp_path / "ng.sqlite3"
    writer = NeutralGridStore.open(db, identity, create_if_missing=True, lock_dir=tmp_path / "locks")
    from test_ngweb_attach import attach_snapshot, engine_config_json

    body = attach_snapshot(engine_config=engine_config_json(enabled=False))
    body["engine_state"] = "PAUSED"
    body["reasons"] = ["OPERATOR_PAUSE"]
    for key in ("snapshot_version", "config_revision", "engine_revision", "committed_at"):
        body.pop(key)
    writer.write_snapshot(None, body)
    (tmp_path / "ng.sqlite3.health.json").write_text(
        '{"persistence_error": null, "fatal_reason": null, "at": "%s", "engine_revision": 0}' % time.time())
    port = _free_port()
    args = launcher.parse_args(["--attach-db", str(db), "--port", str(port)])
    ready, stop, out = asyncio.Event(), asyncio.Event(), io.StringIO()
    task = asyncio.create_task(launcher.serve(args, ready=ready, stop=stop, out=out))
    await asyncio.wait_for(ready.wait(), 10)
    printed = out.getvalue()
    token = printed.split("#auth=")[1].split()[0]
    base = f"http://127.0.0.1:{port}"
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as http:
        async with http.post(base + "/api/login", json={"token": token}, headers={"Origin": base}) as r:
            assert r.status == 200
        async with http.get(base + "/api/state") as r:
            state = await r.json()
        async with http.get(base + "/api/preview") as r:
            preview = await r.json()
    stop.set()
    await asyncio.wait_for(task, 10)
    writer.close()
    assert state["mode"] == "attach" and state["engine"]["display_state"] == "PAUSED"
    assert state["engine_identity"]["grid_id"] == "ng-engine-grid"  # from the engine's committed config
    assert state["health"]["known"] is True and state["health"]["banner"] is None
    assert preview["grid"]["cells"] == 55 and preview["market_source"] == "snapshot"
    assert any(e.startswith("enabled=false") for e in preview["errors"])  # live start refused while disabled
    assert token not in caplog.text


def test_config_option_removed_attach_reads_engine_config():
    with pytest.raises(SystemExit):
        launcher.parse_args(["--attach-db", "x.sqlite3", "--config", "grid.yml"])
