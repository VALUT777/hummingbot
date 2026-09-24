from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[2]
LAUNCHER_PATH = ROOT / "bin/lighter_robinhood_fixed_neutral_grid_launch.py"


def _load_launcher():
    spec = importlib.util.spec_from_file_location("fixed_neutral_grid_launch", LAUNCHER_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


launcher = _load_launcher()


def test_direct_launcher_import_pins_hummingbot_to_its_own_checkout(tmp_path):
    probe = """
import importlib.util
import sys
from pathlib import Path

launcher_path = Path(sys.argv[1])
spec = importlib.util.spec_from_file_location("fixed_neutral_grid_launch_probe", launcher_path)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
print(Path(importlib.util.find_spec("hummingbot").origin).resolve())
"""
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    result = subprocess.run(
        [sys.executable, "-c", probe, str(LAUNCHER_PATH)],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert Path(result.stdout.strip()) == (ROOT / "hummingbot/__init__.py").resolve()


def _files(tmp_path: Path, *, resume: str | None = None, account_index: int = 10123):
    controller = tmp_path / "conf/controllers/lighter_robinhood_fixed_neutral_grid.yml"
    script = tmp_path / "conf/scripts/lighter_robinhood_fixed_neutral_grid.yml"
    connector = tmp_path / "conf/connectors/lighter_perpetual_robinhood.yml"
    data_dir = tmp_path / "data"
    controller.parent.mkdir(parents=True)
    script.parent.mkdir(parents=True)
    connector.parent.mkdir(parents=True)
    data_dir.mkdir()

    controller_data = yaml.safe_load(
        (ROOT / "conf/controllers/lighter_robinhood_fixed_neutral_grid.yml.example").read_text()
    )
    controller_data.update({
        "id": "lit-robinhood-fixed-neutral-grid-v2",
        "enabled": True,
        "grid_id": "lit-neutral-fixed-v2",
        "lower_price": "4.9",
        "upper_price": "5.9",
        "cell_count": 24,
        "order_amount_base": "20",
    })
    controller.write_text(yaml.safe_dump(controller_data), encoding="utf-8")

    script_data = yaml.safe_load(
        (ROOT / "conf/scripts/lighter_robinhood_fixed_neutral_grid.yml.example").read_text()
    )
    script_data["live_start_confirmation"] = (
        "START lit-neutral-fixed-v2 ON lighter_perpetual_robinhood LIT-USDG WITH B=0"
    )
    if resume is not None:
        script_data["resume_after_stop_confirmation"] = resume
    script.write_text(yaml.safe_dump(script_data), encoding="utf-8")

    # The launcher may read this one documented plain field. The encrypted value is a sentinel and must never
    # appear in output or argv.
    connector.write_text(yaml.safe_dump({
        "connector": "lighter_perpetual_robinhood",
        "lighter_perpetual_robinhood_account_index": account_index,
        "lighter_perpetual_robinhood_api_private_key": "SUPER_SECRET_SENTINEL",
    }), encoding="utf-8")
    return controller, script, connector, data_dir


def _runtime(*, stop_ms: int | None, db_exists=True, tty=True, running=False):
    now = [0.0]

    def sleep(seconds: float):
        now[0] += seconds

    return launcher.LaunchRuntime(
        stdin_isatty=lambda: tty,
        bot_running=lambda: running,
        durable_stop=lambda path: stop_ms,
        path_exists=(lambda path: db_exists() if callable(db_exists) else bool(db_exists)),
        monotonic=lambda: now[0],
        sleep=sleep,
        default_db_path=lambda connector, domain, account, pair, data: (
            Path(data) / "neutral_grid" / f"neutral_grid.{domain}.{account}.{pair}.sqlite3"
        ),
    )


def _run(tmp_path: Path, *, stop_ms=None, resume=None, answer="START", runtime=None):
    controller, script, connector, data_dir = _files(tmp_path, resume=resume)
    calls: list[list[str]] = []
    output: list[str] = []
    rc = launcher.run_launch(
        controller_path=controller,
        script_path=script,
        connector_path=connector,
        data_dir=data_dir,
        runtime=runtime or _runtime(stop_ms=stop_ms),
        input_fn=lambda prompt: answer,
        invoke=lambda command: calls.append(command) or 0,
        tell=output.append,
        db_wait_seconds=2,
    )
    return rc, calls, "\n".join(output), data_dir


def test_fresh_launch_shows_actual_24x20_config_and_requires_start(tmp_path):
    rc, calls, rendered, data_dir = _run(tmp_path, answer="START")

    assert rc == 0
    assert "РЕАЛЬНАЯ ТОРГОВЛЯ" in rendered
    assert "4.9…5.9" in rendered
    assert "24 ячеек / 25 границ" in rendered
    assert "20 LIT на ордер" in rendered
    assert "плечо 5x" in rendered
    assert "лимиты net/gross 1000/1000 LIT" in rendered
    assert "аккаунт 10123" in rendered
    assert "Закрытие этого окна/браузера не останавливает бота" in rendered
    assert calls[0] == [
        sys.executable, str(ROOT / "bin/hbot"), "start",
        "lighter_robinhood_fixed_neutral_grid.yml", "--v2-script",
    ]
    expected_db = (
        data_dir
        / "neutral_grid/neutral_grid.lighter_perpetual_robinhood.10123.LIT-USDG.sqlite3"
    )
    assert str(expected_db) in rendered
    assert calls[1] == [
        sys.executable, str(ROOT / "bin/lighter_robinhood_neutral_grid_web.py"),
        "--attach-db", str(expected_db),
    ]
    assert all("password" not in arg.lower() and "SUPER_SECRET_SENTINEL" not in arg for call in calls for arg in call)


def test_durable_stop_without_resume_opens_stopped_maintenance(tmp_path):
    rc, calls, rendered, _ = _run(tmp_path, stop_ms=1790258000000, answer="OPEN")

    assert rc == 0 and len(calls) == 2
    assert "ОСТАНОВЛЕННОЕ ОБСЛУЖИВАНИЕ" in rendered
    assert "STOP 1790258000000 остаётся в силе" in rendered
    assert "новые входы не разрешены" in rendered
    assert "ожидает действие в веб-панели" in rendered


def test_exact_latest_resume_is_real_trading_and_requires_start(tmp_path):
    stop_ms = 1790258000000
    phrase = f"RESUME lit-neutral-fixed-v2 AFTER STOP {stop_ms}"

    rc, calls, rendered, _ = _run(tmp_path, stop_ms=stop_ms, resume=phrase, answer="START")

    assert rc == 0 and len(calls) == 2
    assert "РЕАЛЬНАЯ ТОРГОВЛЯ" in rendered
    assert f"точно на STOP {stop_ms}" in rendered


@pytest.mark.parametrize("resume", [
    "RESUME lit-neutral-fixed-v2 AFTER STOP 1790257999999",
    "RESUME lit-neutral-fixed-v1 AFTER STOP 1790258000000",
])
def test_stale_or_wrong_resume_phrase_refuses_before_hbot(tmp_path, resume):
    calls = []
    controller, script, connector, data_dir = _files(tmp_path, resume=resume)

    with pytest.raises(launcher.LaunchRefused, match="устарела|не совпадает"):
        launcher.run_launch(
            controller_path=controller, script_path=script, connector_path=connector, data_dir=data_dir,
            runtime=_runtime(stop_ms=1790258000000), input_fn=lambda prompt: "START",
            invoke=lambda command: calls.append(command) or 0, tell=lambda message: None,
        )

    assert calls == []


def test_resume_phrase_without_a_durable_stop_refuses(tmp_path):
    calls = []
    controller, script, connector, data_dir = _files(
        tmp_path, resume="RESUME lit-neutral-fixed-v2 AFTER STOP 1790258000000"
    )
    with pytest.raises(launcher.LaunchRefused, match="STOP.*нет|нет.*STOP"):
        launcher.run_launch(
            controller_path=controller, script_path=script, connector_path=connector, data_dir=data_dir,
            runtime=_runtime(stop_ms=None), input_fn=lambda prompt: "START",
            invoke=lambda command: calls.append(command) or 0, tell=lambda message: None,
        )
    assert calls == []


@pytest.mark.parametrize("tty,answer", [(False, "START"), (True, ""), (True, "cancel"), (True, "OPEN")])
def test_non_tty_blank_cancel_or_wrong_word_never_invokes_hbot(tmp_path, tty, answer):
    calls = []
    controller, script, connector, data_dir = _files(tmp_path)
    exc = launcher.LaunchRefused if not tty or answer == "OPEN" else launcher.LaunchCancelled
    with pytest.raises(exc):
        launcher.run_launch(
            controller_path=controller, script_path=script, connector_path=connector, data_dir=data_dir,
            runtime=_runtime(stop_ms=None, tty=tty), input_fn=lambda prompt: answer,
            invoke=lambda command: calls.append(command) or 0, tell=lambda message: None,
        )
    assert calls == []


def test_keyboard_cancel_and_running_bot_never_invoke(tmp_path):
    controller, script, connector, data_dir = _files(tmp_path)
    calls = []
    with pytest.raises(launcher.LaunchCancelled):
        launcher.run_launch(
            controller_path=controller, script_path=script, connector_path=connector, data_dir=data_dir,
            runtime=_runtime(stop_ms=None), input_fn=lambda prompt: (_ for _ in ()).throw(KeyboardInterrupt()),
            invoke=lambda command: calls.append(command) or 0, tell=lambda message: None,
        )
    with pytest.raises(launcher.LaunchRefused, match="уже запущен"):
        launcher.run_launch(
            controller_path=controller, script_path=script, connector_path=connector, data_dir=data_dir,
            runtime=_runtime(stop_ms=None, running=True), input_fn=lambda prompt: "START",
            invoke=lambda command: calls.append(command) or 0, tell=lambda message: None,
        )
    assert calls == []


def test_missing_ledger_after_native_start_times_out_without_opening_web(tmp_path):
    rc, calls, _, _ = _run(
        tmp_path, answer="START", runtime=_runtime(stop_ms=None, db_exists=False)
    )

    assert rc == launcher.DB_WAIT_TIMEOUT
    assert len(calls) == 1
    assert "--replace" not in calls[0]


def test_policy_and_loader_are_validated_without_constructing_a_connector(tmp_path):
    controller, script, connector, data_dir = _files(tmp_path)
    script_data = yaml.safe_load(script.read_text())
    script_data["controllers_config"] = ["unsafe.yml"]
    script.write_text(yaml.safe_dump(script_data))

    with pytest.raises(ValueError, match="controllers_config"):
        launcher.load_launch_plan(
            controller, script, connector, data_dir, durable_stop=lambda path: None,
            default_db_path=_runtime(stop_ms=None).default_db_path,
        )

    script_data["controllers_config"] = ["lighter_robinhood_fixed_neutral_grid.yml"]
    script.write_text(yaml.safe_dump(script_data))
    controller_data = yaml.safe_load(controller.read_text())
    controller_data["max_abs_net_position"] = "999"
    controller.write_text(yaml.safe_dump(controller_data))
    with pytest.raises(ValueError, match="max_abs_net_position"):
        launcher.load_launch_plan(
            controller, script, connector, data_dir, durable_stop=lambda path: None,
            default_db_path=_runtime(stop_ms=None).default_db_path,
        )


def test_connector_error_never_echoes_secret_value(tmp_path):
    controller, script, connector, data_dir = _files(tmp_path)
    connector.write_text("lighter_perpetual_robinhood_account_index: nope\nprivate_key: SUPER_SECRET_SENTINEL\n")

    with pytest.raises(ValueError) as caught:
        launcher.load_launch_plan(
            controller, script, connector, data_dir, durable_stop=lambda path: None,
            default_db_path=_runtime(stop_ms=None).default_db_path,
        )

    assert "SUPER_SECRET_SENTINEL" not in str(caught.value)
