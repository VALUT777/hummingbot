from decimal import Decimal
from pathlib import Path
import sys

import pytest
import yaml

from bin.lighter_robinhood_multi_grid_strike_launch import (
    CONTROLLER_CONFIG_NAME,
    SCRIPT_CONFIG_NAME,
    load_launch_config,
    run_launch,
)


ROOT = Path(__file__).resolve().parents[2]


def test_tracked_examples_are_valid_and_link_the_safe_runner():
    plan = load_launch_config(
        ROOT / "conf/controllers/lighter_robinhood_multi_grid_strike.yml.example",
        ROOT / "conf/scripts/lighter_robinhood_multi_grid_strike.yml.example",
    )

    assert plan.controller.connector_name == "lighter_perpetual_robinhood"
    assert plan.controller.trading_pair == "LIT-USDG"
    assert plan.controller.total_amount_quote == 3000
    assert plan.controller.max_abs_net_position == 1000
    assert plan.controller.max_open_orders == 2
    assert len(plan.controller.grids) == 1
    assert plan.controller.grids[0].grid_id == "lit-main"
    assert plan.script.controllers_config == [CONTROLLER_CONFIG_NAME]
    assert plan.script.script_file_name == "lighter_robinhood_multi_grid_strike.py"


def test_launch_validates_then_uses_native_hidden_password_prompt(tmp_path):
    controller = tmp_path / "controllers" / CONTROLLER_CONFIG_NAME
    script = tmp_path / "scripts" / SCRIPT_CONFIG_NAME
    controller.parent.mkdir()
    script.parent.mkdir()
    controller.write_text(
        (ROOT / "conf/controllers/lighter_robinhood_multi_grid_strike.yml.example").read_text()
    )
    script.write_text((ROOT / "conf/scripts/lighter_robinhood_multi_grid_strike.yml.example").read_text())
    calls = []
    output = []

    rc = run_launch(
        controller_path=controller,
        script_path=script,
        bot_running=lambda: False,
        invoke=lambda command: calls.append(command) or 0,
        tell=output.append,
    )

    assert rc == 0
    assert calls == [[
        sys.executable, str(ROOT / "bin/hbot"), "start", SCRIPT_CONFIG_NAME, "--v2-script",
    ]]
    assert all("password" not in argument.lower() for argument in calls[0])
    rendered = "\n".join(output)
    assert "3000 USDG" in rendered
    assert "примерно 11" in rendered
    assert "пароль Hummingbot" in rendered
    assert "на диапазон" in rendered
    assert "заблокирует новую сетку" in rendered
    assert "API" not in rendered


def test_launch_refuses_a_different_loader_or_running_bot(tmp_path):
    controller = tmp_path / "controllers" / CONTROLLER_CONFIG_NAME
    script = tmp_path / "scripts" / SCRIPT_CONFIG_NAME
    controller.parent.mkdir()
    script.parent.mkdir()
    controller.write_text(
        (ROOT / "conf/controllers/lighter_robinhood_multi_grid_strike.yml.example").read_text()
    )
    script_data = yaml.safe_load(
        (ROOT / "conf/scripts/lighter_robinhood_multi_grid_strike.yml.example").read_text()
    )
    script_data["controllers_config"] = ["unsafe.yml"]
    script.write_text(yaml.safe_dump(script_data))

    with pytest.raises(ValueError, match="controllers_config"):
        load_launch_config(controller, script)

    script_data["controllers_config"] = [CONTROLLER_CONFIG_NAME]
    script.write_text(yaml.safe_dump(script_data))
    with pytest.raises(RuntimeError, match="уже запущен"):
        run_launch(
            controller_path=controller,
            script_path=script,
            bot_running=lambda: True,
            invoke=lambda command: pytest.fail("must not start"),
            tell=lambda message: None,
        )


def test_launch_accepts_valid_tunable_native_sizing_controls(tmp_path):
    controller = tmp_path / "controllers" / CONTROLLER_CONFIG_NAME
    script = tmp_path / "scripts" / SCRIPT_CONFIG_NAME
    controller.parent.mkdir()
    script.parent.mkdir()
    controller_data = yaml.safe_load(
        (ROOT / "conf/controllers/lighter_robinhood_multi_grid_strike.yml.example").read_text()
    )
    controller_data.update({
        "total_amount_quote": "2500",
        "max_open_orders": 3,
        "max_orders_per_batch": 2,
        "activation_bounds": "0.05",
    })
    controller.write_text(yaml.safe_dump(controller_data))
    script.write_text((ROOT / "conf/scripts/lighter_robinhood_multi_grid_strike.yml.example").read_text())

    plan = load_launch_config(controller, script)

    assert plan.controller.total_amount_quote == 2500
    assert plan.controller.max_open_orders == 3
    assert plan.controller.max_orders_per_batch == 2
    assert plan.controller.activation_bounds == Decimal("0.05")
