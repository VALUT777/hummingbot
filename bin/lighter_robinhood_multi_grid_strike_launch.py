#!/usr/bin/env python
"""Validate and launch the Robinhood LIT Multi Grid Strike with native Hummingbot password input."""

from __future__ import annotations

import math
import subprocess
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import yaml


ROOT = Path(__file__).resolve().parents[1]
CONTROLLER_CONFIG_NAME = "lighter_robinhood_multi_grid_strike.yml"
SCRIPT_CONFIG_NAME = "lighter_robinhood_multi_grid_strike.yml"
CONTROLLER_CONFIG_PATH = ROOT / "conf/controllers" / CONTROLLER_CONFIG_NAME
SCRIPT_CONFIG_PATH = ROOT / "conf/scripts" / SCRIPT_CONFIG_NAME


@dataclass(frozen=True)
class LaunchConfiguration:
    controller: Any
    script: Any


def _read_yaml(path: Path) -> dict:
    if not path.is_file():
        raise FileNotFoundError(f"Не найден файл конфигурации: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Конфигурация {path.name} должна быть YAML-объектом")
    return data


def _enum_name(value: Any) -> str:
    return getattr(value, "name", str(value)).upper()


def _validate_policy(controller: Any, script: Any) -> None:
    expected = {
        "controller_name": "lighter_robinhood_multi_grid_strike",
        "connector_name": "lighter_perpetual_robinhood",
        "trading_pair": "LIT-USDG",
        "leverage": 5,
        "keep_position": True,
    }
    for field, value in expected.items():
        if getattr(controller, field) != value:
            raise ValueError(f"Небезопасное значение {field}: ожидается {value}")
    if _enum_name(controller.position_mode) != "ONEWAY":
        raise ValueError("position_mode должен быть ONEWAY")
    if Decimal(controller.max_abs_net_position) != Decimal("1000"):
        raise ValueError("Лимит позиции должен быть 1000 LIT")
    barrier = controller.triple_barrier_config
    if (_enum_name(barrier.open_order_type) != "LIMIT_MAKER"
            or _enum_name(barrier.take_profit_order_type) != "LIMIT_MAKER"):
        raise ValueError("Вход и take-profit должны использовать LIMIT_MAKER")
    if any(value is not None for value in (barrier.stop_loss, barrier.time_limit, barrier.trailing_stop)):
        raise ValueError("stop-loss, time-limit и trailing-stop должны быть пустыми")
    if not controller.grids or any(_enum_name(grid.side) != "BUY" for grid in controller.grids):
        raise ValueError("Нужен хотя бы один LONG/BUY диапазон")
    allocation = sum(Decimal(grid.amount_quote_pct) for grid in controller.grids if grid.enabled)
    if allocation <= 0 or allocation > 1:
        raise ValueError("Сумма долей включённых диапазонов должна быть больше 0 и не больше 1")
    if script.script_file_name != "lighter_robinhood_multi_grid_strike.py":
        raise ValueError("script_file_name должен указывать на безопасный Multi Grid Strike runner")
    if script.controllers_config != [CONTROLLER_CONFIG_NAME]:
        raise ValueError(f"controllers_config должен содержать только {CONTROLLER_CONFIG_NAME}")
    if script.max_global_drawdown_quote is not None or script.max_controller_drawdown_quote is not None:
        raise ValueError("Для этого runner drawdown-параметры должны быть пустыми")


def load_launch_config(controller_path: Path = CONTROLLER_CONFIG_PATH,
                       script_path: Path = SCRIPT_CONFIG_PATH) -> LaunchConfiguration:
    from controllers.generic.lighter_robinhood_multi_grid_strike import LighterRobinhoodMultiGridStrikeConfig
    from scripts.lighter_robinhood_multi_grid_strike import LighterRobinhoodMultiGridStrikeScriptConfig

    controller = LighterRobinhoodMultiGridStrikeConfig(**_read_yaml(controller_path))
    script = LighterRobinhoodMultiGridStrikeScriptConfig(**_read_yaml(script_path))
    _validate_policy(controller, script)
    return LaunchConfiguration(controller=controller, script=script)


def _estimated_levels(controller: Any) -> int:
    spread = Decimal(controller.min_spread_between_orders)
    minimum_quote_with_margin = Decimal(controller.min_order_amount_quote) * Decimal("1.05")
    total_quote = Decimal(controller.total_amount_quote)
    estimate = 0
    for grid in controller.grids:
        if not grid.enabled:
            continue
        by_spread = math.floor(
            ((Decimal(grid.end_price) - Decimal(grid.start_price)) / Decimal(grid.start_price)) / spread
        )
        by_budget = math.floor(
            (total_quote * Decimal(grid.amount_quote_pct)) / minimum_quote_with_margin
        )
        estimate += max(1, min(by_spread, by_budget))
    return estimate


def _summary(plan: LaunchConfiguration) -> str:
    controller = plan.controller
    ranges = ", ".join(
        f"{grid.grid_id}: {grid.start_price}…{grid.end_price} LIT, лимит {grid.limit_price}, "
        f"доля {Decimal(grid.amount_quote_pct) * 100}%"
        for grid in controller.grids if grid.enabled
    )
    return (
        "Проверена конфигурация Multi Grid Strike:\n"
        f"  LONG {controller.trading_pair} через {controller.connector_name}; {ranges}\n"
        f"  бюджет {controller.total_amount_quote} USDG, плечо {controller.leverage}x ONEWAY, "
        f"лимит позиции {controller.max_abs_net_position} LIT\n"
        f"  примерно {_estimated_levels(controller)} уровней; минимум "
        f"{controller.min_order_amount_quote} USDG на уровень\n"
        f"  максимум {controller.max_open_orders} открытых ордера входа на диапазон; "
        "take-profit ордера идут дополнительно\n"
        f"  пакет {controller.max_orders_per_batch} и интервал {controller.order_frequency} с применяются "
        "к каждому диапазону отдельно\n"
        "  ручная остановка и нижний лимит сохраняют позицию; выход выше диапазона может её закрыть. "
        "Сохранённая позиция заблокирует новую сетку при следующем запуске до ручной сверки.\n"
        "Далее Terminal запросит только скрытый пароль Hummingbot."
    )


def _invoke_native(command: list[str]) -> int:
    return subprocess.run(command, cwd=ROOT).returncode


def run_launch(*, controller_path: Path = CONTROLLER_CONFIG_PATH,
               script_path: Path = SCRIPT_CONFIG_PATH,
               bot_running: Callable[[], bool] | None = None,
               invoke: Callable[[list[str]], int] = _invoke_native,
               tell: Callable[[str], None] = print) -> int:
    if bot_running is None:
        from hummingbot.cli import bot
        bot_running = bot.running
    if bot_running():
        raise RuntimeError("Hummingbot уже запущен; сначала используйте штатную остановку")
    plan = load_launch_config(controller_path, script_path)
    tell(_summary(plan))
    command = [sys.executable, str(ROOT / "bin/hbot"), "start", SCRIPT_CONFIG_NAME, "--v2-script"]
    return invoke(command)


def main() -> int:
    try:
        return run_launch()
    except KeyboardInterrupt:
        print("\nЗапуск отменён.")
        return 130
    except Exception as exc:
        print(f"Ошибка запуска: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
