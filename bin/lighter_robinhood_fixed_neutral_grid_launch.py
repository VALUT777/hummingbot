#!/usr/bin/env python
"""Validate and launch the fixed Robinhood LIT neutral grid plus its local web panel.

The launcher reads configuration and the plain account index only. Hummingbot itself owns the encrypted
credentials and prompts for its password without exposing it in argv.
"""

from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import yaml


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CONTROLLER_CONFIG_NAME = "lighter_robinhood_fixed_neutral_grid.yml"
SCRIPT_CONFIG_NAME = "lighter_robinhood_fixed_neutral_grid.yml"
CONTROLLER_CONFIG_PATH = ROOT / "conf/controllers" / CONTROLLER_CONFIG_NAME
SCRIPT_CONFIG_PATH = ROOT / "conf/scripts" / SCRIPT_CONFIG_NAME
CONNECTOR_CONFIG_PATH = ROOT / "conf/connectors/lighter_perpetual_robinhood.yml"
DB_WAIT_TIMEOUT = 3


class LaunchRefused(RuntimeError):
    pass


class LaunchCancelled(RuntimeError):
    pass


@dataclass(frozen=True)
class LaunchRuntime:
    stdin_isatty: Callable[[], bool]
    bot_running: Callable[[], bool]
    durable_stop: Callable[[Path], int | None]
    path_exists: Callable[[Path], bool]
    monotonic: Callable[[], float]
    sleep: Callable[[float], None]
    default_db_path: Callable[[str, str, int, str, Path], Path]


@dataclass(frozen=True)
class ControllerSnapshot:
    id: str
    controller_name: str
    controller_type: str
    enabled: bool
    connector_name: str
    trading_pair: str
    grid_id: str
    lower_price: Decimal
    upper_price: Decimal
    cell_count: int
    order_amount_base: Decimal
    leverage: Decimal
    position_mode: str
    expected_initial_position: Decimal
    max_abs_net_position: Decimal
    max_gross_position: Decimal
    max_active_orders: int
    db_path: str | None


@dataclass(frozen=True)
class ScriptSnapshot:
    script_file_name: str
    controllers_config: list[str]
    max_global_drawdown_quote: Any
    max_controller_drawdown_quote: Any
    live_start_confirmation: str | None
    resume_after_stop_confirmation: str | None


@dataclass(frozen=True)
class LaunchPlan:
    controller: Any
    script: Any
    account_index: int
    db_path: Path
    durable_stop_ms: int | None
    mode: str
    confirmation_word: str


def _default_runtime() -> LaunchRuntime:
    from hummingbot.cli import bot
    from hummingbot.strategy_v2.executors.neutral_grid_executor.executor import durable_stop_ms

    def resolve(connector: str, domain: str, account: int, pair: str, data_dir: Path) -> Path:
        from hummingbot.strategy_v2.executors.neutral_grid_executor.store import EngineIdentity, default_db_path
        return default_db_path(
            EngineIdentity(connector_name=connector, connector_domain=domain,
                           account_index=account, trading_pair=pair),
            base_dir=data_dir,
        )

    return LaunchRuntime(
        stdin_isatty=sys.stdin.isatty,
        bot_running=bot.running,
        durable_stop=lambda path: durable_stop_ms(str(path)),
        path_exists=Path.exists,
        monotonic=time.monotonic,
        sleep=time.sleep,
        default_db_path=resolve,
    )


def _read_yaml(path: Path) -> dict:
    if not path.is_file():
        raise FileNotFoundError(f"Не найден файл конфигурации: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Конфигурация {path.name} должна быть YAML-объектом")
    return data


def _read_plain_account_index(path: Path) -> int:
    """Read only the documented non-secret account index and never echo connector content."""
    if not path.is_file():
        raise FileNotFoundError(f"Не найден профиль коннектора: {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        value = data.get("lighter_perpetual_robinhood_account_index") if isinstance(data, dict) else None
        if isinstance(value, bool) or value is None:
            raise ValueError
        account_index = int(value)
        if account_index < 0 or str(value).strip() != str(account_index):
            raise ValueError
        connector = data.get("connector")
        if connector not in (None, "lighter_perpetual_robinhood"):
            raise ValueError
        return account_index
    except (OSError, ValueError, TypeError, yaml.YAMLError):
        raise ValueError(
            "Профиль lighter_perpetual_robinhood не содержит корректный plain account_index"
        ) from None


def _validate_policy(controller: Any, script: Any) -> None:
    expected = {
        "controller_name": "neutral_grid",
        "controller_type": "generic",
        "connector_name": "lighter_perpetual_robinhood",
        "trading_pair": "LIT-USDG",
        "leverage": Decimal("5"),
        "max_abs_net_position": Decimal("1000"),
        "max_gross_position": Decimal("1000"),
    }
    for field, value in expected.items():
        if getattr(controller, field) != value:
            raise ValueError(f"Небезопасное значение {field}: ожидается {value}")
    if str(controller.position_mode).upper() != "ONEWAY":
        raise ValueError("position_mode должен быть ONEWAY")
    if not controller.enabled:
        raise ValueError("enabled=false: реальный запуск запрещён")
    if script.script_file_name != "lighter_robinhood_fixed_neutral_grid.py":
        raise ValueError("script_file_name должен указывать на fixed neutral grid runner")
    if script.controllers_config != [CONTROLLER_CONFIG_NAME]:
        raise ValueError(f"controllers_config должен содержать только {CONTROLLER_CONFIG_NAME}")
    if script.max_global_drawdown_quote is not None or script.max_controller_drawdown_quote is not None:
        raise ValueError(
            "drawdown-параметры должны быть пустыми: "
            "остановка не закрывает позицию"
        )
    wanted = (f"START {controller.grid_id} ON lighter_perpetual_robinhood LIT-USDG "
              f"WITH B={controller.expected_initial_position}")
    if script.live_start_confirmation != wanted:
        raise ValueError(
            "live_start_confirmation не совпадает с текущей сеткой; "
            f"ожидается: {wanted}"
        )


def _db_path(controller: Any, account_index: int, data_dir: Path,
             default_db_path: Callable[[str, str, int, str, Path], Path]) -> Path:
    if controller.db_path:
        if "://" in str(controller.db_path):
            raise ValueError("db_path должен быть локальным путём")
        return Path(controller.db_path).expanduser().absolute()
    return default_db_path(
        controller.connector_name, controller.connector_name, account_index, controller.trading_pair, data_dir,
    ).absolute()


def _required(data: dict, key: str) -> Any:
    if key not in data:
        raise ValueError(f"В конфигурации отсутствует поле {key}")
    return data[key]


def _decimal(data: dict, key: str) -> Decimal:
    value = _required(data, key)
    if isinstance(value, (bool, float)):
        raise ValueError(f"{key} должен быть точным decimal-текстом")
    try:
        result = Decimal(str(value))
    except Exception:
        raise ValueError(f"{key} должен быть конечным decimal") from None
    if not result.is_finite():
        raise ValueError(f"{key} должен быть конечным decimal")
    return result


def _integer(data: dict, key: str) -> int:
    value = _required(data, key)
    if isinstance(value, bool):
        raise ValueError(f"{key} должен быть целым")
    try:
        result = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{key} должен быть целым") from None
    if str(value).strip() != str(result):
        raise ValueError(f"{key} должен быть целым")
    return result


def _controller_snapshot(data: dict) -> ControllerSnapshot:
    controller = ControllerSnapshot(
        id=str(_required(data, "id")),
        controller_name=str(_required(data, "controller_name")),
        controller_type=str(_required(data, "controller_type")),
        enabled=_required(data, "enabled") if isinstance(_required(data, "enabled"), bool) else False,
        connector_name=str(_required(data, "connector_name")),
        trading_pair=str(_required(data, "trading_pair")),
        grid_id=str(_required(data, "grid_id")),
        lower_price=_decimal(data, "lower_price"),
        upper_price=_decimal(data, "upper_price"),
        cell_count=_integer(data, "cell_count"),
        order_amount_base=_decimal(data, "order_amount_base"),
        leverage=_decimal(data, "leverage"),
        position_mode=str(_required(data, "position_mode")),
        expected_initial_position=_decimal(data, "expected_initial_position"),
        max_abs_net_position=_decimal(data, "max_abs_net_position"),
        max_gross_position=_decimal(data, "max_gross_position"),
        max_active_orders=_integer(data, "max_active_orders"),
        db_path=data.get("db_path"),
    )
    if not (Decimal("0") < controller.lower_price < controller.upper_price):
        raise ValueError("bounds должны удовлетворять 0 < lower_price < upper_price")
    if controller.cell_count < 1 or controller.order_amount_base <= 0 or controller.max_active_orders < 1:
        raise ValueError(
            "cell_count, order_amount_base и max_active_orders должны быть положительными"
        )
    return controller


def _script_snapshot(data: dict) -> ScriptSnapshot:
    controllers = _required(data, "controllers_config")
    if not isinstance(controllers, list) or not all(isinstance(item, str) for item in controllers):
        raise ValueError("controllers_config должен быть списком имён")
    return ScriptSnapshot(
        script_file_name=str(_required(data, "script_file_name")),
        controllers_config=list(controllers),
        max_global_drawdown_quote=data.get("max_global_drawdown_quote"),
        max_controller_drawdown_quote=data.get("max_controller_drawdown_quote"),
        live_start_confirmation=data.get("live_start_confirmation"),
        resume_after_stop_confirmation=data.get("resume_after_stop_confirmation"),
    )


def load_launch_plan(controller_path: Path = CONTROLLER_CONFIG_PATH,
                     script_path: Path = SCRIPT_CONFIG_PATH,
                     connector_path: Path = CONNECTOR_CONFIG_PATH,
                     data_dir: Path | None = None,
                     *, durable_stop: Callable[[Path], int | None],
                     default_db_path: Callable[[str, str, int, str, Path], Path]) -> LaunchPlan:
    controller = _controller_snapshot(_read_yaml(controller_path))
    script = _script_snapshot(_read_yaml(script_path))
    _validate_policy(controller, script)
    account_index = _read_plain_account_index(connector_path)
    if data_dir is None:
        from hummingbot import data_path
        data_dir = data_path()
    db_path = _db_path(controller, account_index, Path(data_dir), default_db_path)
    stop_ms = durable_stop(db_path)
    configured_resume = script.resume_after_stop_confirmation
    if stop_ms is None:
        if configured_resume not in (None, ""):
            raise LaunchRefused(
                "В конфигурации есть фраза RESUME, но durable STOP в журнале нет"
            )
        mode, word = "trading", "START"
    else:
        wanted = f"RESUME {controller.grid_id} AFTER STOP {stop_ms}"
        if configured_resume in (None, ""):
            mode, word = "maintenance", "OPEN"
        elif configured_resume == wanted:
            mode, word = "trading", "START"
        else:
            raise LaunchRefused(
                "Фраза resume устарела или не совпадает с последним durable STOP "
                f"{stop_ms}; запуск запрещён"
            )
    return LaunchPlan(controller, script, account_index, db_path, stop_ms, mode, word)


def _summary(plan: LaunchPlan) -> str:
    c = plan.controller
    common = (
        f"  {c.connector_name} {c.trading_pair}, аккаунт {plan.account_index}\n"
        f"  grid_id {c.grid_id}; диапазон {c.lower_price}…{c.upper_price}\n"
        f"  {c.cell_count} ячеек / {c.cell_count + 1} границ; {c.order_amount_base} LIT на ордер\n"
        f"  плечо {c.leverage}x {getattr(c.position_mode, 'name', c.position_mode)}; "
        f"лимиты net/gross {c.max_abs_net_position}/{c.max_gross_position} LIT; "
        f"лимит активных ордеров {c.max_active_orders}\n"
        f"  журнал {plan.db_path}\n"
    )
    if plan.mode == "maintenance":
        mode = (
            "Режим: ОСТАНОВЛЕННОЕ ОБСЛУЖИВАНИЕ.\n"
            f"Durable STOP {plan.durable_stop_ms} остаётся в силе: "
            "новые входы не разрешены. Движок обновит позицию/историю "
            "и ожидает "
            "действие в веб-панели.\n"
        )
    else:
        resume = (f" Resume привязан точно на STOP {plan.durable_stop_ms}."
                  if plan.durable_stop_ms is not None else "")
        mode = f"Режим: РЕАЛЬНАЯ ТОРГОВЛЯ.{resume}\n"
    return "".join((
        "Проверена фактическая конфигурация нейтральной LIT сетки:\n",
        common,
        mode,
        "Закрытие этого окна/браузера не останавливает бота. "
        "Для остановки используйте "
        "Стоп в панели или Остановить нейтральную LIT сетку.command.\n",
        "Пароль Hummingbot будет запрошен штатно и скрыто; "
        "пароль и ключи не "
        "передаются в argv.",
    ))


def _invoke_native(command: list[str]) -> int:
    return subprocess.run(command, cwd=ROOT).returncode


def _confirm(plan: LaunchPlan, input_fn: Callable[[str], str]) -> None:
    try:
        answer = input_fn(f"Введите {plan.confirmation_word} для продолжения: ")
    except (EOFError, KeyboardInterrupt):
        raise LaunchCancelled("Запуск отменён до Hummingbot") from None
    answer = answer.strip()
    if not answer or answer.lower() in {"cancel", "отмена"}:
        raise LaunchCancelled("Запуск отменён до Hummingbot")
    if answer != plan.confirmation_word:
        raise LaunchRefused(
            f"Ожидалось точное слово {plan.confirmation_word}; Hummingbot не запущен"
        )


def run_launch(*, controller_path: Path = CONTROLLER_CONFIG_PATH,
               script_path: Path = SCRIPT_CONFIG_PATH,
               connector_path: Path = CONNECTOR_CONFIG_PATH,
               data_dir: Path | None = None,
               runtime: LaunchRuntime | None = None,
               input_fn: Callable[[str], str] = input,
               invoke: Callable[[list[str]], int] = _invoke_native,
               tell: Callable[[str], None] = print,
               db_wait_seconds: float = 30.0) -> int:
    runtime = runtime or _default_runtime()
    if runtime.bot_running():
        raise LaunchRefused(
            "Hummingbot уже запущен; сначала используйте штатную остановку"
        )
    if not runtime.stdin_isatty():
        raise LaunchRefused(
            "Требуется интерактивный Terminal (TTY); "
            "автоматический запуск запрещён"
        )
    plan = load_launch_plan(
        controller_path, script_path, connector_path, data_dir, durable_stop=runtime.durable_stop,
        default_db_path=runtime.default_db_path,
    )
    tell(_summary(plan))
    _confirm(plan, input_fn)

    hbot = [sys.executable, str(ROOT / "bin/hbot"), "start", SCRIPT_CONFIG_NAME, "--v2-script"]
    result = invoke(hbot)
    if result != 0:
        return result

    deadline = runtime.monotonic() + db_wait_seconds
    while not runtime.path_exists(plan.db_path) and runtime.monotonic() < deadline:
        runtime.sleep(0.25)
    if not runtime.path_exists(plan.db_path):
        tell(
            f"Журнал {plan.db_path} не появился за {db_wait_seconds:g} с; "
            "веб-панель не запущена."
        )
        return DB_WAIT_TIMEOUT

    web = [
        sys.executable,
        str(ROOT / "bin/lighter_robinhood_neutral_grid_web.py"),
        "--attach-db",
        str(plan.db_path),
    ]
    return invoke(web)


def main() -> int:
    try:
        return run_launch()
    except LaunchCancelled as exc:
        print(str(exc))
        return 130
    except (LaunchRefused, FileNotFoundError, ValueError) as exc:
        print(f"Ошибка запуска: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
