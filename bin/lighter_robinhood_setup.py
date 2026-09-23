#!/usr/bin/env python
"""Russian local setup wizard for the Robinhood Lighter neutral grid."""

from __future__ import annotations

import asyncio
import fcntl
import getpass
import os
import re
import subprocess
import sys
import time
import warnings
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional

import aiohttp
import yaml

from bin.lighter_robinhood_preflight import (
    AiohttpPublicClient,
    AuthenticatedReadClient,
    PreflightCredentials,
    SdkProbe,
    evaluate_private_snapshot,
    redact,
    run_public_preflight,
)


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "conf/scripts/lighter_robinhood_neutral_grid.yml"
CONFIG_NAME = CONFIG_PATH.name
DOMAIN = "lighter_perpetual_robinhood"
LOCK_PATH = ROOT / "data/lighter_robinhood_setup.lock"
_HEX_KEY = re.compile(r"[0-9a-fA-F]{80}\Z")
_INTEGER = re.compile(r"(?:0|[1-9][0-9]*)\Z")


class SecretInputUnavailable(RuntimeError):
    pass


def parse_index(value: str, label: str, minimum: int, maximum: Optional[int] = None) -> int:
    value = value.strip()
    if not _INTEGER.fullmatch(value):
        raise ValueError(f"{label}: введите целое число без знака +")
    parsed = int(value)
    if parsed < minimum or (maximum is not None and parsed > maximum):
        limit = f"{minimum}…{maximum}" if maximum is not None else f"не меньше {minimum}"
        raise ValueError(f"{label}: допустимо {limit}")
    return parsed


def normalize_api_private_key(value: str) -> str:
    value = value.strip()
    normalized = value[2:] if value.startswith(("0x", "0X")) else value
    if not _HEX_KEY.fullmatch(normalized):
        raise ValueError("API Private Key должен содержать ровно 80 шестнадцатеричных символов")
    return normalized.lower()


def parse_nonnegative_decimal(value: str, label: str, *, positive: bool = False) -> Decimal:
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        raise ValueError(f"{label}: введите обычное число") from None
    if not parsed.is_finite() or parsed < 0 or (positive and parsed == 0):
        qualifier = "положительным" if positive else "неотрицательным"
        raise ValueError(f"{label} должно быть конечным {qualifier} числом")
    return parsed


def read_hidden(prompt: str, *, stdin=sys.stdin, getter=getpass.getpass) -> str:
    if not stdin.isatty():
        raise SecretInputUnavailable("Скрытый ввод доступен только в окне Terminal")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            return getter(prompt)
    except (EOFError, KeyboardInterrupt):
        raise
    except Exception as exc:
        raise SecretInputUnavailable("Terminal не смог обеспечить скрытый ввод") from exc


def build_candidate(
    lower: Decimal,
    upper: Decimal,
    reserve: Decimal,
    order_amount: Decimal = Decimal("10"),
    grid_levels: int = 21,
) -> Dict[str, Any]:
    if lower <= 0 or upper <= lower:
        raise ValueError("Нижняя граница должна быть больше нуля и меньше верхней")
    if not order_amount.is_finite() or order_amount <= 0 or order_amount > Decimal("1000"):
        raise ValueError("Размер заявки должен быть больше 0 и не больше 1000 LIT")
    if isinstance(grid_levels, bool) or not isinstance(grid_levels, int) or grid_levels < 2:
        raise ValueError("Количество уровней сетки должно быть целым числом не меньше 2")
    return {
        "script_file_name": "lighter_robinhood_neutral_grid.py",
        "controllers_config": [],
        "enabled": True,
        "connector_name": DOMAIN,
        "trading_pair": "LIT-USDG",
        "lower_price": str(lower),
        "upper_price": str(upper),
        "grid_levels": grid_levels,
        "order_amount_base": str(order_amount),
        "max_abs_net_position": "1000",
        "leverage": 5,
        "max_open_orders": 2,
        "refresh_seconds": 30,
        "max_data_age_seconds": 10,
        "margin_reserve_usdg": str(reserve),
    }


def atomic_write_config(path: Path, config: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_encrypted_credentials(
    path: Path, *, account_index: int, api_key_index: int, api_private_key: str
) -> None:
    """Write with Hummingbot's native secret encryption; caller must unlock Security first."""
    from hummingbot.client.config.config_helpers import ClientConfigAdapter, save_to_yml
    from hummingbot.connector.derivative.lighter_perpetual.lighter_perpetual_utils import (
        LighterPerpetualRobinhoodConfigMap,
    )

    if SecurityManager.current() is None:
        raise RuntimeError("Хранилище Hummingbot не разблокировано")
    model = LighterPerpetualRobinhoodConfigMap(
        lighter_perpetual_robinhood_l1_address=None,
        lighter_perpetual_robinhood_account_index=account_index,
        lighter_perpetual_robinhood_api_key_index=api_key_index,
        lighter_perpetual_robinhood_api_private_key=api_private_key,
        lighter_perpetual_robinhood_account_limit="Standard",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    save_to_yml(path, ClientConfigAdapter(model))
    if not path.exists():
        raise RuntimeError("Hummingbot не сохранил зашифрованные данные")


class SecurityManager:
    @staticmethod
    def current():
        from hummingbot.client.config.security import Security

        return Security.secrets_manager


@dataclass(frozen=True)
class CollectedCredentials:
    account_index: int
    api_key_index: int
    api_private_key: str = field(repr=False)

    def preflight(self) -> PreflightCredentials:
        return PreflightCredentials(self.account_index, self.api_key_index, self.api_private_key)


class Console:
    def ask(self, prompt: str) -> str:
        return input(prompt)

    def hidden(self, prompt: str) -> str:
        return read_hidden(prompt)

    def tell(self, message: str = "") -> None:
        print(message, flush=True)


@dataclass
class Services:
    running: Callable[[], bool]
    new_password_required: Callable[[], bool]
    unlock: Callable[[str], None]
    load_credentials: Callable[[], Optional[CollectedCredentials]]
    persist_credentials: Callable[[CollectedCredentials], None]
    preflight: Callable[[Dict[str, Any], CollectedCredentials], Any]
    launch: Callable[[str], Any]
    running_this: Optional[Callable[[], bool]] = None
    credentials_exist: Optional[Callable[[], bool]] = None


@dataclass(frozen=True)
class LaunchResult:
    returncode: int
    output: str = ""
    matching_process: bool = False
    confirmed_ready: bool = False


def _validate_candidate(candidate: Dict[str, Any]) -> None:
    from scripts.lighter_robinhood_neutral_grid import LighterRobinhoodNeutralGridConfig

    LighterRobinhoodNeutralGridConfig(**candidate)


def _read_existing_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError("Рабочая конфигурация повреждена: ожидался YAML-объект")
    expected = {
        "script_file_name": "lighter_robinhood_neutral_grid.py",
        "controllers_config": [],
        "connector_name": DOMAIN,
        "trading_pair": "LIT-USDG",
        "max_open_orders": 2,
        "leverage": 5,
    }
    if any(loaded.get(key) != value for key, value in expected.items()):
        raise RuntimeError("Существующая конфигурация не совпадает с безопасным LIT-профилем")
    numeric_expected = {
        "max_abs_net_position": Decimal("1000"),
        "refresh_seconds": Decimal("30"),
        "max_data_age_seconds": Decimal("10"),
    }
    try:
        if any(Decimal(str(loaded.get(key))) != value for key, value in numeric_expected.items()):
            raise RuntimeError("Существующая конфигурация изменяет фиксированные защитные параметры")
    except (InvalidOperation, TypeError):
        raise RuntimeError("Существующая конфигурация имеет неверные защитные параметры") from None
    try:
        amount = parse_nonnegative_decimal(str(loaded.get("order_amount_base")), "order_amount_base", positive=True)
        raw_levels = loaded.get("grid_levels")
        if type(raw_levels) is not int or raw_levels < 2:
            raise ValueError("grid_levels должен быть некавыченным целым числом не меньше 2")
        levels = raw_levels
    except ValueError as exc:
        raise RuntimeError(f"Существующая конфигурация: {exc}") from None
    if amount > Decimal("1000") or levels < 2:
        raise RuntimeError("Существующая конфигурация имеет небезопасный размер или число уровней")
    return loaded


def _prompt_decimal(console: Console, label: str, default: Any, *, positive: bool) -> Decimal:
    while True:
        suffix = f" [{default}]" if default not in (None, "") else ""
        value = console.ask(f"{label}{suffix}: ").strip()
        if not value and default not in (None, ""):
            value = str(default)
        try:
            return parse_nonnegative_decimal(value, label, positive=positive)
        except ValueError as exc:
            console.tell(f"Ошибка поля: {exc}. Повторите то же поле.")


def _prompt_index(
    console: Console, prompt: str, label: str, minimum: int, maximum: Optional[int] = None,
    default: Any = None,
) -> int:
    while True:
        suffix = f" [{default}]" if default not in (None, "") else ""
        value = console.ask(f"{prompt}{suffix}: ")
        if not value.strip() and default not in (None, ""):
            value = str(default)
        try:
            return parse_index(value, label, minimum, maximum)
        except ValueError as exc:
            console.tell(f"Ошибка поля: {exc}. Повторите то же поле.")


def _prompt_private_key(console: Console) -> str:
    while True:
        try:
            return normalize_api_private_key(console.hidden("[3/10] API Private Key (скрыто): "))
        except ValueError as exc:
            console.tell(f"Ошибка поля: {exc}. Повторите то же поле.")


def _same_credentials(left: CollectedCredentials, right: Optional[CollectedCredentials]) -> bool:
    if right is None:
        return False
    try:
        return (
            left.account_index == parse_index(str(right.account_index), "Account Index", 0)
            and left.api_key_index == parse_index(str(right.api_key_index), "API Key Index", 4, 254)
            and left.api_private_key == normalize_api_private_key(right.api_private_key)
        )
    except ValueError:
        return False


def run_wizard(services: Services, console: Console, *, config_path: Path = CONFIG_PATH) -> int:
    secrets: list[str] = []
    try:
        if services.running():
            raise RuntimeError("Hummingbot уже запущен. Остановите его отдельным ярлыком и повторите")
        existing_config = _read_existing_config(config_path)
        has_saved_credentials = services.credentials_exist is not None and services.credentials_exist()
        reuse_saved = False
        if has_saved_credentials:
            choice = console.ask(
                "Найден зашифрованный API-ключ. Использовать его без повторного ввода? [Y/n]: "
            ).strip().lower()
            reuse_saved = choice in ("", "y", "yes", "д", "да")

        credentials: Optional[CollectedCredentials] = None
        if not reuse_saved:
            console.tell("[1/10] Account Index — номер аккаунта из Robinhood Lighter.")
            account = _prompt_index(console, "[1/10] Account Index", "Account Index", 0)
            console.tell("[2/10] API Key Index — номер API-ключа; 4 допустим, диапазон 4–254.")
            api_key = _prompt_index(console, "[2/10] API Key Index", "API Key Index", 4, 254)
            console.tell("[3/10] API Private Key — скрытый API-секрет. Public Key и ключ кошелька не нужны.")
            console.tell("При скрытом вводе Terminal не показывает даже точки — это нормально.")
            private = _prompt_private_key(console)
            secrets.extend((private, "0x" + private))
            credentials = CollectedCredentials(account, api_key, private)

        lower = _prompt_decimal(
            console, "[4/10] Нижняя цена LIT", existing_config.get("lower_price"), positive=True
        )
        while True:
            upper = _prompt_decimal(
                console, "[5/10] Верхняя цена LIT", existing_config.get("upper_price"), positive=True
            )
            if upper > lower:
                break
            console.tell("Ошибка поля: верхняя цена должна быть выше нижней. Повторите то же поле.")
        while True:
            order_amount = _prompt_decimal(
                console, "[6/10] Размер одной заявки, LIT", existing_config.get("order_amount_base", 10), positive=True
            )
            if order_amount <= Decimal("1000"):
                break
            console.tell("Ошибка поля: размер заявки не может превышать 1000 LIT. Повторите то же поле.")
        grid_levels = _prompt_index(
            console, "[7/10] Количество уровней сетки", "grid_levels", 2,
            default=existing_config.get("grid_levels", 21),
        )
        console.tell(
            "[8/10] Это дополнительный резерв сверх расчётной маржи полной позиции; "
            "сравнение будет справочным и не заблокирует запуск при известных корректных данных."
        )
        reserve = _prompt_decimal(
            console, "[8/10] Дополнительный резерв USDG (для справочной оценки)",
            existing_config.get("margin_reserve_usdg"), positive=False,
        )
        while console.ask("[9/10] Maker Only — введите OFF: ").strip() != "OFF":
            console.tell("Ошибка поля: Maker Only должен быть выключен. Повторите OFF.")

        first_password = services.new_password_required()
        if first_password:
            console.tell("[10/10] Создайте новый локальный пароль Hummingbot — это не API-ключ и не ключ кошелька.")
        else:
            console.tell("[10/10] Разблокируйте существующее локальное хранилище Hummingbot.")
        console.tell("Скрытый ввод не показывает даже точки — это нормально.")
        while True:
            password = console.hidden("[10/10] Пароль Hummingbot: ")
            secrets.append(password)
            if not password:
                console.tell("Ошибка поля: пароль не может быть пустым. Повторите то же поле.")
                continue
            if first_password:
                confirmation = console.hidden("[10/10] Повторите пароль Hummingbot: ")
                secrets.append(confirmation)
                if password != confirmation:
                    console.tell("Ошибка поля: пароли не совпадают. Повторите этап пароля.")
                    continue
            try:
                services.unlock(password)
                break
            except Exception as exc:
                import typer
                from hummingbot.cli.output import ExitCode

                if isinstance(exc, typer.Exit) and exc.exit_code == int(ExitCode.CONFIG_ERROR):
                    console.tell("Пароль Hummingbot не подошёл. Повторите то же поле; хранилище не сбрасывается.")
                    continue
                raise

        if reuse_saved:
            loaded = services.load_credentials()
            if loaded is None:
                raise RuntimeError("Зашифрованный API-ключ не удалось прочитать")
            credentials = CollectedCredentials(
                parse_index(str(loaded.account_index), "Account Index", 0),
                parse_index(str(loaded.api_key_index), "API Key Index", 4, 254),
                normalize_api_private_key(loaded.api_private_key),
            )
            secrets.extend((credentials.api_private_key, "0x" + credentials.api_private_key))
        needs_persist = not reuse_saved
        candidate = build_candidate(lower, upper, reserve, order_amount, grid_levels)
        _validate_candidate(candidate)
        if services.running():
            raise RuntimeError("Hummingbot был запущен в другом окне; конфигурация не изменена")
        atomic_write_config(config_path, {**candidate, "enabled": False})

        console.tell("Выполняется приватная проверка только для чтения…")
        report = services.preflight(candidate, credentials)
        console.tell(report.summary())
        if report.live_ready is not True:
            raise RuntimeError("Preflight не подтвердил LIVE READY")

        if needs_persist:
            if services.running():
                raise RuntimeError("Hummingbot был запущен в другом окне; ключи не изменены")
            services.persist_credentials(credentials)
        reloaded = services.load_credentials()
        if not _same_credentials(credentials, reloaded):
            raise RuntimeError("Проверка зашифрованного сохранения не совпала с введёнными индексами/ключом")

        required = getattr(report, "required_margin_usdg", None)
        console.tell(
            f"Готово: account {credentials.account_index}, LIT {lower}…{upper}, заявка {order_amount} LIT, "
            f"уровней {grid_levels}, лимит 1000 LIT, плечо 5x, "
            f"справочная оценка маржи USDG {required if required is not None else 'проверена preflight'}."
        )
        if console.ask("Для реального запуска ордеров введите START: ").strip() != "START":
            console.tell("Запуск отменён. Конфигурация осталась выключенной.")
            return 0
        if services.running():
            raise RuntimeError("Перед запуском обнаружен уже работающий Hummingbot")
        console.tell("Обновляю read-only проверку непосредственно перед запуском…")
        refreshed = services.preflight(candidate, reloaded)
        console.tell(refreshed.summary())
        if refreshed.live_ready is not True:
            raise RuntimeError("Повторный preflight не подтвердил LIVE READY")
        if services.running():
            raise RuntimeError("Hummingbot был запущен в другом окне; запуск отменён")
        atomic_write_config(config_path, candidate)
        try:
            launch_result = services.launch(password)
        except (KeyboardInterrupt, subprocess.TimeoutExpired):
            if services.running_this is not None and services.running_this():
                console.tell("Результат команды неоднозначен, но Hummingbot работает. Проверьте ярлыком «Статус LIT бота».")
                return 2
            atomic_write_config(config_path, {**candidate, "enabled": False})
            raise RuntimeError("Запуск был прерван; работающий процесс Hummingbot не обнаружен") from None
        except Exception as exc:
            if services.running_this is not None and services.running_this():
                console.tell("Запуск завершился неясно, но Hummingbot работает. Сначала проверьте статус.")
                return 2
            atomic_write_config(config_path, {**candidate, "enabled": False})
            raise RuntimeError(f"Ошибка запуска: {redact_message(exc, secrets)}") from None
        return_code = launch_result.returncode if isinstance(launch_result, LaunchResult) else launch_result
        output = launch_result.output if isinstance(launch_result, LaunchResult) else ""
        if output:
            console.tell(redact_message(output.strip(), secrets))
        if isinstance(launch_result, LaunchResult) and return_code == 0 and not launch_result.confirmed_ready:
            if launch_result.matching_process:
                console.tell("Команда принята, но свежая готовность стратегии не подтверждена. Проверьте статус.")
                return 2
            atomic_write_config(config_path, {**candidate, "enabled": False})
            raise RuntimeError("Команда запуска не подтвердила новый процесс этого бота")
        if return_code != 0:
            matching_process = (
                launch_result.matching_process if isinstance(launch_result, LaunchResult)
                else services.running_this is not None and services.running_this()
            )
            if matching_process:
                console.tell("Команда вернула ошибку, но новый процесс этого LIT-бота работает. Проверьте статус.")
                return 2
            atomic_write_config(config_path, {**candidate, "enabled": False})
            raise RuntimeError(f"Hummingbot не запустился (код {return_code})")
        console.tell("Бот запущен отдельно от этого окна. Закрытие окна его не остановит.")
        console.tell("Используйте «Статус LIT бота» и «Остановить LIT бота». Остановка не закрывает позицию.")
        return 0
    except KeyboardInterrupt:
        console.tell("\nОтменено пользователем. Если запуск уже начался, проверьте «Статус LIT бота».")
        return 130
    except Exception as exc:
        console.tell("Ошибка: " + redact_message(exc, secrets))
        return 1


def redact_message(value: Any, secrets: Iterable[str]) -> str:
    return redact(value, secrets)


async def _authenticated_preflight(candidate: Dict[str, Any], credentials: CollectedCredentials):
    async with aiohttp.ClientSession() as session:
        report = await run_public_preflight(
            AiohttpPublicClient(session), SdkProbe(), config=candidate,
            max_data_age=float(candidate["max_data_age_seconds"]),
        )
        if report.public_ready and report.market is not None:
            snapshot = await AuthenticatedReadClient(session, credentials.preflight()).snapshot(report.market.market_id)
            report = evaluate_private_snapshot(
                report, credentials.preflight(), snapshot, Decimal(candidate["margin_reserve_usdg"])
            )
        return report


def _load_credentials() -> Optional[CollectedCredentials]:
    from hummingbot.client.config.security import Security

    values = Security.api_keys(DOMAIN)
    if not values:
        return None
    return CollectedCredentials(
        values["lighter_perpetual_robinhood_account_index"],
        values["lighter_perpetual_robinhood_api_key_index"],
        values["lighter_perpetual_robinhood_api_private_key"],
    )


def _credentials_exist() -> bool:
    from hummingbot.client.config.config_helpers import get_connector_config_yml_path

    return get_connector_config_yml_path(DOMAIN).is_file()


def _persist_credentials(credentials: CollectedCredentials) -> None:
    from hummingbot.client.config.config_helpers import (
        api_keys_from_connector_config_map,
        get_connector_config_yml_path,
        load_connector_config_map_from_file,
    )
    from hummingbot.client.config.security import Security

    destination = get_connector_config_yml_path(DOMAIN)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    previous_bytes = destination.read_bytes() if destination.exists() else None
    previous_mode = destination.stat().st_mode & 0o777 if destination.exists() else None
    previous_cache = Security.decrypted_value(DOMAIN)
    try:
        write_encrypted_credentials(
            temporary,
            account_index=credentials.account_index,
            api_key_index=credentials.api_key_index,
            api_private_key=credentials.api_private_key,
        )
        staged = api_keys_from_connector_config_map(load_connector_config_map_from_file(temporary))
        staged_credentials = CollectedCredentials(
            staged["lighter_perpetual_robinhood_account_index"],
            staged["lighter_perpetual_robinhood_api_key_index"],
            staged["lighter_perpetual_robinhood_api_private_key"],
        )
        if not _same_credentials(credentials, staged_credentials):
            raise RuntimeError("Зашифрованный временный файл не прошёл проверку")
        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
        try:
            Security.decrypt_connector_config(destination)
            if not _same_credentials(credentials, _load_credentials()):
                raise RuntimeError("Повторное чтение зашифрованных данных не совпало")
        except Exception:
            if previous_bytes is None:
                destination.unlink(missing_ok=True)
                Security._secure_configs.pop(DOMAIN, None)
            else:
                restore = destination.with_name(f".{destination.name}.{os.getpid()}.restore")
                restore.write_bytes(previous_bytes)
                os.chmod(restore, previous_mode or 0o600)
                os.replace(restore, destination)
                if previous_cache is not None:
                    Security._secure_configs[DOMAIN] = previous_cache
                else:
                    Security._secure_configs.pop(DOMAIN, None)
            raise
    finally:
        temporary.unlink(missing_ok=True)


def _matching_bot(started_after: float = 0, prior_pid: Optional[int] = None, require_ready: bool = False) -> bool:
    from hummingbot.cli import bot

    pid = bot.read_pid()
    meta = bot.read_meta() or {}
    if (pid is None or pid == prior_pid or not bot.running()
            or meta.get("pid") != pid or meta.get("name") != CONFIG_PATH.stem
            or meta.get("type") != "v2-script" or meta.get("file") != CONFIG_NAME
            or meta.get("script_config") != CONFIG_NAME
            or not isinstance(meta.get("started_at"), (int, float))
            or meta["started_at"] < started_after):
        return False
    if not require_ready:
        return True
    status = bot.read_status() or {}
    engine = status.get("engine")
    return (
        status.get("pid") == pid
        and status.get("name") == CONFIG_PATH.stem
        and status.get("running") is True
        and isinstance(status.get("updated_at"), (int, float))
        and status["updated_at"] >= started_after
        and isinstance(engine, dict)
        and engine.get("strategy_running") is True
    )


def _launch(password: str) -> LaunchResult:
    from hummingbot.cli import bot

    started_at = time.time()
    prior_pid = bot.read_pid()
    command = [
        sys.executable, "-m", "hummingbot.cli.main", "start", CONFIG_NAME,
        "--v2-script", "--password-stdin",
    ]
    completed = subprocess.run(
        command, cwd=ROOT, input=password + "\n", text=True, timeout=135,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    matching = _matching_bot(started_at, prior_pid, require_ready=False)
    ready = matching and _matching_bot(started_at, prior_pid, require_ready=True)
    return LaunchResult(completed.returncode, completed.stdout or "", matching, ready)


def _default_services() -> Services:
    from hummingbot.cli import bot
    from hummingbot.cli.password import unlock_keystore
    from hummingbot.client.config.security import Security

    return Services(
        running=bot.running,
        new_password_required=Security.new_password_required,
        unlock=unlock_keystore,
        load_credentials=_load_credentials,
        persist_credentials=_persist_credentials,
        preflight=lambda candidate, credentials: asyncio.run(_authenticated_preflight(candidate, credentials)),
        launch=_launch,
        running_this=lambda: _matching_bot(time.time() - 180, require_ready=False),
        credentials_exist=_credentials_exist,
    )


def main() -> int:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open("a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Другой мастер запуска уже открыт. Используйте только одно окно.")
            return 1
        return run_wizard(_default_services(), Console())


if __name__ == "__main__":
    raise SystemExit(main())
