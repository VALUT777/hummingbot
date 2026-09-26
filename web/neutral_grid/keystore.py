"""Keystore profile selection and unlock through the existing Hummingbot encrypted keystore.

Nothing here implements cryptography: unlock is ``Security.login(ETHKeyFileSecretManger(password))``
exactly like the Hummingbot client, and the API-key format check reuses
``bin.lighter_robinhood_setup.normalize_api_private_key`` (80 hex, optional ``0x``; a 64-hex wallet key
is rejected). The service exposes only *masked presence* of each connect-key field: it never returns,
logs or echoes a password, an encrypted blob or a decrypted value, and there is no readback method.
The keystore is never created from the web backend (use the setup wizard for that).
"""
from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional

LOGGER = logging.getLogger("web.neutral_grid.keystore")

ROBINHOOD_PROFILE = "lighter_perpetual_robinhood"
ROBINHOOD_KEY_FIELD = "lighter_perpetual_robinhood_api_private_key"
_DEMO_REFUSAL = "Демо-режим работает на офлайн fake exchange: ключи не используются и не читаются."
_MAX_FAILURES = 5
_FAILURE_WINDOW_S = 300.0


class KeystoreError(Exception):
    """User-facing error; messages never contain secret material."""


def _connectors_dir() -> Path:
    from hummingbot.client.config import config_helpers
    return Path(config_helpers.CONNECTORS_CONF_DIR_PATH)


class KeystoreService:
    def __init__(self, *, connectors_dir: Optional[Callable[[], Path]] = None,
                 clock: Callable[[], float] = time.time, demo: bool = False):
        self._connectors_dir = connectors_dir or _connectors_dir
        self._clock = clock
        self._selected: Optional[str] = None
        self._unlocked_profile: Optional[str] = None
        self._key_format_valid: Optional[bool] = None
        self._failures: List[float] = []
        self._lock = threading.Lock()
        self.demo = demo

    # ----------------------------------------------------------------- masked presence
    def profiles(self) -> List[Dict[str, object]]:
        from hummingbot.client.config.config_helpers import read_yml_file

        if self.demo:
            return []  # the offline demo never reads the real keystore
        directory = self._connectors_dir()
        result: List[Dict[str, object]] = []
        if not directory.is_dir():
            return result
        for path in sorted(directory.iterdir()):
            if not path.is_file() or path.suffix != ".yml" or path.name.startswith((".", "_")):
                continue
            try:
                data = read_yml_file(path)
            except Exception:
                result.append({"name": path.stem, "readable": False, "fields": {}})
                continue
            name = str(data.get("connector") or path.stem)
            fields = {
                str(key): ("задано" if value not in (None, "") else "нет")
                for key, value in data.items() if key != "connector"
            }
            result.append({"name": name, "readable": True, "fields": fields})
        return result

    def select(self, name: object) -> Dict[str, object]:
        if self.demo:
            raise KeystoreError(_DEMO_REFUSAL)
        if not isinstance(name, str) or not name:
            raise KeystoreError("Не указано имя профиля.")
        names = {p["name"] for p in self.profiles()}
        if name not in names:
            raise KeystoreError("Профиль с таким именем не найден в зашифрованном keystore.")
        with self._lock:
            self._selected = name
            if self._unlocked_profile is not None and self._unlocked_profile != name:
                self._key_format_valid = None
        return self.status()

    def status(self) -> Dict[str, object]:
        with self._lock:
            return {
                "demo": self.demo,
                "keystore_exists": None if self.demo else self._keystore_exists(),
                "selected_profile": self._selected,
                "unlocked": self._unlocked_profile is not None and self._unlocked_profile == self._selected,
                "api_key_format": (
                    "unknown" if self._key_format_valid is None
                    else ("valid" if self._key_format_valid else "invalid")
                ),
                "locked_out": self._locked_out(),
            }

    # ----------------------------------------------------------------- unlock
    def unlock(self, password: object) -> Dict[str, object]:
        """Unlock via the native keystore; ``password`` is used once and never stored or echoed."""
        if self.demo:
            raise KeystoreError(_DEMO_REFUSAL)
        if not isinstance(password, str) or not password:
            raise KeystoreError("Пароль не передан.")
        with self._lock:
            if self._selected is None:
                raise KeystoreError("Сначала выберите профиль ключей.")
            if self._locked_out():
                raise KeystoreError("Слишком много неудачных попыток. Подождите несколько минут.")
            if not self._keystore_exists():
                raise KeystoreError(
                    "Keystore Hummingbot ещё не создан. Создайте его мастером bin/lighter_robinhood_setup.py; "
                    "веб-интерфейс keystore не создаёт.")
            ok = self._login(password)
            del password  # used exactly once; never stored on self, never logged
            if not ok:
                self._failures.append(self._clock())
                LOGGER.warning("Keystore unlock rejected (wrong password).")
                raise KeystoreError("Неверный пароль keystore.")
            self._unlocked_profile = self._selected
            self._key_format_valid = self._check_key_format(self._selected)
            LOGGER.info("Keystore unlocked for profile %s (key format %s).", self._selected,
                        "valid" if self._key_format_valid else "invalid/unknown")
        return self.status()

    def unlocked_profile(self) -> Optional[str]:
        with self._lock:
            return self._unlocked_profile

    # ----------------------------------------------------------------- internals
    def _locked_out(self) -> bool:
        now = self._clock()
        self._failures = [t for t in self._failures if t > now - _FAILURE_WINDOW_S]
        return len(self._failures) >= _MAX_FAILURES

    @staticmethod
    def _keystore_exists() -> bool:
        from hummingbot.client.config.security import Security
        return not Security.new_password_required()

    @staticmethod
    def _login(password: str) -> bool:
        from hummingbot.client.config.config_crypt import ETHKeyFileSecretManger
        from hummingbot.client.config.security import Security
        try:
            return bool(Security.login(ETHKeyFileSecretManger(password)))
        except Exception:
            # Never include exception text: third-party errors may quote their inputs.
            LOGGER.warning("Keystore unlock failed with an internal error (details suppressed).")
            raise KeystoreError("Не удалось расшифровать keystore (подробности скрыты).") from None

    @staticmethod
    def _check_key_format(profile: str) -> Optional[bool]:
        if profile != ROBINHOOD_PROFILE:
            return None
        from hummingbot.client.config.security import Security
        try:
            value = Security.api_keys(profile).get(ROBINHOOD_KEY_FIELD)
        except Exception:
            return False
        if not value:
            return False
        from bin.lighter_robinhood_setup import normalize_api_private_key
        try:
            normalize_api_private_key(str(value))
            return True
        except ValueError:
            return False
