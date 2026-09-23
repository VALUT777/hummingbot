from decimal import Decimal
import getpass
import warnings

import pytest
import yaml

from bin.lighter_robinhood_setup import (
    CollectedCredentials,
    LaunchResult,
    Services,
    SecretInputUnavailable,
    atomic_write_config,
    build_candidate,
    normalize_api_private_key,
    parse_index,
    _persist_credentials,
    read_hidden,
    redact_message,
    run_wizard,
    write_encrypted_credentials,
)
from hummingbot.client.config.config_crypt import ETHKeyFileSecretManger
from hummingbot.client.config.config_helpers import load_connector_config_map_from_file
from hummingbot.client.config.security import Security


@pytest.mark.parametrize("value,minimum,maximum", [("0", 0, None), ("4", 4, 254), ("254", 4, 254)])
def test_parse_index_accepts_only_canonical_integers(value, minimum, maximum):
    assert parse_index(value, "index", minimum, maximum) == int(value)


@pytest.mark.parametrize("value", ["", "01", "+4", "4.0", " 4", "4 ", "255", "True"])
def test_api_index_rejects_ambiguous_or_out_of_range_input(value):
    with pytest.raises(ValueError):
        parse_index(value, "API Key Index", 4, 254)


def test_private_key_accepts_optional_0x_and_requires_32_byte_hex():
    raw = "a1" * 32
    assert normalize_api_private_key(raw) == raw
    assert normalize_api_private_key("0x" + raw) == raw
    for bad in ("", "0x", "ab" * 31, "gg" * 32):
        with pytest.raises(ValueError):
            normalize_api_private_key(bad)


def test_hidden_input_refuses_non_tty_without_echo_fallback():
    class NotATty:
        def isatty(self):
            return False

    with pytest.raises(SecretInputUnavailable):
        read_hidden("secret", stdin=NotATty(), getter=lambda _: pytest.fail("must not prompt"))


def test_hidden_input_rejects_getpass_echo_fallback_warning():
    class Tty:
        def isatty(self):
            return True

    def unsafe_getter(_):
        warnings.warn("echo fallback", getpass.GetPassWarning)
        return "would-have-echoed"

    with pytest.raises(SecretInputUnavailable):
        read_hidden("secret", stdin=Tty(), getter=unsafe_getter)


def test_candidate_is_live_in_memory_while_persisted_config_stays_disabled(tmp_path):
    path = tmp_path / "grid.yml"
    candidate = build_candidate(Decimal("4.5"), Decimal("6.5"), Decimal("100"))
    assert candidate["enabled"] is True

    atomic_write_config(path, {**candidate, "enabled": False})

    saved = yaml.safe_load(path.read_text())
    assert saved["enabled"] is False
    assert saved["lower_price"] == "4.5"
    assert saved["margin_reserve_usdg"] == "100"


def test_actual_hummingbot_encryption_round_trip_contains_no_plaintext_key(tmp_path):
    path = tmp_path / "lighter_perpetual_robinhood.yml"
    private_key = "ab" * 32
    old_manager = Security.secrets_manager
    try:
        Security.secrets_manager = ETHKeyFileSecretManger("local-test-password")
        write_encrypted_credentials(path, account_index=0, api_key_index=4, api_private_key=private_key)
        raw = path.read_text()
        assert private_key not in raw
        loaded = load_connector_config_map_from_file(path)
        assert loaded.lighter_perpetual_robinhood_account_index == 0
        assert loaded.lighter_perpetual_robinhood_api_key_index == 4
        assert loaded.lighter_perpetual_robinhood_api_private_key.get_secret_value() == private_key
    finally:
        Security.secrets_manager = old_manager


def test_redaction_covers_plain_and_0x_key_in_exception_text():
    key = "cd" * 32
    message = redact_message(RuntimeError(f"native signer rejected 0x{key} and {key}"), [key])
    assert key not in message
    assert "[REDACTED]" in message


class ScriptedConsole:
    def __init__(self, answers, hidden):
        self.answers = iter(answers)
        self.secrets = iter(hidden)
        self.output = []

    def ask(self, prompt):
        return next(self.answers)

    def hidden(self, prompt):
        return next(self.secrets)

    def tell(self, message=""):
        self.output.append(message)


class Report:
    def __init__(self, ready):
        self.live_ready = ready
        self.required_margin_usdg = Decimal("1400")

    def summary(self):
        return "LIVE READY" if self.live_ready else "NOT LIVE-READY"


def _answers(start="START"):
    return ["0", "4", "OFF", "4.5", "6.5", "100", start]


def test_failed_preflight_keeps_config_disabled_and_does_not_replace_credentials(tmp_path):
    config = tmp_path / "grid.yml"
    persisted = []
    launched = []
    services = Services(
        running=lambda: False,
        new_password_required=lambda: True,
        unlock=lambda password: None,
        load_credentials=lambda: None,
        persist_credentials=persisted.append,
        preflight=lambda candidate, credentials: Report(False),
        launch=lambda password: launched.append(password) or 0,
    )
    console = ScriptedConsole(_answers(), ["storage-password", "storage-password", "ab" * 32])

    assert run_wizard(services, console, config_path=config) == 1
    assert yaml.safe_load(config.read_text())["enabled"] is False
    assert persisted == []
    assert launched == []


def test_start_requires_two_live_preflights_and_reloaded_encrypted_credentials(tmp_path):
    config = tmp_path / "grid.yml"
    stored = []
    launches = []
    preflights = []

    def load():
        return stored[-1] if stored else None

    def preflight(candidate, credentials):
        preflights.append((dict(candidate), credentials))
        assert candidate["enabled"] is True
        return Report(True)

    def launch(password):
        assert yaml.safe_load(config.read_text())["enabled"] is True
        launches.append(password)
        return LaunchResult(0, confirmed_ready=True)

    services = Services(
        running=lambda: False,
        new_password_required=lambda: True,
        unlock=lambda password: None,
        load_credentials=load,
        persist_credentials=stored.append,
        preflight=preflight,
        launch=launch,
    )
    console = ScriptedConsole(_answers(), ["storage-password", "storage-password", "ab" * 32])

    assert run_wizard(services, console, config_path=config) == 0
    assert len(preflights) == 2
    assert stored == [CollectedCredentials(0, 4, "ab" * 32)]
    assert launches == ["storage-password"]


def test_second_preflight_failure_never_enables_disk_config(tmp_path):
    config = tmp_path / "grid.yml"
    credential = CollectedCredentials(0, 4, "ab" * 32)
    reports = iter([Report(True), Report(False)])
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        persist_credentials=lambda value: pytest.fail("existing credentials must not be rewritten"),
        preflight=lambda candidate, credentials: next(reports),
        launch=lambda password: pytest.fail("must not launch"),
    )
    console = ScriptedConsole(["", "OFF", "4.5", "6.5", "100", "START"], ["storage-password"])

    assert run_wizard(services, console, config_path=config) == 1
    assert yaml.safe_load(config.read_text())["enabled"] is False


def test_own_enabled_config_from_previous_stopped_run_can_be_reused(tmp_path):
    config = tmp_path / "grid.yml"
    config.write_text(yaml.safe_dump(build_candidate(Decimal("4.5"), Decimal("6.5"), Decimal("100"))))
    credential = CollectedCredentials(0, 4, "ab" * 32)
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        persist_credentials=lambda value: pytest.fail("must reuse"),
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: pytest.fail("canceled before launch"),
    )
    console = ScriptedConsole(["", "OFF", "", "", "", "cancel"], ["storage-password"])

    assert run_wizard(services, console, config_path=config) == 0
    assert yaml.safe_load(config.read_text())["enabled"] is False


def test_launch_exception_rolls_back_enabled_and_redacts_secret(tmp_path):
    config = tmp_path / "grid.yml"
    credential = CollectedCredentials(0, 4, "ab" * 32)
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        persist_credentials=lambda value: pytest.fail("must reuse"),
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: (_ for _ in ()).throw(OSError(f"failed with {credential.api_private_key}")),
    )
    console = ScriptedConsole(["", "OFF", "4.5", "6.5", "100", "START"], ["storage-password"])

    assert run_wizard(services, console, config_path=config) == 1
    assert yaml.safe_load(config.read_text())["enabled"] is False
    assert credential.api_private_key not in "\n".join(console.output)


def test_launch_output_is_redacted_before_display(tmp_path):
    config = tmp_path / "grid.yml"
    credential = CollectedCredentials(0, 4, "ab" * 32)
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        persist_credentials=lambda value: None,
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: LaunchResult(0, f"native output {credential.api_private_key}", True, True),
    )
    console = ScriptedConsole(["", "OFF", "4.5", "6.5", "100", "START"], ["storage-password"])

    assert run_wizard(services, console, config_path=config) == 0
    output = "\n".join(console.output)
    assert credential.api_private_key not in output
    assert "[REDACTED]" in output


def test_zero_return_without_fresh_matching_process_is_not_reported_started(tmp_path):
    config = tmp_path / "grid.yml"
    credential = CollectedCredentials(0, 4, "ab" * 32)
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        persist_credentials=lambda value: None,
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: LaunchResult(0, "accepted", False, False),
    )
    console = ScriptedConsole(["", "OFF", "4.5", "6.5", "100", "START"], ["storage-password"])

    assert run_wizard(services, console, config_path=config) == 1
    assert yaml.safe_load(config.read_text())["enabled"] is False
    assert "Бот запущен" not in "\n".join(console.output)


def test_nonzero_launch_does_not_trust_unrelated_running_bot(tmp_path):
    config = tmp_path / "grid.yml"
    credential = CollectedCredentials(0, 4, "ab" * 32)
    running_calls = 0

    def unrelated_starts_after_launch():
        nonlocal running_calls
        running_calls += 1
        return running_calls >= 5

    services = Services(
        running=unrelated_starts_after_launch,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        persist_credentials=lambda value: None,
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: LaunchResult(1, "failed", False, False),
        running_this=lambda: False,
    )
    console = ScriptedConsole(["", "OFF", "4.5", "6.5", "100", "START"], ["storage-password"])

    assert run_wizard(services, console, config_path=config) == 1
    assert yaml.safe_load(config.read_text())["enabled"] is False


def test_nonzero_launch_with_exact_matching_process_is_reported_uncertain(tmp_path):
    config = tmp_path / "grid.yml"
    credential = CollectedCredentials(0, 4, "ab" * 32)
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        persist_credentials=lambda value: None,
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: LaunchResult(1, "timeout", True, False),
    )
    console = ScriptedConsole(["", "OFF", "4.5", "6.5", "100", "START"], ["storage-password"])

    assert run_wizard(services, console, config_path=config) == 2
    assert yaml.safe_load(config.read_text())["enabled"] is True
    assert "новый процесс этого LIT-бота" in "\n".join(console.output)


def test_credential_replace_restores_previous_encrypted_file_on_reload_failure(tmp_path, monkeypatch):
    import hummingbot.client.config.config_helpers as helpers

    destination = tmp_path / "lighter_perpetual_robinhood.yml"
    old = CollectedCredentials(0, 4, "11" * 32)
    new = CollectedCredentials(1, 5, "22" * 32)
    old_manager = Security.secrets_manager
    old_cache = Security._secure_configs.copy()
    try:
        Security.secrets_manager = ETHKeyFileSecretManger("local-test-password")
        write_encrypted_credentials(
            destination, account_index=old.account_index,
            api_key_index=old.api_key_index, api_private_key=old.api_private_key,
        )
        previous = destination.read_bytes()
        monkeypatch.setattr(helpers, "get_connector_config_yml_path", lambda domain: destination)
        monkeypatch.setattr(Security, "decrypt_connector_config", lambda path: (_ for _ in ()).throw(RuntimeError("boom")))

        with pytest.raises(RuntimeError, match="boom"):
            _persist_credentials(new)

        assert destination.read_bytes() == previous
        loaded = load_connector_config_map_from_file(destination)
        assert loaded.lighter_perpetual_robinhood_api_private_key.get_secret_value() == old.api_private_key
    finally:
        Security.secrets_manager = old_manager
        Security._secure_configs = old_cache


def test_credential_replace_restores_previous_file_on_post_replace_mismatch(tmp_path, monkeypatch):
    import bin.lighter_robinhood_setup as setup
    import hummingbot.client.config.config_helpers as helpers

    destination = tmp_path / "lighter_perpetual_robinhood.yml"
    old = CollectedCredentials(0, 4, "33" * 32)
    new = CollectedCredentials(1, 5, "44" * 32)
    old_manager = Security.secrets_manager
    old_cache = Security._secure_configs.copy()
    try:
        Security.secrets_manager = ETHKeyFileSecretManger("local-test-password")
        write_encrypted_credentials(
            destination, account_index=old.account_index,
            api_key_index=old.api_key_index, api_private_key=old.api_private_key,
        )
        previous = destination.read_bytes()
        monkeypatch.setattr(helpers, "get_connector_config_yml_path", lambda domain: destination)
        monkeypatch.setattr(Security, "decrypt_connector_config", lambda path: None)
        monkeypatch.setattr(setup, "_load_credentials", lambda: old)

        with pytest.raises(RuntimeError, match="не совпало"):
            _persist_credentials(new)

        assert destination.read_bytes() == previous
    finally:
        Security.secrets_manager = old_manager
        Security._secure_configs = old_cache
