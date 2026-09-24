from decimal import Decimal
import getpass
import warnings

import pytest
import typer
import yaml
from lighter.signer_client import create_api_key
from bin.lighter_robinhood_preflight import PreflightReport

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


@pytest.mark.parametrize(
    "value,minimum,maximum", [("0", 0, None), (" 4 ", 4, 254), ("254", 4, 254)]
)
def test_parse_index_accepts_only_canonical_integers(value, minimum, maximum):
    assert parse_index(value, "index", minimum, maximum) == int(value)


@pytest.mark.parametrize("value", ["", "01", "+4", "4.0", "255", "True"])
def test_api_index_rejects_ambiguous_or_out_of_range_input(value):
    with pytest.raises(ValueError):
        parse_index(value, "API Key Index", 4, 254)


def test_private_key_accepts_sdk_generated_40_byte_key_without_network():
    private_key, public_key, error = create_api_key()
    assert error is None
    assert len(private_key.removeprefix("0x")) == 80
    assert len(public_key.removeprefix("0x")) == 80
    assert normalize_api_private_key(private_key) == private_key.removeprefix("0x").lower()


def test_private_key_accepts_optional_prefix_case_and_whitespace_but_requires_exact_80_hex():
    raw = "a1" * 40
    assert normalize_api_private_key(raw) == raw
    assert normalize_api_private_key("  0X" + raw.upper() + "  ") == raw
    for bad in ("", "0x", "ab" * 32, "ab" * 39, "ab" * 41, "gg" * 40):
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


def test_candidate_preserves_custom_order_size_and_integer_grid_levels():
    candidate = build_candidate(
        Decimal("4.5"), Decimal("6.5"), Decimal("100"), Decimal("12.5"), 31
    )

    assert candidate["order_amount_base"] == "12.5"
    assert candidate["grid_levels"] == 31
    assert type(candidate["grid_levels"]) is int


def test_wizard_accepts_live_ready_report_with_margin_warning(tmp_path):
    credential = CollectedCredentials(0, 4, "ab" * 40)
    report = PreflightReport(private_checked=True, required_margin_usdg=Decimal("2100"))
    report.add("PASS", "domain", "ok")
    report.add("PASS", "private.identity", "ok")
    report.add("WARN", "private.margin", "informational shortfall")
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        persist_credentials=lambda value: pytest.fail("must reuse"),
        preflight=lambda candidate, credentials: report,
        launch=lambda password: pytest.fail("user canceled before launch"),
        credentials_exist=lambda: True,
    )
    console = ScriptedConsole(
        ["", "5", "5.5", "10", "25", "1000", "OFF", "cancel"],
        ["storage-password"],
    )

    assert run_wizard(services, console, config_path=tmp_path / "grid.yml") == 0
    assert "LIVE READY WITH MARGIN WARNING" in "\n".join(console.output)


def test_default_saved_credentials_reuse_skips_all_api_key_fields(tmp_path):
    credential = CollectedCredentials(0, 4, "ab" * 40)
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        persist_credentials=lambda value: pytest.fail("saved credentials must not be rewritten"),
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: pytest.fail("user canceled before launch"),
        credentials_exist=lambda: True,
    )
    console = ScriptedConsole(
        ["", "5", "5.5", "10", "25", "1000", "OFF", "cancel"],
        ["storage-password"],
    )

    assert run_wizard(services, console, config_path=tmp_path / "grid.yml") == 0
    prompts = [prompt for kind, prompt in console.events if kind in ("ask", "hidden")]
    assert not any("Account Index" in prompt for prompt in prompts)
    assert not any("API Key Index" in prompt for prompt in prompts)
    assert not any("API Private Key" in prompt for prompt in prompts)
    assert prompts.count("[10/10] Пароль Hummingbot: ") == 1
    assert any("вводить ключ и индексы заново не нужно" in message for message in console.output)


def test_actual_hummingbot_encryption_round_trip_contains_no_plaintext_key(tmp_path):
    path = tmp_path / "lighter_perpetual_robinhood.yml"
    private_key = "ab" * 40
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
    key = "cd" * 40
    message = redact_message(RuntimeError(f"native signer rejected 0x{key} and {key}"), [key])
    assert key not in message
    assert "[REDACTED]" in message


class ScriptedConsole:
    def __init__(self, answers, hidden):
        self.answers = iter(answers)
        self.secrets = iter(hidden)
        self.output = []
        self.events = []

    def ask(self, prompt):
        self.events.append(("ask", prompt))
        return next(self.answers)

    def hidden(self, prompt):
        self.events.append(("hidden", prompt))
        return next(self.secrets)

    def tell(self, message=""):
        self.events.append(("tell", message))
        self.output.append(message)


def test_new_user_prompts_one_field_at_a_time_retrying_only_invalid_field(tmp_path):
    config = tmp_path / "grid.yml"
    stored = []
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: stored[-1] if stored else None,
        persist_credentials=stored.append,
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: pytest.fail("user canceled before launch"),
    )
    services.credentials_exist = lambda: False
    console = ScriptedConsole(
        ["bad-index", "0", " 4 ", "4.5", "6.5", "10", "21", "100", "OFF", "cancel"],
        ["not-a-private-key", "  " + "ab" * 40 + "  ", "storage-password"],
    )

    assert run_wizard(services, console, config_path=config) == 0
    prompts = [(kind, text) for kind, text in console.events if kind in ("ask", "hidden")]
    labels = [text for _, text in prompts]
    assert labels[:6] == [
        "[1/10] Account Index: ",
        "[1/10] Account Index: ",
        "[2/10] API Key Index: ",
        "[3/10] API Private Key (скрыто): ",
        "[3/10] API Private Key (скрыто): ",
        "[4/10] Нижняя цена LIT: ",
    ]
    assert labels.index("[10/10] Пароль Hummingbot: ") > labels.index("[9/10] Maker Only — введите OFF: ")
    assert stored == [CollectedCredentials(0, 4, "ab" * 40)]


def test_wrong_keystore_password_retries_after_all_values_without_reasking_key(tmp_path):
    config = tmp_path / "grid.yml"
    stored = []
    attempts = []

    def unlock(password):
        attempts.append(password)
        if password == "wrong-password":
            raise typer.Exit(code=4)

    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=unlock,
        load_credentials=lambda: stored[-1] if stored else None,
        persist_credentials=stored.append,
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: pytest.fail("user canceled before launch"),
        credentials_exist=lambda: False,
    )
    console = ScriptedConsole(
        ["0", "4", "4.5", "6.5", "10", "21", "100", "OFF", "cancel"],
        ["ab" * 40, "wrong-password", "correct-password"],
    )

    assert run_wizard(services, console, config_path=config) == 0
    assert attempts == ["wrong-password", "correct-password"]
    assert [event for event in console.events if event == ("hidden", "[3/10] API Private Key (скрыто): ")] == [
        ("hidden", "[3/10] API Private Key (скрыто): ")
    ]


def test_fatal_keystore_error_does_not_loop_or_reset(tmp_path):
    attempts = []

    def unlock(password):
        attempts.append(password)
        raise OSError("keystore permission denied")

    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=unlock,
        load_credentials=lambda: None,
        persist_credentials=lambda value: pytest.fail("must not persist"),
        preflight=lambda candidate, credentials: pytest.fail("must not preflight"),
        launch=lambda password: pytest.fail("must not launch"),
        credentials_exist=lambda: False,
    )
    console = ScriptedConsole(
        ["0", "4", "4.5", "6.5", "10", "21", "100", "OFF"],
        ["ab" * 40, "storage-password"],
    )

    assert run_wizard(services, console, config_path=tmp_path / "grid.yml") == 1
    assert attempts == ["storage-password"]
    assert "permission denied" in "\n".join(console.output)


def test_cancel_at_private_key_never_unlocks_or_starts(tmp_path):
    config = tmp_path / "grid.yml"

    class CancelAtPrivateKey(ScriptedConsole):
        def hidden(self, prompt):
            self.events.append(("hidden", prompt))
            raise KeyboardInterrupt

    services = Services(
        running=lambda: False,
        new_password_required=lambda: pytest.fail("password stage must not be reached"),
        unlock=lambda password: pytest.fail("must not unlock"),
        load_credentials=lambda: None,
        persist_credentials=lambda value: pytest.fail("must not persist"),
        preflight=lambda candidate, credentials: pytest.fail("must not preflight"),
        launch=lambda password: pytest.fail("must not launch"),
        credentials_exist=lambda: False,
    )
    console = CancelAtPrivateKey(["0", "4"], [])

    assert run_wizard(services, console, config_path=config) == 130
    assert not config.exists()


class Report:
    def __init__(self, ready):
        self.live_ready = ready
        self.required_margin_usdg = Decimal("1400")

    def summary(self):
        return "LIVE READY" if self.live_ready else "NOT LIVE-READY"


def _answers(start="START"):
    return ["0", "4", "4.5", "6.5", "10", "21", "100", "OFF", start]


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
    console = ScriptedConsole(_answers(), ["ab" * 40, "storage-password", "storage-password"])

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
    console = ScriptedConsole(_answers(), ["ab" * 40, "storage-password", "storage-password"])

    assert run_wizard(services, console, config_path=config) == 0
    assert len(preflights) == 2
    assert stored == [CollectedCredentials(0, 4, "ab" * 40)]
    assert launches == ["storage-password"]
    assert any("сохранены в зашифрованном хранилище" in message for message in console.output)


def test_second_preflight_failure_never_enables_disk_config(tmp_path):
    config = tmp_path / "grid.yml"
    credential = CollectedCredentials(0, 4, "ab" * 40)
    reports = iter([Report(True), Report(False)])
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        credentials_exist=lambda: True,
        persist_credentials=lambda value: pytest.fail("existing credentials must not be rewritten"),
        preflight=lambda candidate, credentials: next(reports),
        launch=lambda password: pytest.fail("must not launch"),
    )
    console = ScriptedConsole(["", "4.5", "6.5", "10", "21", "100", "OFF", "START"], ["storage-password"])

    assert run_wizard(services, console, config_path=config) == 1
    assert yaml.safe_load(config.read_text())["enabled"] is False


def test_own_enabled_config_from_previous_stopped_run_can_be_reused(tmp_path):
    config = tmp_path / "grid.yml"
    config.write_text(yaml.safe_dump(build_candidate(Decimal("4.5"), Decimal("6.5"), Decimal("100"))))
    credential = CollectedCredentials(0, 4, "ab" * 40)
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        credentials_exist=lambda: True,
        persist_credentials=lambda value: pytest.fail("must reuse"),
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: pytest.fail("canceled before launch"),
    )
    console = ScriptedConsole(["", "", "", "", "", "", "OFF", "cancel"], ["storage-password"])

    assert run_wizard(services, console, config_path=config) == 0
    assert yaml.safe_load(config.read_text())["enabled"] is False


def test_launch_exception_rolls_back_enabled_and_redacts_secret(tmp_path):
    config = tmp_path / "grid.yml"
    credential = CollectedCredentials(0, 4, "ab" * 40)
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        credentials_exist=lambda: True,
        persist_credentials=lambda value: pytest.fail("must reuse"),
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: (_ for _ in ()).throw(OSError(f"failed with {credential.api_private_key}")),
    )
    console = ScriptedConsole(["", "4.5", "6.5", "10", "21", "100", "OFF", "START"], ["storage-password"])

    assert run_wizard(services, console, config_path=config) == 1
    assert yaml.safe_load(config.read_text())["enabled"] is False
    assert credential.api_private_key not in "\n".join(console.output)


def test_launch_output_is_redacted_before_display(tmp_path):
    config = tmp_path / "grid.yml"
    credential = CollectedCredentials(0, 4, "ab" * 40)
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        credentials_exist=lambda: True,
        persist_credentials=lambda value: None,
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: LaunchResult(0, f"native output {credential.api_private_key}", True, True),
    )
    console = ScriptedConsole(["", "4.5", "6.5", "10", "21", "100", "OFF", "START"], ["storage-password"])

    assert run_wizard(services, console, config_path=config) == 0
    output = "\n".join(console.output)
    assert credential.api_private_key not in output
    assert "[REDACTED]" in output


def test_zero_return_without_fresh_matching_process_is_not_reported_started(tmp_path):
    config = tmp_path / "grid.yml"
    credential = CollectedCredentials(0, 4, "ab" * 40)
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        credentials_exist=lambda: True,
        persist_credentials=lambda value: None,
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: LaunchResult(0, "accepted", False, False),
    )
    console = ScriptedConsole(["", "4.5", "6.5", "10", "21", "100", "OFF", "START"], ["storage-password"])

    assert run_wizard(services, console, config_path=config) == 1
    assert yaml.safe_load(config.read_text())["enabled"] is False
    assert "Бот запущен" not in "\n".join(console.output)


def test_nonzero_launch_does_not_trust_unrelated_running_bot(tmp_path):
    config = tmp_path / "grid.yml"
    credential = CollectedCredentials(0, 4, "ab" * 40)
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
        credentials_exist=lambda: True,
        persist_credentials=lambda value: None,
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: LaunchResult(1, "failed", False, False),
        running_this=lambda: False,
    )
    console = ScriptedConsole(["", "4.5", "6.5", "10", "21", "100", "OFF", "START"], ["storage-password"])

    assert run_wizard(services, console, config_path=config) == 1
    assert yaml.safe_load(config.read_text())["enabled"] is False


def test_nonzero_launch_with_exact_matching_process_is_reported_uncertain(tmp_path):
    config = tmp_path / "grid.yml"
    credential = CollectedCredentials(0, 4, "ab" * 40)
    services = Services(
        running=lambda: False,
        new_password_required=lambda: False,
        unlock=lambda password: None,
        load_credentials=lambda: credential,
        credentials_exist=lambda: True,
        persist_credentials=lambda value: None,
        preflight=lambda candidate, credentials: Report(True),
        launch=lambda password: LaunchResult(1, "timeout", True, False),
    )
    console = ScriptedConsole(["", "4.5", "6.5", "10", "21", "100", "OFF", "START"], ["storage-password"])

    assert run_wizard(services, console, config_path=config) == 2
    assert yaml.safe_load(config.read_text())["enabled"] is True
    assert "новый процесс этого LIT-бота" in "\n".join(console.output)


def test_credential_replace_restores_previous_encrypted_file_on_reload_failure(tmp_path, monkeypatch):
    import hummingbot.client.config.config_helpers as helpers

    destination = tmp_path / "lighter_perpetual_robinhood.yml"
    old = CollectedCredentials(0, 4, "11" * 40)
    new = CollectedCredentials(1, 5, "22" * 40)
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
    old = CollectedCredentials(0, 4, "33" * 40)
    new = CollectedCredentials(1, 5, "44" * 40)
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
