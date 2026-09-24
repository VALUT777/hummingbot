from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from hummingbot.strategy_v2.models.base import RunnableStatus
from scripts.lighter_robinhood_multi_grid_strike import (
    LighterRobinhoodMultiGridStrikeScript,
    LighterRobinhoodMultiGridStrikeScriptConfig,
)


def test_runner_rejects_drawdown_controls_that_can_trigger_non_preserving_stop_paths():
    with pytest.raises(ValidationError):
        LighterRobinhoodMultiGridStrikeScriptConfig(max_global_drawdown_quote=1)
    with pytest.raises(ValidationError):
        LighterRobinhoodMultiGridStrikeScriptConfig(max_controller_drawdown_quote=1)


def test_runner_loads_only_the_dedicated_controller_and_script_names():
    cfg = LighterRobinhoodMultiGridStrikeScriptConfig()
    assert cfg.script_file_name == "lighter_robinhood_multi_grid_strike.py"
    assert cfg.controllers_config == ["lighter_robinhood_multi_grid_strike.yml"]
    with pytest.raises(ValidationError):
        LighterRobinhoodMultiGridStrikeScriptConfig(controllers_config=["other.yml"])
    with pytest.raises(ValidationError):
        LighterRobinhoodMultiGridStrikeScriptConfig(script_file_name="v2_with_controllers.py")


@pytest.mark.asyncio
async def test_runner_stop_marks_every_active_executor_keep_position_before_base_shutdown():
    strategy = LighterRobinhoodMultiGridStrikeScript.__new__(LighterRobinhoodMultiGridStrikeScript)
    controller = MagicMock()
    executor = MagicMock(id="executor-1", controller_id="controller-1", is_active=True)
    strategy.controllers = {"controller-1": controller}
    strategy.get_all_executors = MagicMock(return_value=[executor])
    strategy.executor_orchestrator = MagicMock()

    with patch("scripts.lighter_robinhood_multi_grid_strike.V2WithControllers.on_stop", new=AsyncMock()) as base_stop:
        await strategy.on_stop()

    controller.stop.assert_called_once()
    actions = strategy.executor_orchestrator.execute_actions.call_args.args[0]
    assert len(actions) == 1 and actions[0].executor_id == "executor-1"
    assert actions[0].keep_position is True
    base_stop.assert_awaited_once_with()


def test_manual_kill_switch_stops_native_executor_with_position_preserved():
    strategy = LighterRobinhoodMultiGridStrikeScript.__new__(LighterRobinhoodMultiGridStrikeScript)
    controller = MagicMock(status=RunnableStatus.RUNNING)
    controller.config.manual_kill_switch = True
    executor = MagicMock(id="executor-1", controller_id="controller-1")
    strategy.controllers = {"controller-1": controller}
    strategy.drawdown_exited_controllers = []
    strategy.get_executors_by_controller = MagicMock(return_value=[executor])
    strategy.executor_orchestrator = MagicMock()
    strategy.logger = MagicMock(return_value=MagicMock())

    strategy.check_manual_kill_switch()

    controller.stop.assert_called_once()
    action = strategy.executor_orchestrator.execute_actions.call_args.args[0][0]
    assert action.keep_position is True


def test_completed_preserving_shutdown_stops_application_without_new_executor_actions():
    strategy = LighterRobinhoodMultiGridStrikeScript.__new__(LighterRobinhoodMultiGridStrikeScript)
    strategy.get_all_executors = MagicMock(return_value=[])
    strategy.executor_orchestrator = MagicMock()
    application = MagicMock()

    with patch(
        "scripts.lighter_robinhood_multi_grid_strike.HummingbotApplication.main_application",
        return_value=application,
    ):
        strategy.check_executors_status()

    application.stop.assert_called_once_with()
    strategy.executor_orchestrator.execute_actions.assert_not_called()
