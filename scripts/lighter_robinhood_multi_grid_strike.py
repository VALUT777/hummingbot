import os
from typing import List

from pydantic import model_validator

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import StopExecutorAction
from scripts.v2_with_controllers import V2WithControllers, V2WithControllersConfig


class LighterRobinhoodMultiGridStrikeScriptConfig(V2WithControllersConfig):
    script_file_name: str = os.path.basename(__file__)
    controllers_config: List[str] = ["lighter_robinhood_multi_grid_strike.yml"]

    @model_validator(mode="after")
    def reject_non_preserving_drawdown_controls(self):
        if self.script_file_name != os.path.basename(__file__):
            raise ValueError("script_file_name must select the dedicated Robinhood Multi Grid Strike runner")
        if self.controllers_config != ["lighter_robinhood_multi_grid_strike.yml"]:
            raise ValueError("runner requires exactly the dedicated Robinhood Multi Grid Strike controller")
        if self.max_global_drawdown_quote is not None or self.max_controller_drawdown_quote is not None:
            raise ValueError("drawdown stop controls are disabled because positions must be preserved")
        return self


class LighterRobinhoodMultiGridStrikeScript(V2WithControllers):
    @staticmethod
    def _preserving_stop_actions(executors) -> List[StopExecutorAction]:
        return [
            StopExecutorAction(
                executor_id=executor.id,
                controller_id=executor.controller_id,
                keep_position=True,
            )
            for executor in executors
            if executor.is_active
        ]

    async def on_stop(self):
        for controller in self.controllers.values():
            controller.stop()
        actions = self._preserving_stop_actions(self.get_all_executors())
        if actions:
            self.executor_orchestrator.execute_actions(actions)
        await super().on_stop()

    def check_manual_kill_switch(self):
        for controller_id, controller in self.controllers.items():
            if controller.config.manual_kill_switch and controller.status == RunnableStatus.RUNNING:
                self.logger().info(f"Controller {controller_id} manually stopped with position preservation.")
                controller.stop()
                actions = self._preserving_stop_actions(self.get_executors_by_controller(controller_id))
                if actions:
                    self.executor_orchestrator.execute_actions(actions)
            elif not controller.config.manual_kill_switch and controller.status == RunnableStatus.TERMINATED:
                if controller_id not in self.drawdown_exited_controllers:
                    controller.start()

    def check_executors_status(self):
        active_executors = [
            executor for executor in self.get_all_executors()
            if executor.status == RunnableStatus.RUNNING
        ]
        non_trading = [executor for executor in active_executors if not executor.is_trading]
        if not active_executors:
            HummingbotApplication.main_application().stop()
            return
        actions = self._preserving_stop_actions(non_trading)
        if actions:
            self.executor_orchestrator.execute_actions(actions)
