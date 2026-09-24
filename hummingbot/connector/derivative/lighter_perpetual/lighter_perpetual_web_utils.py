import time
from typing import Any, Dict, Optional

from hummingbot.connector.derivative.lighter_perpetual import lighter_perpetual_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class LighterRESTPreProcessor(RESTPreProcessorBase):
    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        headers = dict(request.headers or {})
        headers.setdefault("Accept", "application/json")
        request.headers = headers
        return request


def rest_url(path_url: str = "", domain: str = "lighter_perpetual") -> str:
    return f"{CONSTANTS.get_domain_settings(domain).rest_url}{path_url}"


def public_rest_url(path_url: str = "", domain: str = "lighter_perpetual") -> str:
    return rest_url(path_url=path_url, domain=domain)


def private_rest_url(path_url: str = "", domain: str = "lighter_perpetual") -> str:
    return rest_url(path_url=path_url, domain=domain)


def wss_url(domain: str = "lighter_perpetual") -> str:
    return CONSTANTS.get_domain_settings(domain).ws_url


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    return WebAssistantsFactory(
        throttler=throttler,
        rest_pre_processors=[LighterRESTPreProcessor()],
        auth=auth,
    )


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(throttler: AsyncThrottler, domain: str) -> float:
    return time.time()


def is_exchange_information_valid(rule: Dict[str, Any]) -> bool:
    if rule.get("status") != "active":
        return False
    market_config = rule.get("market_config", {})
    return not market_config.get("hidden", False)
