#!/usr/bin/env python
"""Close the Robinhood Lighter LIT long with one bounded reduce-only market workflow."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Collection, Protocol

from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType


DOMAIN = "lighter_perpetual_robinhood"
PAIR = "LIT-USDG"


class ClosePositionError(RuntimeError):
    pass


@dataclass(frozen=True)
class CloseResult:
    initial_position: Decimal
    closed_amount: Decimal
    attempts: int
    already_flat: bool = False


class CloseClient(Protocol):
    async def snapshot(self, client_order_ids: Collection[str]) -> dict:
        ...

    async def submit_reduce_only_market_sell(self, amount: Decimal) -> str:
        ...


def _decimal(value: Any, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ClosePositionError(f"{label} is invalid") from exc
    if not result.is_finite():
        raise ClosePositionError(f"{label} is invalid")
    return result


def _active_ids(snapshot: dict) -> set[str]:
    orders = snapshot.get("active_orders")
    if not isinstance(orders, list):
        raise ClosePositionError("active orders are malformed")
    result = set()
    for order in orders:
        if not isinstance(order, dict):
            raise ClosePositionError("active orders are malformed")
        value = order.get("client_order_id_str", order.get("client_order_id"))
        if value is None or isinstance(value, bool) or not str(value):
            raise ClosePositionError("active order has no client id")
        result.add(str(value))
    return result


def _validated_snapshot(snapshot: dict, expected_account_index: int, owned_ids: set[str]) -> tuple[Decimal, Decimal]:
    if not isinstance(snapshot, dict):
        raise ClosePositionError("authoritative account snapshot is malformed")
    account_index = snapshot.get("account_index")
    if isinstance(account_index, bool) or not isinstance(account_index, int) or account_index != expected_account_index:
        raise ClosePositionError("authoritative account index does not match saved credentials")
    request_started = _decimal(snapshot.get("request_started_at"), "request timestamp")
    fetched_at = _decimal(snapshot.get("fetched_at"), "fetch timestamp")
    if request_started < 0 or fetched_at < request_started or fetched_at - request_started > Decimal("10"):
        raise ClosePositionError("authoritative REST snapshot is stale")
    position = _decimal(snapshot.get("net_position"), "net position")
    margin = _decimal(snapshot.get("available_margin"), "available USDG")
    leverage = _decimal(snapshot.get("leverage"), "leverage")
    if (
        snapshot.get("net_position_known") is not True
        or snapshot.get("available_margin_known") is not True
        or margin < 0
        or snapshot.get("collateral_token") != "USDG"
        or snapshot.get("position_mode") != "ONEWAY"
        or snapshot.get("leverage_confirmed") is not True
        or leverage != Decimal("5")
        or snapshot.get("market_state_known") is not True
        or snapshot.get("market_status") != "active"
    ):
        raise ClosePositionError("account, USDG, leverage, or market state is not authoritative")
    foreign_ids = _active_ids(snapshot) - owned_ids
    if foreign_ids:
        raise ClosePositionError("account has an active order not owned by this close operation")
    return position, fetched_at


async def close_long_position(
    client: CloseClient,
    *,
    expected_account_index: int,
    max_attempts: int = 2,
    poll_limit: int = 12,
    poll_interval: float = 1,
) -> CloseResult:
    if max_attempts < 1 or poll_limit < 1 or poll_interval < 0:
        raise ValueError("invalid close retry settings")
    owned_ids: set[str] = set()
    first = await client.snapshot(owned_ids)
    position, first_revision = _validated_snapshot(first, expected_account_index, owned_ids)
    if first.get("pending_submissions_unknown") is not False:
        raise ClosePositionError("account has an unknown pending submission")
    if position < 0:
        raise ClosePositionError("refusing to close a short position")
    initial_position = position
    if position == 0:
        if poll_interval:
            await asyncio.sleep(poll_interval)
        second = await client.snapshot(owned_ids)
        second_position, second_revision = _validated_snapshot(second, expected_account_index, owned_ids)
        if (second.get("pending_submissions_unknown") is not False
                or second_position != 0 or second_revision <= first_revision):
            raise ClosePositionError("flat account state was not confirmed twice")
        return CloseResult(initial_position=Decimal("0"), closed_amount=Decimal("0"), attempts=0, already_flat=True)

    for attempt in range(1, max_attempts + 1):
        submitted_position = position
        client_id = str(await client.submit_reduce_only_market_sell(submitted_position))
        if not client_id:
            raise ClosePositionError("close submission returned no client id")
        owned_ids.add(client_id)
        last_inconsistent = False
        for _ in range(poll_limit):
            if poll_interval:
                await asyncio.sleep(poll_interval)
            current = await client.snapshot(owned_ids)
            current_position, revision = _validated_snapshot(current, expected_account_index, owned_ids)
            details = current.get("orders_by_client_id")
            detail = details.get(client_id) if isinstance(details, dict) else None
            if not isinstance(detail, dict) or detail.get("terminal") is not True:
                continue
            if detail.get("cumulative_fill_known") is not True:
                continue
            fill = _decimal(detail.get("cumulative_fill_base"), "terminal cumulative fill")
            if fill < 0 or fill > submitted_position:
                raise ClosePositionError("terminal fill is inconsistent with submitted amount")
            expected_residual = submitted_position - fill
            if current_position != expected_residual:
                last_inconsistent = True
                continue
            if current.get("pending_submissions_unknown") is not False or _active_ids(current):
                last_inconsistent = True
                continue
            position = current_position
            if position == 0:
                if poll_interval:
                    await asyncio.sleep(poll_interval)
                confirmed = await client.snapshot(owned_ids)
                confirmed_position, confirmed_revision = _validated_snapshot(
                    confirmed, expected_account_index, owned_ids
                )
                if (confirmed.get("pending_submissions_unknown") is not False
                        or confirmed_position != 0 or _active_ids(confirmed)
                        or confirmed_revision <= revision):
                    raise ClosePositionError("flat account state was not confirmed twice")
                return CloseResult(
                    initial_position=initial_position,
                    closed_amount=initial_position,
                    attempts=attempt,
                )
            break
        else:
            if last_inconsistent:
                raise ClosePositionError("terminal fill and account position are inconsistent")
            raise ClosePositionError("close order outcome is unknown; no retry was submitted")
        if attempt == max_attempts:
            raise ClosePositionError("position remains after the bounded reduce-only close attempts")
    raise ClosePositionError("position close did not complete")


class ConnectorCloseClient:
    def __init__(self, connector):
        self._connector = connector

    async def snapshot(self, client_order_ids: Collection[str]) -> dict:
        return await self._connector.get_grid_account_snapshot(
            PAIR, client_order_ids=list(client_order_ids), force_refresh=True
        )

    async def submit_reduce_only_market_sell(self, amount: Decimal) -> str:
        await self._connector._update_trading_rules()
        market = self._connector.market_info_for_trading_pair(PAIR)
        reference = _decimal(market.raw_info.get("last_trade_price"), "fresh market reference price")
        if reference <= 0:
            raise ClosePositionError("fresh market reference price is invalid")
        quantized_amount = self._connector.quantize_order_amount(PAIR, amount)
        if quantized_amount != amount:
            raise ClosePositionError("net position is not exactly aligned to the LIT size increment")
        client_id = self._connector._new_client_order_id()
        await self._connector._place_order(
            order_id=client_id,
            trading_pair=PAIR,
            amount=amount,
            trade_type=TradeType.SELL,
            order_type=OrderType.MARKET,
            price=reference,
            position_action=PositionAction.CLOSE,
        )
        return client_id

    async def close(self):
        signer = getattr(self._connector, "_signer_client", None)
        if signer is not None:
            await signer.close()
        factory = getattr(self._connector, "_web_assistants_factory", None)
        if factory is not None:
            await factory.close()


async def _run_authenticated_close() -> CloseResult:
    from hummingbot.client.config.config_helpers import get_connector_class
    from hummingbot.client.config.security import Security
    from hummingbot.client.settings import AllConnectorSettings
    from bin.lighter_robinhood_preflight import redact

    keys = Security.api_keys(DOMAIN)
    if not keys:
        raise ClosePositionError("saved Robinhood Lighter credentials were not found")
    private_key = str(keys.get("lighter_perpetual_robinhood_api_private_key", ""))
    try:
        account_index = keys["lighter_perpetual_robinhood_account_index"]
        if isinstance(account_index, bool):
            raise ValueError
        account_index = int(account_index)
        setting = AllConnectorSettings.get_connector_settings()[DOMAIN]
        params = setting.conn_init_parameters(
            trading_pairs=[PAIR], trading_required=True, api_keys=keys,
            balance_asset_limit={}, rate_limits_share_pct=Decimal("100"),
        )
        connector = get_connector_class(DOMAIN)(**params)
        client = ConnectorCloseClient(connector)
        try:
            return await close_long_position(client, expected_account_index=account_index)
        finally:
            await client.close()
    except Exception as exc:
        if isinstance(exc, ClosePositionError):
            raise
        raise ClosePositionError(redact(exc, [private_key])) from None


def main() -> int:
    from hummingbot.cli import bot
    from hummingbot.cli.password import login
    from bin.lighter_robinhood_preflight import redact

    try:
        if bot.running():
            raise ClosePositionError("Hummingbot is running; stop it before closing the position")
        _, password = login()
        del password
        print("Проверяю аккаунт и закрываю только LONG LIT reduce-only MARKET…")
        result = asyncio.run(_run_authenticated_close())
        if result.already_flat:
            print("Позиция LIT уже закрыта; две свежие проверки подтвердили нулевую позицию.")
        else:
            print(
                f"Позиция LIT закрыта: {result.closed_amount} LIT, "
                f"reduce-only попыток: {result.attempts}."
            )
        return 0
    except KeyboardInterrupt:
        print("\nЗакрытие отменено.")
        return 130
    except Exception as exc:
        print(f"Ошибка закрытия: {redact(exc)}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
