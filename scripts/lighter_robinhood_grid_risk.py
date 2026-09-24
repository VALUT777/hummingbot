import json
import os
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Dict, Hashable, Optional, Set


class ReservationError(ValueError):
    pass


class JournalError(RuntimeError):
    pass


class IntentJournal:
    def __init__(self, path):
        self.path = Path(path)
        self.entries = {}
        if self.path.exists():
            try:
                document = json.loads(self.path.read_text(encoding="utf-8"))
                if document.get("version") != 2 or not isinstance(document.get("entries"), dict):
                    raise ValueError("unsupported journal schema")
                for provisional_id, entry in document["entries"].items():
                    if (not provisional_id or not isinstance(entry, dict)
                            or entry.get("side") not in {"BUY", "SELL"}
                            or not isinstance(entry.get("amount"), str)
                            or not isinstance(entry.get("baseline"), str)
                            or not isinstance(entry.get("connector_domain"), str)
                            or not isinstance(entry.get("trading_pair"), str)
                            or isinstance(entry.get("account_index"), bool)
                            or not isinstance(entry.get("account_index"), int)
                            or (entry.get("client_order_id") is not None
                                and not isinstance(entry.get("client_order_id"), str))):
                        raise ValueError("invalid journal entry")
                    amount = Decimal(entry["amount"])
                    baseline = Decimal(entry["baseline"])
                    if not amount.is_finite() or amount <= 0 or not baseline.is_finite():
                        raise ValueError("invalid journal amount or baseline")
                bound_ids = [
                    entry["client_order_id"]
                    for entry in document["entries"].values()
                    if entry["client_order_id"] is not None
                ]
                if len(bound_ids) != len(set(bound_ids)):
                    raise ValueError("duplicate bound client order id")
                self.entries = document["entries"]
            except (OSError, TypeError, ValueError, InvalidOperation, json.JSONDecodeError) as exc:
                raise JournalError(f"cannot safely read intent journal: {exc}") from exc

    def reserve(
        self,
        provisional_id: str,
        side: str,
        amount: Decimal,
        baseline: Decimal,
        connector_domain: str,
        trading_pair: str,
        account_index: int,
    ) -> None:
        if not provisional_id or provisional_id in self.entries or side not in {"BUY", "SELL"}:
            raise JournalError("invalid or duplicate journal reservation")
        if (not connector_domain or not trading_pair or isinstance(account_index, bool)
                or not isinstance(account_index, int)):
            raise JournalError("journal reservation requires an exact account identity")
        self.entries[provisional_id] = {
            "side": side,
            "amount": str(amount),
            "baseline": str(baseline),
            "connector_domain": connector_domain,
            "trading_pair": trading_pair,
            "account_index": account_index,
            "client_order_id": None,
        }
        self._persist()

    def bind_client_order_id(self, provisional_id: str, client_order_id: str) -> None:
        entry = self.entries.get(provisional_id)
        if entry is None or not client_order_id:
            raise JournalError("cannot bind unknown intent or empty client order id")
        canonical_id = str(client_order_id)
        if any(
            other_id != provisional_id and other.get("client_order_id") == canonical_id
            for other_id, other in self.entries.items()
        ):
            raise JournalError("client order id is already bound to another intent")
        entry["client_order_id"] = canonical_id
        self._persist()

    def clear(self) -> None:
        self.entries = {}
        self._persist()

    def _persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
        try:
            with temporary.open("w", encoding="utf-8") as output:
                json.dump({"version": 2, "entries": self.entries}, output, sort_keys=True)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, self.path)
            directory_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError as exc:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise JournalError(f"cannot durably persist intent journal: {exc}") from exc


@dataclass
class ReservedOrder:
    side: str
    amount: Decimal
    terminal: bool = False
    accepted: bool = False
    fills: Dict[str, Decimal] = field(default_factory=dict)
    authoritative_cumulative: Optional[Decimal] = None


class ExposureEpoch:
    """Pure fail-closed exposure ledger for one submission epoch.

    Submitted quantities, rather than remaining quantities, are reserved. This is
    deliberately conservative when submission or cancellation outcomes are late.
    """

    def __init__(self, baseline: Decimal, cap: Decimal, amount_increment: Decimal):
        for name, value in (("baseline", baseline), ("cap", cap), ("amount_increment", amount_increment)):
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ValueError(f"{name} must be a finite Decimal")
        if cap <= 0 or amount_increment <= 0 or abs(baseline) > cap:
            raise ValueError("cap and amount increment must be positive and baseline must be within cap")
        if baseline % amount_increment != 0:
            raise ValueError("baseline must be aligned to amount increment")
        self.baseline = baseline
        self.cap = cap
        self.amount_increment = amount_increment
        self.orders: Dict[str, ReservedOrder] = {}
        self.invalid_reason: Optional[str] = None
        self._last_consistent_revision: Optional[Hashable] = None
        self._consistent_observations = 0

    @property
    def reserved_buy(self) -> Decimal:
        return sum((order.amount for order in self.orders.values() if order.side == "BUY"), Decimal("0"))

    @property
    def reserved_sell(self) -> Decimal:
        return sum((order.amount for order in self.orders.values() if order.side == "SELL"), Decimal("0"))

    @property
    def signed_fills(self) -> Decimal:
        return sum(
            ((order.authoritative_cumulative
              if order.authoritative_cumulative is not None
              else sum(order.fills.values(), Decimal("0"))) * (1 if order.side == "BUY" else -1)
             for order in self.orders.values()),
            Decimal("0"),
        )

    def reserve(self, order_id: str, side: str, raw_amount: Decimal) -> Decimal:
        if self.invalid_reason is not None:
            raise ReservationError(self.invalid_reason)
        if not order_id or order_id in self.orders:
            raise ReservationError("order id must be present and unique")
        if side not in {"BUY", "SELL"}:
            raise ReservationError("side must be BUY or SELL")
        if not isinstance(raw_amount, Decimal) or not raw_amount.is_finite() or raw_amount <= 0:
            raise ReservationError("amount must be a finite positive Decimal")
        amount = ((raw_amount / self.amount_increment).to_integral_value(rounding=ROUND_DOWN)
                  * self.amount_increment)
        if amount <= 0:
            raise ReservationError("amount is dust after quantization")
        buy_total = self.reserved_buy + (amount if side == "BUY" else Decimal("0"))
        sell_total = self.reserved_sell + (amount if side == "SELL" else Decimal("0"))
        if self.baseline + buy_total > self.cap or self.baseline - sell_total < -self.cap:
            raise ReservationError("reservation would exceed absolute net position cap")
        self.orders[order_id] = ReservedOrder(side=side, amount=amount)
        return amount

    def mark_accepted(self, order_id: str) -> bool:
        order = self._known_order(order_id, "acceptance")
        if order is None:
            return False
        order.accepted = True
        return True

    def rename_order(self, provisional_id: str, client_order_id: str) -> None:
        if not client_order_id or client_order_id in self.orders:
            raise ReservationError("returned client order id must be present and unique")
        order = self.orders.pop(provisional_id, None)
        if order is None:
            self.invalid_reason = f"submission returned for unknown reservation {provisional_id}"
            raise ReservationError(self.invalid_reason)
        self.orders[client_order_id] = order

    def mark_terminal(self, order_id: str) -> bool:
        order = self._known_order(order_id, "terminal event")
        if order is None:
            return False
        order.terminal = True
        return True

    def record_fill(self, order_id: str, trade_id: str, amount: Decimal) -> bool:
        order = self._known_order(order_id, "fill")
        if order is None:
            return False
        if not trade_id:
            self.invalid_reason = "fill is missing trade identifier"
            return False
        if trade_id in order.fills:
            if order.fills[trade_id] != amount:
                self.invalid_reason = f"conflicting duplicate fill {trade_id}"
                return False
            return True
        if not isinstance(amount, Decimal) or not amount.is_finite() or amount <= 0:
            self.invalid_reason = "fill amount is invalid"
            return False
        cumulative = sum(order.fills.values(), Decimal("0")) + amount
        if cumulative > order.amount:
            self.invalid_reason = f"fills exceed reserved quantity for {order_id}"
            return False
        if order.authoritative_cumulative is not None and cumulative > order.authoritative_cumulative:
            self.invalid_reason = f"fills exceed authoritative cumulative quantity for {order_id}"
            return False
        order.fills[trade_id] = amount
        self._consistent_observations = 0
        self._last_consistent_revision = None
        return True

    def set_authoritative_cumulative(self, order_id: str, cumulative: Decimal) -> bool:
        order = self._known_order(order_id, "authoritative fill")
        if order is None:
            return False
        if (not isinstance(cumulative, Decimal) or not cumulative.is_finite()
                or cumulative < 0 or cumulative > order.amount
                or cumulative % self.amount_increment != 0):
            self.invalid_reason = f"authoritative cumulative fill is invalid for {order_id}"
            return False
        local = sum(order.fills.values(), Decimal("0"))
        if cumulative < local or (
                order.authoritative_cumulative is not None and cumulative < order.authoritative_cumulative):
            self.invalid_reason = f"authoritative cumulative fill regressed for {order_id}"
            return False
        order.authoritative_cumulative = cumulative
        return True

    def observe(
        self,
        revision: Hashable,
        net_position: Decimal,
        active_order_ids: Set[str],
        pending_submissions: bool,
    ) -> bool:
        if self.invalid_reason is not None or revision is None:
            return False
        if not isinstance(net_position, Decimal) or not net_position.is_finite() or abs(net_position) > self.cap:
            self.invalid_reason = "account position is invalid or beyond cap"
            return False
        if pending_submissions or active_order_ids or any(not order.terminal for order in self.orders.values()):
            self._consistent_observations = 0
            self._last_consistent_revision = None
            return False
        expected = self.baseline + self.signed_fills
        if net_position % self.amount_increment != 0 or expected % self.amount_increment != 0:
            self._consistent_observations = 0
            self._last_consistent_revision = None
            return False
        if net_position != expected:
            self._consistent_observations = 0
            self._last_consistent_revision = None
            return False
        if revision == self._last_consistent_revision:
            return False
        self._last_consistent_revision = revision
        self._consistent_observations += 1
        return self._consistent_observations >= 2

    def _known_order(self, order_id: str, event: str) -> Optional[ReservedOrder]:
        order = self.orders.get(order_id)
        if order is None:
            self.invalid_reason = f"{event} for unknown order {order_id}"
        return order
