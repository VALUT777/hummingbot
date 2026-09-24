"""Client order id allocation logic (NG-HIST-004, AC-43 logic part).

* numeric CID in ``[1, 2**48 - 1]`` (SDK ``MAX_CLIENT_ORDER_ID_BIT_COUNT=48``); never truncated or hashed;
* strictly monotonic from a durable high-water mark (the store persists it together with the intent);
* the same ``LegIdentity`` always maps to the same CID (durable ``lookup`` + in-memory map), so a timeout,
  crash or status lag can never mint a second CID for one leg;
* every new candidate is checked with the provided ``exists(cid)`` callback (durable map, venue evidence);
  a collision, overflow/exhaustion or malformed value latches the allocator into a failed, fail-closed state.

Persistence (the atomic ``(identity -> cid)`` row + high-water) belongs to the store (WS-B); this class is the
pure decision logic it must follow.
"""
from __future__ import annotations

from typing import Callable, Dict, Optional

from hummingbot.strategy_v2.executors.neutral_grid_executor.contracts import MAX_CLIENT_ORDER_ID, LegIdentity


class CidError(Exception):
    """Fail-closed CID error: engine must not submit/cancel anything that needs a new CID."""


class CidExhausted(CidError):
    pass


class CidCollision(CidError):
    pass


class CidInvalid(CidError):
    pass


def validate_cid(value: object) -> int:
    """Exact int CID within the 48-bit bound. No coercion from float/str, no masking."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise CidInvalid(f"client order id must be int, got {type(value).__name__}: {value!r}")
    if not 1 <= value <= MAX_CLIENT_ORDER_ID:
        raise CidInvalid(f"client order id {value} outside [1, {MAX_CLIENT_ORDER_ID}] (48-bit bound)")
    return value


class CidAllocator:
    def __init__(self, exists: Callable[[int], bool], *, high_water: int = 0,
                 lookup: Optional[Callable[[LegIdentity], Optional[int]]] = None,
                 max_cid: int = MAX_CLIENT_ORDER_ID):
        if isinstance(high_water, bool) or not isinstance(high_water, int) or high_water < 0:
            raise CidInvalid(f"high_water must be a non-negative int, got {high_water!r}")
        if isinstance(max_cid, bool) or not isinstance(max_cid, int) or not 1 <= max_cid <= MAX_CLIENT_ORDER_ID:
            raise CidInvalid(f"max_cid must be in [1, {MAX_CLIENT_ORDER_ID}], got {max_cid!r}")
        if high_water > max_cid:
            raise CidInvalid(f"high_water {high_water} beyond max_cid {max_cid}")
        self._exists = exists
        self._lookup = lookup
        self._max = max_cid
        self.high_water = high_water
        self.failed: Optional[str] = None
        self._by_identity: Dict[LegIdentity, int] = {}
        self._by_cid: Dict[int, LegIdentity] = {}

    def _fail(self, exc: CidError) -> CidError:
        self.failed = str(exc)
        return exc

    def known(self, identity: LegIdentity) -> Optional[int]:
        """Existing CID for ``identity`` (memory first, then the durable lookup)."""
        cid = self._by_identity.get(identity)
        if cid is None and self._lookup is not None:
            found = self._lookup(identity)
            if found is not None:
                try:
                    cid = validate_cid(found)
                except CidInvalid as exc:
                    raise self._fail(exc)          # corrupt durable row: latch fail-closed
                self._remember(identity, cid)
        return cid

    def _remember(self, identity: LegIdentity, cid: int) -> None:
        other = self._by_cid.get(cid)
        if other is not None and other != identity:
            raise self._fail(CidCollision(f"CID {cid} already mapped to {other}, not {identity}"))
        self._by_identity[identity] = cid
        self._by_cid[cid] = identity

    def allocate(self, identity: LegIdentity) -> int:
        """CID for ``identity``: the existing one if any, otherwise the next monotonic, collision-checked id."""
        if not isinstance(identity, LegIdentity):
            raise CidInvalid(f"identity must be LegIdentity, got {type(identity).__name__}")
        existing = self.known(identity)
        if existing is not None:
            return existing
        if self.failed is not None:
            raise CidError(f"allocator failed closed: {self.failed}")
        candidate = self.high_water + 1
        if candidate > self._max:
            raise self._fail(CidExhausted(f"CID space exhausted at {self.high_water} (max {self._max})"))
        if candidate in self._by_cid or self._exists(candidate):
            raise self._fail(CidCollision(f"CID {candidate} already in use; refusing to skip or hash"))
        self.high_water = candidate
        self._remember(identity, candidate)
        return candidate

    def identity_of(self, cid: int) -> Optional[LegIdentity]:
        return self._by_cid.get(validate_cid(cid))
