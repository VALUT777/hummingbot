"""Single-engine host for the offline demo (and any future in-process host).

The host owns exactly one engine object and one asyncio task that ticks it. HTTP handlers never create,
start or stop engines: Start/Pause/Resume/Stop are commands in the engine's durable queue, applied by
the engine on its own tick. Closing or reloading the browser therefore has no effect on the engine; only
shutting down the backend process stops the tick loop (the ledger stays on disk).

``status()`` reports *process-level* facts (task alive, tick count, last tick error). The UI shows them
separately and never maps "task alive" to an engine state (AC-48).
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Dict, Optional

LOGGER = logging.getLogger("web.neutral_grid.host")


class EngineAlreadyHosted(RuntimeError):
    pass


class EngineHost:
    _hosted_identities: set = set()

    def __init__(self, engine: Any, identity: str, *, tick_interval_s: float = 1.0,
                 clock: Callable[[], float] = time.time):
        if identity in EngineHost._hosted_identities:
            raise EngineAlreadyHosted(f"engine {identity!r} is already hosted in this process")
        EngineHost._hosted_identities.add(identity)
        self.engine = engine
        self.identity = identity
        self._tick_interval_s = tick_interval_s
        self._clock = clock
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self.ticks = 0
        self.last_tick_at: Optional[float] = None
        self.last_error: Optional[str] = None
        self._released = False

    def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.get_running_loop().create_task(self._run(), name=f"ng-engine-{self.identity}")

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.engine.tick()
                self.last_error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - a failing tick must not kill the host loop
                self.last_error = f"{type(exc).__name__}"
                LOGGER.exception("engine tick raised; the engine reports its own state in the next snapshot")
            self.ticks += 1
            self.last_tick_at = self._clock()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._tick_interval_s)
            except asyncio.TimeoutError:
                pass

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=10)
            except asyncio.TimeoutError:
                self._task.cancel()
        if not self._released:
            EngineHost._hosted_identities.discard(self.identity)
            self._released = True

    def status(self) -> Dict[str, Any]:
        alive = self._task is not None and not self._task.done()
        return {"engine_task_alive": alive, "ticks": self.ticks, "last_tick_at": self.last_tick_at,
                "last_tick_error": self.last_error,
                "note": "Процесс backend; состояние движка — только из зафиксированного снимка."}
