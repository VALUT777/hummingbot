#!/usr/bin/env python
"""Local web UI/API for the Robinhood Lighter neutral grid (spec NG-UI-001..003).

Modes:
  --demo-fake-exchange   run the single engine in this process on the package FakeExchange with a
                         temporary SQLite ledger: fully offline, no credentials, no network.
  --attach-db PATH       serve the committed snapshots of an engine that runs elsewhere (the Hummingbot
                         executor) from its SQLite store and enqueue commands into its durable queue.

Security defaults: binds 127.0.0.1 only; a non-loopback bind needs --allow-non-loopback-bind and is
still only protected by session + CSRF + Origin (use an SSH tunnel or an authenticating TLS proxy for
remote access). In attach mode the Hummingbot host owns and unlocks connector credentials; this web
process neither accepts a keystore password nor changes the attached profile. Closing the browser never stops the engine.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path
from typing import List, Optional, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8787
_FORBIDDEN_ARG_WORDS = ("password", "passwd", "secret", "private-key", "private_key", "api-key", "api_key")


class LaunchRefused(RuntimeError):
    pass


class _QuietParser(argparse.ArgumentParser):
    """argparse echoes unrecognized values; a mistyped secret must not be printed back."""

    def error(self, message: str):
        self.print_usage(sys.stderr)
        self.exit(2, f"{self.prog}: некорректные или неизвестные аргументы (значения не выводятся). "
                     "Справка: --help\n")


def build_parser() -> argparse.ArgumentParser:
    parser = _QuietParser(
        prog="lighter_robinhood_neutral_grid_web.py",
        description="Локальный веб-интерфейс нейтральной сетки (по умолчанию только 127.0.0.1).")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--demo-fake-exchange", action="store_true",
                      help="офлайн-демо: движок на FakeExchange и временной SQLite, без ключей и сети")
    mode.add_argument("--attach-db", type=Path, metavar="PATH",
                      help="подключиться к SQLite-журналу движка, работающего в процессе Hummingbot")
    parser.add_argument("--host", default=DEFAULT_HOST, help="адрес привязки (по умолчанию 127.0.0.1)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="порт (по умолчанию 8787)")
    parser.add_argument("--allow-non-loopback-bind", action="store_true",
                        help="разрешить не-loopback адрес (опасно: нет TLS; используйте SSH-туннель)")
    parser.add_argument("--allowed-host", action="append", default=[],
                        help="дополнительное имя хоста для проверки Host/Origin (только вместе с флагом выше)")
    parser.add_argument("--stale-after", type=float, default=15.0,
                        help="через сколько секунд снимок считается устаревшим (по умолчанию 15)")
    parser.add_argument("--data-dir", type=Path, help="каталог временного журнала демо (по умолчанию mkdtemp)")
    return parser


def refuse_secret_arguments(argv: Sequence[str]) -> None:
    """Secrets must never be passed on the command line (visible in ps, shell history, logs)."""
    for arg in argv:
        if not arg.startswith("-"):
            continue
        lowered = arg.split("=", 1)[0].lower().lstrip("-")
        if any(word in lowered for word in _FORBIDDEN_ARG_WORDS):
            raise LaunchRefused("Пароли и ключи не принимаются в аргументах командной строки. "
                                "Выберите и разблокируйте ключи в процессе Hummingbot.")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    argv = list(sys.argv[1:] if argv is None else argv)
    refuse_secret_arguments(argv)
    args = build_parser().parse_args(argv)
    from web.neutral_grid.security import check_bind
    args.bind_warning = check_bind(args.host, args.allow_non_loopback_bind)
    if args.allowed_host and not args.allow_non_loopback_bind:
        raise LaunchRefused("--allowed-host допускается только вместе с --allow-non-loopback-bind")
    if not (0 < args.port < 65536):
        raise LaunchRefused("Некорректный порт")
    return args


def login_url(host: str, port: int, token: str) -> str:
    shown = f"[{host}]" if ":" in host and not host.startswith("[") else host
    if shown in ("0.0.0.0", "[::]"):
        shown = "127.0.0.1"
    return f"http://{shown}:{port}/#auth={token}"


async def serve(args: argparse.Namespace, *, ready: Optional[asyncio.Event] = None,
                stop: Optional[asyncio.Event] = None, out=sys.stdout) -> None:
    from aiohttp import web

    from web.neutral_grid import runtime
    from web.neutral_grid.server import create_app

    if args.demo_fake_exchange:
        bundle = await runtime.build_demo(args)
    else:
        bundle = await runtime.build_attach(args)
    ctx = bundle.context
    app = create_app(ctx)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host=args.host, port=args.port)
    await site.start()
    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    if args.bind_warning:
        print(args.bind_warning, file=out)
    # The access token is shown once on this terminal only (never logged); it is not an exchange secret.
    print("Нейтральная сетка — локальный интерфейс запущен.", file=out)
    print(f"Режим: {ctx.mode}. Откройте в браузере: {login_url(args.host, args.port, ctx.access.token)}", file=out)
    print("Закрытие браузера не останавливает движок. Остановка сервера: Ctrl+C.", file=out)
    out.flush()
    if ready is not None:
        ready.set()
    try:
        await stop.wait()
    finally:
        await runner.cleanup()
        await bundle.close()


def main(argv: Optional[List[str]] = None) -> int:
    from web.neutral_grid.security import BindRefused
    try:
        args = parse_args(argv)
    except (LaunchRefused, BindRefused) as exc:
        print(f"Отказ запуска: {exc}", file=sys.stderr)
        return 2
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(serve(args))
    except LaunchRefused as exc:
        print(f"Отказ запуска: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
