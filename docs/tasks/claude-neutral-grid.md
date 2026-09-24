# Handoff Claude: neutral fixed-cell grid

Реализуй техническое задание `docs/superpowers/specs/2026-09-24-neutral-grid-spec.md` полностью. Спецификация выше любых прежних экспериментов с native GridExecutor.

## Рабочая среда

1. Выполни fetch `origin/codex/lighter-robinhood`, зафиксируй SHA commit, содержащего актуальное ТЗ, и начни именно из него. `76411e04b` — исходная codebase до spec commit, а не branch point реализации.
2. Создай отдельную branch `codex/neutral-grid-implementation` и отдельный checkout/worktree. Не работай в исходном dirty checkout.
3. В исходном checkout есть два незакоммиченных тестовых файла от старой попытки. Не изменяй, не удаляй, не коммить и не считай их требованиями:
   - `test/controllers/generic/test_lighter_robinhood_multi_grid_strike.py`;
   - `test/hummingbot/strategy_v2/executors/grid_executor/test_grid_executor.py`.
4. Не используй секреты, live authenticated API, legacy/live запуск бота или реальные submit/cancel. Новый UI/API разрешено запускать только с offline fake exchange.

## Обязательная форма решения

- Native Hummingbot V2 persistent neutral engine в `hummingbot/strategy_v2/executors/neutral_grid_executor/`.
- Generic controller `controllers/generic/neutral_grid.py`.
- Launcher `scripts/lighter_robinhood_fixed_neutral_grid.py` и новый disabled example config; старые neutral/MultiGrid entrypoints не менять.
- Тонкий Robinhood adapter с текущим encrypted credential и order plumbing.
- Узкое connector extension для cursor pagination, authoritative history и pre-persisted unique client ID submission.
- SQLite single-writer inbox + ledger + outbox + cursor с атомарными transitions.
- Mandatory local web UI/API в `web/neutral_grid/` с launcher `bin/lighter_robinhood_neutral_grid_web.py`: Russian responsive preview/status/cell grid/history/errors и idempotent start/pause/resume/stop command queue единственного engine. Default `127.0.0.1`, session+CSRF+Origin; existing encrypted keystore, masked presence, no secret readback/browser exchange calls/localStorage secrets. Не вводи тяжёлую deprecated Dashboard dependency.
- Один engine управляет всеми ячейками. Не создавать 55 PositionExecutor и не делать global rewrite GridExecutor.

Особо проверь трудные инварианты: integer-tick grid formula и exact size step; virtual opposing cells в ONEWAY и `reduce_only=false`; fixed prices без recenter/clamp; BUY-cell rearm BUY и SELL-cell rearm SELL; `ENTRY_LIVE + TP_LIVE`; partial-entry TP по history; whole-cell lock; GTT renewal; late fills; canonical history keys и full overlap обеих histories; 48-bit durable CID mapping; intent before API без ложной exactly-once гарантии; signed first-bootstrap baseline без restart recapture; endpoint net/gross caps; conservative entry+TP slot admission, bounded queued cells и exit priority; FIFO self-trade router; dust; DB/retention/persistence failures; definitive zero-fill rejection; stop uncertainty; UI читает versioned committed snapshot и отвергает stale commands.

## Процесс и доказательства

1. До кода составь traceability всех `NG-*` и `AC-01..AC-57` к компонентам и тестам. Не упрощай requirement молча.
2. Раздели pure state/risk/persistence, connector history adapter и Hummingbot orchestration. Добавляй тесты до или вместе с каждым инвариантом.
3. Используй deterministic fake exchange и crash injection для всех persistence windows. Не заменяй их mock-тестом, который просто повторяет implementation.
4. Выполни targeted tests, browser/API tests на fake exchange, existing Lighter connector tests, committed baseline neutral/risk и relevant controller/executor regressions, compile/import, lint changed files и `git diff --check`. Старые два dirty test файла не входят в clean-branch gate.
5. Подготовь короткий acceptance report: changed files, decisions, AC → test names, exact commands/results, unresolved risks. Заявление «всё работает» без свежих results не принимается.
6. Создай PR с base `codex/lighter-robinhood`, без auto-merge и без live testing. Codex независимо проверит diff, матрицу и свежие результаты, а не утверждение Claude.

## Недопустимые компромиссы

Не считай WS, cancel ack, исчезновение из active list или одну page history доказательством. Не угадывай order по price/time/size. Не используй float для Decimal/ID. Не переводи virtual TP в reduce-only. Не округляй quantity вверх, не сдвигай fixed target, не добавляй market cleanup. Не объявляй stop успешным при uncertain cancel. Не покупай стартовый inventory, не flatten manual baseline и не зашивай market minimum 5 LIT.
