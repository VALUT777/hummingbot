# Acceptance report — neutral fixed-cell grid (черновик для независимой приёмки Codex/Astra)

**Статус:** DRAFT. Исполнитель (Claude) передаёт на независимую приёмку. Заявления ниже — producer evidence, не независимый APPROVED.
**ТЗ:** `docs/superpowers/specs/2026-09-24-neutral-grid-spec.md`, handoff `docs/tasks/claude-neutral-grid.md`.
**Spec commit / branch point:** `ba597722d` (`origin/codex/lighter-robinhood`). Исходная codebase до spec — `76411e04b`.
**Ветка:** `codex/neutral-grid-implementation`. **Проверенный HEAD:** `<<HEAD>>`. PR: base `codex/lighter-robinhood`, draft, без auto-merge.
**Live:** никаких live credentials, authenticated Robinhood/Lighter API, реальных submit/cancel или запуска legacy-бота не было. Браузерная приёмка — только offline FakeExchange.
**Два старых грязных тестовых файла** исходного checkout (`test/controllers/generic/test_lighter_robinhood_multi_grid_strike.py`, `test/hummingbot/strategy_v2/executors/grid_executor/test_grid_executor.py`) не изменялись, не коммитились и в gate не входят (в gate участвуют их закоммиченные версии из ветки).

## 1. Что реализовано

| Компонент | Файлы | NG |
|---|---|---|
| Контракт типов/портов | `hummingbot/strategy_v2/executors/neutral_grid_executor/contracts.py`, `docs/neutral-grid/CONTRACTS.md` | — |
| Pure core | `…/neutral_grid_executor/{grid,cells,risk,admission,router,cid,dust}.py` | GRID, CELL, RISK-002/004/005, ORD-003, HIST-004 |
| SQLite single-writer store (v4) | `…/neutral_grid_executor/store.py`, `…/migrations/m0001..m0004` | DB-001..005, HIST-002/004, RISK-001, UI-001/003 |
| Authoritative history | `…/neutral_grid_executor/history.py` (HistoryScanner), `…/lighter_port.py` | HIST-001..003, DB-005 |
| Узкое расширение Lighter-коннектора | `hummingbot/connector/derivative/lighter_perpetual/lighter_perpetual_{derivative,api_utils,constants}.py` (аддитивно) | HIST-001/002/004, ORD-001/002 |
| Engine (один на все ячейки) + FakeExchange | `…/neutral_grid_executor/{engine,executor,fake_exchange,commands,snapshot,data_types}.py` | все orchestration NG, OPS-001..003 |
| Generic controller | `controllers/generic/neutral_grid.py` | ARCH-001/003 |
| Robinhood launcher + disabled example | `scripts/lighter_robinhood_fixed_neutral_grid.py`, `conf/{scripts,controllers}/lighter_robinhood_fixed_neutral_grid.yml.example` | ARCH-001..003 |
| Локальный web UI/API | `web/neutral_grid/**`, `bin/lighter_robinhood_neutral_grid_web.py` | UI-001..003, §11 |
| Трассировка/решения | `docs/neutral-grid/TRACEABILITY.md`, `docs/neutral-grid/trace/ws-{a,b,c,d,e}-*.md` | — |

Не изменены (проверено `git diff --stat ba597722d <<HEAD>>` по путям — пусто): `scripts/lighter_robinhood_neutral_grid.py`, `scripts/lighter_robinhood_multi_grid_strike.py`, `bin/lighter_robinhood_multi_grid_strike_launch.py`, `controllers/generic/{multi_grid_strike,lighter_robinhood_multi_grid_strike}.py`, `hummingbot/strategy_v2/executors/grid_executor/**`, старые example configs. Ни одного `PositionExecutor` на ячейку; один `NeutralGridExecutor`, одна asyncio-задача engine на все 55 ячеек.

## 2. Ключевые решения (подробно — в `trace/ws-*.md`, раздел Decisions)

1. **Store авторитетен, core — проекция.** Все переходы/outbox/история применяются через `NeutralGridStore` в одной SQLite-транзакции (WAL + `synchronous=FULL` + `fullfsync`, STRICT-таблицы, только TEXT/INTEGER, float и Decimal-параметры отвергаются, триггеры append-only). Pure `CellLedger` строится из строк store; расхождение → freeze.
2. **Intent before side effect.** intent+CID+reservation tx → `mark_dispatching` tx (с owner-token процесса, m0004) → transport → result tx. PENDING после рестарта = доказанно не отправлен (тот же CID); DISPATCHED = UNKNOWN навсегда до evidence/аудита. NOT_SENT/zero-fill принимаются только для первой попытки того же процесса без venue evidence. Exactly-once — только для ledger effect, не для размещения на бирже.
3. **CID** = `cid_epoch << 40 | seq` < 2^48, `cid_map` append-only, та же identity → тот же CID; коллизия/исчерпание — fail closed; восстановление через аудит `retire_colliding_cid` (только зафиксированный конфликтующий CID). Коннектор отказывает в повторной отправке CID на всё время жизни процесса.
4. **История.** Сканер: новые→старые, `limit=100`, непрозрачный cursor, полный overlap до durable high-water; найденный ID/дубликат не останавливает обход; конфликтующие ключи никогда не предлагаются к применению (удерживаются для аудита); order key = точный exchange id; повтор/битый cursor/обрыв → incomplete. Terminal release = точная terminal-строка + полный scan + равенство cumulative + settlement delay + повторный scan.
5. **CID-ордера принадлежат engine**: исключены из legacy per-order polling, lost-order auto-cancel, `cancel_all`/`limit_orders`, поэтому штатный Hummingbot `stop` не отменяет их в обход durable intent; stop идёт только через engine drain (STOPPED / STOPPED_WITH_INVENTORY / STOP_UNCERTAIN).
6. **Freeze'ы scoped.** История/инвариант конкретной ячейки блокирует TP этой ячейки; неатрибутируемый конфликт — глобально (fail closed). Risk-reducing cancel (outside bounds, stop) продолжаются. Новые entries блокируются глобально.
7. **Аудиты привязаны к увиденному**: `ack_history_conflict` несёт `conflict_set_id` опубликованного набора; baseline audit/`resolve_unknown_submit` отклоняются, пока evidence не устоялось (свежая позиция после последнего коммита истории, нет pending WS, walk после чтения, отдельная задержка `unknown_resolution_delay_s`).
8. **START привязан к конфигу** (fingerprint всех полей GridConfig); CONFIRM_BASELINE только после применённого START; auto-START launcher'а не снимает durable STOP; Hummingbot stop не может быть отменён повторной отправкой START.
9. **Web** только читает committed snapshot и пишет в durable command queue (authorizer: INSERT в `commands`); 127.0.0.1 по умолчанию; session + CSRF + Origin/Host; строгий 409 при stale revision для всех команд с повторной отправкой только кликом оператора; редакция секретов на единственной точке вывода JSON; нет CDN, нет localStorage для секретов; ids/decimals — строки.
10. **Host-wide single-writer lock и prior-run marker** в `~/.hummingbot/neutral_grid/{locks,markers}` (override `HUMMINGBOT_NEUTRAL_GRID_HOST_DIR`); документировано, что это НЕ распределённый lock.
11. **Caps** — пользовательский лимит (профиль 1000 LIT — default + warning, не потолок); 5 LIT нигде не зашит; минимумы/шаг/тик — из свежих trading rules.

## 3. Процесс и независимые проверки внутри исполнения

- Контракт-first, 5 параллельных воркстримов (A core, B store, C connector/history, D engine/controller/launcher, E web) в отдельных worktree/ветках `codex/ng-*`, интеграция в `codex/neutral-grid-implementation`.
- 4 раунда адверсариального ревью (по 3 линзы: spec, честность тестов, запрещённые компромиссы; каждое замечание проверял отдельный скептик): раунд 1 — 85 подтверждённых дефектов (из 161 сырого); верификация исправлений — 71 FIXED / 14 PARTIAL + 7 от критиков; раунд 3 — 14/21 FIXED, 7 PARTIAL + 4 новых; раунд 4 — исправления у D/E (см. §6: **раунд 4 не прошёл независимую пере-проверку** — передаётся Codex).
- Каждое исправление — RED-тест до фикса (SHA red-коммитов в trace), для test-gap — мутационные проверки.
- Сырые результаты ревью: `docs/neutral-grid/reviews/*.json`.

## 4. AC → тесты

`docs/neutral-grid/TRACEABILITY.md` — сгенерирован из `trace/ws-*.md` и сверен с `pytest --collect-only`: <<TRACE>>. Все AC-01..AC-57 имеют ≥1 реально собранный и прошедший тест. Тонкие места покрытия (1 тест): AC-04, AC-10, AC-14, AC-32, AC-36, AC-41.

## 5. Команды и результаты (свежий прогон на `<<HEAD>>`)

`PY=$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python` (Python 3.12.14, lighter-sdk 1.1.4, pytest 9.1.1). Свежий worktree требует 61 Cython `.so` (скопированы из основного checkout, `.gitignore`d; `.pyx/.pxd` не менялись) или `setup.py build_ext --inplace`. Браузерные тесты используют headless Chrome через CDP (`NGWEB_CHROME` для override) и **skip-аются при его отсутствии** — skip ≠ PASS.

<<GATE>>

Baseline до изменений (чистый `a18cb37d4`, тот же набор регрессий): 270 passed.

## 6. Нерешённые риски и то, что требует независимой проверки

<<RISKS>>
