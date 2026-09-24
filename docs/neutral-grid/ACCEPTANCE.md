# Acceptance report — neutral fixed-cell grid (черновик для независимой приёмки Codex/Astra)

**Статус:** DRAFT. Исполнитель (Claude) передаёт на независимую приёмку. Заявления ниже — producer evidence, не независимый APPROVED.
**ТЗ:** `docs/superpowers/specs/2026-09-24-neutral-grid-spec.md`, handoff `docs/tasks/claude-neutral-grid.md`.
**Spec commit / branch point:** `ba597722d` (`origin/codex/lighter-robinhood`). Исходная codebase до spec — `76411e04b`.
**Ветка:** `codex/neutral-grid-implementation`. **Проверенный код:** `6f0b36ef4` (следующий коммит ветки меняет только документацию: этот отчёт, трассировку, handoff и сырые улики ревью). PR: base `codex/lighter-robinhood`, draft, без auto-merge.
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

Не изменены (проверено `git diff --stat ba597722d 6f0b36ef4` по путям — пусто): `scripts/lighter_robinhood_neutral_grid.py`, `scripts/lighter_robinhood_multi_grid_strike.py`, `bin/lighter_robinhood_multi_grid_strike_launch.py`, `controllers/generic/{multi_grid_strike,lighter_robinhood_multi_grid_strike}.py`, `hummingbot/strategy_v2/executors/grid_executor/**`, старые example configs. Ни одного `PositionExecutor` на ячейку; один `NeutralGridExecutor`, одна asyncio-задача engine на все 55 ячеек.

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

`docs/neutral-grid/TRACEABILITY.md` — сгенерирован из `trace/ws-*.md` и сверен с `pytest --collect-only`: 954 собранных теста, 597 ссылок в trace-файлах, 0 висящих. Все AC-01..AC-57 имеют ≥1 реально собранный и прошедший тест. Тонкие места покрытия (1 тест): AC-04, AC-10, AC-14, AC-32, AC-36, AC-41.

## 5. Команды и результаты (свежий прогон на `6f0b36ef4`)

`PY=$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python` (Python 3.12.14, lighter-sdk 1.1.4, pytest 9.1.1). Свежий worktree требует 61 Cython `.so` (скопированы из основного checkout, `.gitignore`d; `.pyx/.pxd` не менялись) или `setup.py build_ext --inplace`. Браузерные тесты используют headless Chrome через CDP (`NGWEB_CHROME` для override) и **skip-аются при его отсутствии** — skip ≠ PASS.

```bash
PY=$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python
# G1: всё новое + регрессия, ОДИН прогон
$PY -m pytest -q -p no:cacheprovider -rs \
  test/hummingbot/strategy_v2/executors/neutral_grid_executor test/controllers/generic test/web/neutral_grid \
  test/hummingbot/connector/derivative/lighter_perpetual \
  test/scripts/test_lighter_robinhood_grid_risk.py test/scripts/test_lighter_robinhood_neutral_grid.py \
  test/hummingbot/strategy_v2/executors/grid_executor
# G2 / G3: тяжёлые property sweeps
NG_PROPERTY_SEEDS=60 NG_PROPERTY_STEPS=120 $PY -m pytest -q -p no:cacheprovider test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/test_ng_engine_properties.py
NG_CORE_PROPERTY_SEEDS=400 NG_CORE_PROPERTY_STEPS=150 $PY -m pytest -q -p no:cacheprovider test/hummingbot/strategy_v2/executors/neutral_grid_executor/core/test_properties.py
# G4: compile / lint / whitespace / JS syntax по всем изменённым .py
git diff --name-only --diff-filter=d ba597722d HEAD -- '*.py' | tr '\n' '\0' | xargs -0 $PY -m py_compile
git diff --name-only --diff-filter=d ba597722d HEAD -- '*.py' | tr '\n' '\0' | xargs -0 $PY -m flake8
git diff --check ba597722d HEAD
node --check web/neutral_grid/static/app.js
```

| Gate | Результат на `6f0b36ef4` (2026-09-24T08:22Z) |
|---|---|
| G1 single run (новые + Lighter connector + neutral/risk scripts + grid_executor + controllers/generic, включая закоммиченные версии двух «старых» тестов) | **1084 passed, 285 subtests passed, 0 skipped, 0 failed** (7 мин 17 с) — браузерные тесты реально выполнены headless Chrome |
| G2 engine property sweep 60×120 | **61 passed** |
| G3 core property sweep 400×150 | **3 passed, 1000 subtests passed** |
| G4 py_compile / flake8 / node --check | OK по 101 изменённому `.py`; `app.js` OK |
| G4 `git diff --check ba597722d HEAD` | OK для кода; единственное замечание было в `TRACEABILITY.md` (пустая строка в конце) — исправлено в docs-коммите |
| G5 старые entrypoints/GridExecutor/старые example configs | `git diff --stat` пусто |
| G6 реальный `~/.hummingbot/neutral_grid` | не создан тестами |
| G7 trace → collect-only | 954 теста, 597 ссылок, 0 висящих; все AC покрыты |
| Окружение | `pip freeze` окружения до/после работы идентичен (ничего не устанавливалось) |

Baseline до изменений (чистый `a18cb37d4`, тот же набор регрессий): 270 passed.

## 6. Нерешённые риски и то, что требует независимой проверки

**A. Не перепроверено независимо (раунд 4, только producer evidence)** — H1 (WS-гейт `resolve_unknown_submit` по trade-id меткам + `unknown_resolution_delay_s`), H2 (START после Hummingbot stop; resume только по последнему применённому STOP), M1 (привязка `ack_history_conflict` к опубликованному `conflict_set_id`, канонические opaque ключи/fingerprint, паритет web↔engine), M2 (durable запись противоречий до ack), M3 (scoped startup для атрибутированных проблем истории), M4 (порядок аудита baseline vs active list и любая строка истории), L1 (rebind START до bootstrap), web: редакция не портит id внутри `result`. Исходные сценарии — `reviews/round3-verification.json` (`open`, `critic`).

**B. Принципиальные ограничения (задокументированы, fail-closed):**
1. Отсутствие ордера после UNKNOWN submit недоказуемо; операторский `resolve_unknown_submit` допускается после `unknown_resolution_delay_s` (120 s) + settlement scans + свежий active list. Лаг active list биржи больше этой выдержки остаётся риском оператора.
2. Venue idempotence одного CID не документирована → DISPATCHED submit остаётся SUBMIT_UNKNOWN до evidence/аудита; STOP в этом случае честно `STOP_UNCERTAIN`.
3. SDK 1.1.4 не документирует definitive zero-fill reject → адаптер никогда не выдаёт `DEFINITIVE_REJECT_ZERO_FILL`; только доказанный pre-send `NOT_SENT` освобождает intent.
4. Порядок сортировки `accountInactiveOrders` не документирован → сканер fail-closed при нарушении порядка (`ordering_violation`).
5. Fake exchange не доказывает реальную консистентность/лаг Lighter; settlement delay/scans/overlap — проектные значения (NG-ARCH-003), требуют калибровки.
6. Вес запросов: steady state ≈15.6k/min при `poll_interval_s=5` из пула Standard 18k (и коннектор тоже его тратит); при нехватке история становится stale и entries блокируются (proof не пропускается). Перед production — настройка `poll_interval_s`/`history_freshness_s`.
7. Ledger correction (принять некоммиченную версию закоммиченного ключа) не поддержана (R6) → такие конфликты требуют ручной реконсиляции вне engine; engine остаётся FROZEN.
8. Durable STOP не снимается автоматически; после любого stop нужен web START или launcher-фраза resume.
9. Single-writer lock — только на хосте (не распределённый), как и требует ТЗ.
10. Default ledger path перенесён на per-account/market; файл старой схемы/пути не подхватывается без явного `db_path`; launcher отказывает при нечитаемом ledger.
11. Late execution на ноге, сохранённой как REJECTED_ZERO_FILL, закрывается тем же аудитом, но без отдельного fake-сценария (fake не умеет «отклонить и исполнить» один submit).
12. TP–TP FIFO через линию anchor может задерживать выходы в быстрых двусторонних движениях (видно как `WAIT_TP_FIFO` с возрастом очереди).
13. CLI-подтверждение live start — фраза в конфиге; интерактивный путь — web.
14. Покрытие одним тестом: AC-04, AC-10, AC-14, AC-32, AC-36, AC-41.

**C. Не делалось по ТЗ:** live testing/trading, authenticated API, реальные ордера, запуск legacy-бота, merge PR.
