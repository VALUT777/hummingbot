# Handoff Codex/Astra: независимая приёмка neutral fixed-cell grid

Встречный документ к `docs/tasks/claude-neutral-grid.md`. Claude (координатор) исполнил ТЗ `docs/superpowers/specs/2026-09-24-neutral-grid-spec.md` и передаёт **черновик** на независимую приёмку. Всё ниже — producer evidence; независимый вердикт — за Codex. По распоряжению владельца Claude дальше не продолжает до вашего вердикта/возврата.

## 1. Что принимать

| Что | Значение |
|---|---|
| Ветка PR | `codex/neutral-grid-implementation` → base `codex/lighter-robinhood`, **draft PR, без auto-merge** |
| Spec commit / branch point | `ba597722d` (`origin/codex/lighter-robinhood`) |
| Контракт воркстримов | `a18cb37d4` (`docs/neutral-grid/CONTRACTS.md`, `neutral_grid_executor/contracts.py`) |
| Handoff HEAD | см. последний коммит ветки (этот документ входит в него); проверенный код — `6f0b36ef4` |
| Финальные источники воркстримов (все — предки HEAD) | core `79f9e2f71`, store `1c8ec8be4` (schema v4), connector `a01d679e4`, engine `19dc443d6`, web `dbaadc418` |

Полный diff: `git diff ba597722d 6f0b36ef4` — ~115 файлов. Вне новых путей изменены только три файла Lighter-коннектора (аддитивно): `lighter_perpetual_{derivative,api_utils,constants}.py`.

## 2. Где улики

- `docs/neutral-grid/ACCEPTANCE.md` — acceptance report: компоненты, решения, команды и **свежие результаты на `6f0b36ef4`**, нерешённые риски.
- `docs/neutral-grid/TRACEABILITY.md` — NG-* → компоненты; AC-01..57 → pytest node ids (сгенерировано из trace, сверено с `--collect-only`, 0 висящих ссылок).
- `docs/neutral-grid/trace/ws-{a-core,b-store,c-connector,d-engine,e-web}.md` — по воркстримам: NG/AC → node ids, решения, handoff, секции Review fixes / Round 3 / Round 4 с SHA red-коммитов и таблицами мутаций, open risks, команды.
- `docs/neutral-grid/reviews/*.json` — сырые результаты внутренних адверсариальных ревью и верификаций (каждое замечание: файл/строка, требование, сценарий, доказательство, вердикт скептика).
- `docs/neutral-grid/reviews/probes/**.py.txt` — скрипты-пробы верификаторов (переименованы в `.txt`, чтобы не попадать в lint; для запуска вернуть `.py` и поправить пути tmp-каталогов на свои).

## 3. История внутренних проверок (важно для фокуса приёмки)

| Раунд | Что | Итог |
|---|---|---|
| 1 | 6 адверсариальных ревью (A, B, C, E, D×2): 3 линзы + скептик на каждое замечание | 161 сырое → 85 подтверждённых; все исправлены с RED→GREEN |
| 2 | Верификация 85 исправлений + 2 критика (запрещённые компромиссы; регрессии/стыки) | 71 FIXED, 14 PARTIAL; 7 новых (1 HIGH scoped-freeze, утечка auth token в snapshot) |
| 3 | Исправления + верификация 21 пункта + критик раунда 3 | 14 FIXED, 7 PARTIAL; 4 новых (2 HIGH: WS-гейт resolve_unknown_submit, повторный START отменял Stop) |
| 4 | Исправления H1, H2, M1–M4, L1 + паритет M1 web↔engine | **только producer evidence**: RED→GREEN (23 red), 16/16 мутаций убиты (WS-D), e2e на реальном engine (WS-E); **независимо НЕ перепроверено** |

**Рекомендуемый фокус независимой приёмки:** (1) раунд 4 целиком (см. `trace/ws-d-engine.md` «Round 4», `trace/ws-e-web.md` «Round 4: M1» и «Final M1 contract»; открытые пункты раунда 3 — `reviews/round3-verification.json` → `open`, `critic`); (2) `engine.py` (≈2.9k строк: stop/restart/unknown submit/аудиты/scoped freeze); (3) транзакционная семантика `store.py` (outbox, owner token m0004, tx poisoning); (4) изменения коннектора для НЕ-grid вызовов (legacy polling, `cancel_all` для обычных ордеров, restore); (5) web security (loopback, session/CSRF/Origin, редакция без порчи id).

## 4. Воспроизведение

- Python: `$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python` (3.12.14; lighter-sdk 1.1.4; pytest 9.1.1; flake8 7.3.0). `hypothesis`/`playwright` нет: property-тесты — seeded randomized, браузер — headless Chrome через CDP (`test/web/neutral_grid/ngweb_cdp.py`, override `NGWEB_CHROME`). **Без Chrome браузерные тесты skip — это не PASS.**
- Чистый checkout/worktree требует 61 Cython `.so` (gitignored): скопировать из основного checkout (`.pyx/.pxd` не менялись относительно `ba597722d`) или `setup.py build_ext --inplace`.
- Тесты сами ставят `HUMMINGBOT_NEUTRAL_GRID_HOST_DIR` во временный каталог; реальный `~/.hummingbot/neutral_grid` не создаётся (проверено G6).
- Команды gate — в `ACCEPTANCE.md` §5 (G1–G7), один прогон G1 ≈ 7 мин, sweeps ≈ 6 мин.
- Секреты, live API, реальные ордера, legacy-бот — не использовались и не нужны. Web demo: `python bin/lighter_robinhood_neutral_grid_web.py --demo-fake-exchange` (offline, 127.0.0.1).

## 5. Решения, которые, возможно, требуют подтверждения владельца/Codex

1. `unknown_resolution_delay_s` (default 120 s) — операторский аудит «ордер не дошёл» разрешён только после этой выдержки + settlement scans + свежего active list; абсолютного доказательства отсутствия нет.
2. Строгий 409 для всех 4 web-команд (включая Stop) при stale revision; анти-дребезг engine (3 чистых тика до NORMAL). Внутренний Hummingbot-stop executor'а переотправляется со свежими ревизиями (это собственный CLI-интент оператора).
3. Durable STOP никогда не снимается автоматически: нужен явный web START или launcher-фраза `RESUME <grid_id> AFTER STOP <stop_ms>` с последним применённым stop.
4. `ack_history_conflict` подтверждает только опубликованный набор (`conflict_set_id`); выбор некоммиченной версии для уже закоммиченного ключа (ledger correction) не поддержан (R6) — требует ручной реконсиляции вне engine.
5. Default ledger path перенесён на per-account/market (`<data>/neutral_grid/neutral_grid.<domain>.<account>.<pair>.sqlite3`); host lock/marker — `~/.hummingbot/neutral_grid/{locks,markers}`; lock НЕ распределённый.
6. Caps 1000 LIT — default профиля с warning, не потолок (по ТЗ — пользовательский лимит).

## 6. Состояние исполнителей

Пять сессий-исполнителей (WS-A..E) завершили работу и простаивают; их локальные ветки `codex/ng-{core,store,connector,engine,web}` целиком вошли в `codex/neutral-grid-implementation` и не публиковались отдельно. Возврат замечаний — через Git (CR/handoff с ID, SHA, критерием, ответственным), Claude исправит пачкой.
