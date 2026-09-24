# Независимая приёмка Codex — neutral grid, 2026-09-24

**Статус документа:** FINAL.
**Вердикт проверенного HEAD `c0293c71a95046d0a6e7f95efc2114c50ba33601`:** **CHANGES_REQUESTED / НЕ ПРИНЯТО**.
**Итог после исправлений:** **OFFLINE ACCEPTED / ПРИНЯТО ДЛЯ ОФЛАЙН-ИНТЕГРАЦИИ** — CR-01…CR-03
исправлены, независимо просмотрены и прошли gates ниже. Live authentication/API/orders этой приёмкой не доказаны.
**Base:** `ba597722d6619fea2f38d647b7cc31d3b9b7f818`.

Проверка была офлайн. Live credentials, authenticated Robinhood API, реальные ордера, установка зависимостей и
merge не использовались.

## Исторические findings исходного HEAD — все разрешены

### CR-01 — P1 — две durable leg могут получить один exchange order ID

**Post-fix статус:** RESOLVED в `06f9fc55db62e2a24b13c09bdd47b06f1bebfbad`.

**Owner:** WS-B (store).
**Требования:** NG-HIST-001, NG-HIST-002, NG-DB-002; AC-22, AC-40, AC-43.

В [store.py на проверенном HEAD](https://github.com/VALUT777/hummingbot/blob/c0293c71a95046d0a6e7f95efc2114c50ba33601/hummingbot/strategy_v2/executors/neutral_grid_executor/store.py#L3319-L3324) наличие owned client ID
сразу выбирает CID. Exchange ID проверяется только против exchange ID уже выбранной строки
([строки 3341–3344](https://github.com/VALUT777/hummingbot/blob/c0293c71a95046d0a6e7f95efc2114c50ba33601/hummingbot/strategy_v2/executors/neutral_grid_executor/store.py#L3341-L3344)). Если у этой
строки exchange ID ещё неизвестен, код записывает ID из trade без проверки, что тот уже принадлежит другой leg
([строки 3358–3359](https://github.com/VALUT777/hummingbot/blob/c0293c71a95046d0a6e7f95efc2114c50ba33601/hummingbot/strategy_v2/executors/neutral_grid_executor/store.py#L3358-L3359)). Схема создаёт
обычный, не unique индекс [m0001_initial.py:288](https://github.com/VALUT777/hummingbot/blob/c0293c71a95046d0a6e7f95efc2114c50ba33601/hummingbot/strategy_v2/executors/neutral_grid_executor/migrations/m0001_initial.py#L288).

Триггер: leg A сохранена как accepted без exchange ID; leg B уже имеет exchange ID `9007199254741893`;
история возвращает trade с client ID A и exchange ID B. Scanner завершает scan как полный без конфликта, store
зачисляет fill A и присваивает тот же exchange ID обеим строкам. Повторный probe показал:

```text
scan True [] 1
store_conflicts []
applied [(1099511627777, '9007199254741893', '1')]
A 1 9007199254741893
B 0 9007199254741893
same_exchange_id_rows 2
ledger_problems []
```

Это разрушает однозначную связь execution → durable leg: fill может быть отнесён не тому циклу, а последующие
поиски по exchange ID становятся неоднозначными без durable conflict/freeze. Scanner не делает этот pre-gate;
engine передаёт evidence в store, поэтому защита нужна на durable boundary, а не только в одном вызывающем пути.

**Критерии исправления и обязательные regression tests:**

1. Два non-null `orders.exchange_order_id` не могут принадлежать разным CID, включая migration существующей БД.
2. Если client ID и exchange ID одной evidence-строки разрешаются в разные CID, строка становится durable
   history conflict, fill не применяется и leg/order не изменяются.
3. `verify_ledger()` обнаруживает уже сохранённый duplicate/mismatched mapping и fail-closed блокирует exposure.
4. Store-level тест воспроизводит probe; существующий engine scoped-conflict путь покрывает потребление durable
   conflict, а restart сохраняет conflict и не угадывает владельца.

Probe: [cr01_dual_identity.py.txt](reviews/probes/codex-acceptance-2026-09-24/cr01_dual_identity.py.txt).

### CR-02 — P2 — maker-only ограничение выбранного API key не входит в TP admission

**Post-fix статус:** RESOLVED в `06f9fc55db62e2a24b13c09bdd47b06f1bebfbad` и web delta
`7f8346ccdb37347bc17e1538ad2666bba00d79be`.

**Owner:** WS-C (connector/history port).
**Требования:** NG-ORD-001, NG-GRID-003, NG-ARCH-003; AC-01, AC-02, AC-38.

[lighter_port.py:332–353 на проверенном HEAD](https://github.com/VALUT777/hummingbot/blob/c0293c71a95046d0a6e7f95efc2114c50ba33601/hummingbot/strategy_v2/executors/neutral_grid_executor/lighter_port.py#L332-L353)
вычисляет `supports_limit` и `supports_post_only` только из состояния рынка. Выбранный
`connector._api_key_index` не сверяется со списком maker-only ключей. В установленном SDK 1.1.4 метод
`AccountApi.get_maker_only_api_keys` доступен, но probe с выбранным ключом `4` в venue list `[4]` показал:

```text
configured_api_key 4
venue_maker_only_indexes [4]
maker_only_capability_checked 0
reported_supports_limit True
reported_supports_post_only True
```

Probe доказывает validation gap; он не заявляет результат live reject. Обычный TP по требованиям — LIMIT GTT,
который должен иметь возможность исполниться taker. Maker-only ключ несовместим с этим контрактом, однако preview
и admission сейчас считают capability доступной.

**Критерии исправления и обязательные regression tests:**

1. До admission/start выбранный API key проверяется через фактический SDK contract; maker-only выбранный key
   блокирует обычный LIMIT TP и виден как точная причина.
2. Ошибка, неизвестный ответ или отсутствие доказательства capability блокируют новую exposure fail-closed.
3. Проверка не переключает account/key автоматически и не выбирает другой ключ за оператора.
4. Тесты покрывают maker-only выбранный key, обычный key и unavailable/malformed capability response.

Probe: [cr02_maker_only_key.py.txt](reviews/probes/codex-acceptance-2026-09-24/cr02_maker_only_key.py.txt).

### CR-03 — P2 — web selection/unlock не были связаны с attached engine

**Post-fix статус:** RESOLVED в `06f9fc55db62e2a24b13c09bdd47b06f1bebfbad`.

**Owner:** WS-E (web).
**Требования:** NG-UI-001, NG-UI-002; AC-46, AC-47, AC-49.

В проверенном HEAD attach создавал отдельный process-local `KeystoreService` в
[runtime.py:267 на проверенном HEAD](https://github.com/VALUT777/hummingbot/blob/c0293c71a95046d0a6e7f95efc2114c50ba33601/web/neutral_grid/runtime.py#L267), а Start зависел только от snapshot preview и durable
command gateway. UI при этом обещал, что LIVE будет работать «от имени выбранного профиля»
([app.js:891 на проверенном HEAD](https://github.com/VALUT777/hummingbot/blob/c0293c71a95046d0a6e7f95efc2114c50ba33601/web/neutral_grid/static/app.js#L891)). Выбор/разблокировка в web-процессе не выбирали и не
разблокировали credentials уже работающего Hummingbot engine. Probe с `keystore_exists=False`, без выбранного и
разблокированного профиля получил `202 / QUEUED` для Start.

Attach-only архитектура корректна: панель управляет уже привязанным Hummingbot engine через snapshots/command
queue. Дефект состоял в несуществующем credential control и ложном operator-facing утверждении, а не в обходе
проверки ключа самим engine.

**Критерии исправления и обязательные regression tests:**

1. Attach показывает read-only identity из fingerprint-verified committed snapshot: connector, account, pair,
   grid; при отсутствующем/повреждённом snapshot identity не подставляется из defaults.
2. Credentials выбираются и разблокируются в Hummingbot host. Attach backend не читает сторонний local keystore,
   не принимает пароль и отвечает отказом на попытки сменить профиль/unlock.
3. Start dialog называет attached connector/account/pair и не утверждает, что web-selected profile влияет на engine.
4. Demo остаётся офлайн и не читает keystore; session/CSRF/Origin и отсутствие secret readback сохраняются.

Probe: [cr03_detached_web_profile.py.txt](reviews/probes/codex-acceptance-2026-09-24/cr03_detached_web_profile.py.txt).

### Проверенное исправление CR-03

Attach-контекст больше не создаёт и не читает отдельный local keystore. `GET /api/keystore` возвращает только
read-only identity последнего committed snapshot, а обе mutation routes отказывают с `409` до чтения тела
пароля/профиля. В UI attached identity остаётся видима; selector профиля и unlock скрыты. Заголовок обновляет
identity из `/api/state`, а подтверждение Start берёт connector/account/pair из того же validated preview,
чьи `preview_id` и revisions связываются с командой. Поэтому даже в окне до очередного state poll диалог не
подставляет устаревший web profile. При отсутствующей identity панель явно показывает «Привязка неизвестна».

CLI attach больше не принимает `--profile`/`--unlock-tty`; документация прямо относит выбор и разблокировку
ключей к процессу Hummingbot и называет проверку snapshot именно core-отпечатком, а не криптографической подписью.
Red/green regressions покрывают API refusal, отсутствие default identity, фактическую видимость DOM, late snapshot
и immediate-preview race. Свежий полный web suite: **158 passed, 0 skipped, 4 upstream deprecation warnings,
175.88 s**; реальные Chrome cases выполнены. Лог: `/tmp/ng-codex-cr03-web-final.log`.

Независимая ручная перепроверка attach UI на отдельном fake demo-engine DB показала connector, account `4242`,
pair и grid в видимой карточке; selector/unlock отсутствовали, ложного утверждения о разблокировке не было.
Reload сохранил корректную identity. Fake процессы и вкладки после проверки закрыты.

## Повторная проверка исправлений

* **CR-01:** migration добавляет уникальное владение non-null exchange order ID. Legacy БД с уже существующими
  duplicate ID намеренно получает отказ migration без удаления или автоматического исправления данных. Scanner
  и store regression tests подтверждают durable conflict без применения fill; существующий engine path потребления
  scoped conflict отдельно просмотрен и покрыт suite.
* **CR-02:** выбранный API key проверяется при каждом обновлении rules; maker-only и недоказанная capability дают
  фиксированные публичные blocker codes и не переключают account/key. Engine блокирует новую экспозицию. Attach
  preview сохраняет blocker из committed runtime rules, включает его в material identity и запрещает Start даже
  при all-`LIMIT_MAKER` конфигурации; неизвестное значение fail-closed.
* **CR-03:** attached identity берётся только из committed engine snapshot; web profile/unlock удалены из attach,
  Start подтверждает identity того же validated preview, который связывается с командой.

Независимый review всех трёх исправлений завершён без открытых findings. Итог ниже относится к офлайн-коду и
fake/in-process интеграции; live authentication, Robinhood API и реальные ордера не проверялись.

## Финальные gates после исправлений

Broad code commit: `06f9fc55db62e2a24b13c09bdd47b06f1bebfbad`. Финальный web capability delta:
`7f8346ccdb37347bc17e1538ad2666bba00d79be`.

| Gate | Commit | Результат |
|---|---|---|
| G1 — полный regression scope | `06f9fc55db62e2a24b13c09bdd47b06f1bebfbad` | 1103 passed, 299 subtests passed, 0 skipped/failed, 8 warnings, 563.93 s |
| G2 — 60 × 120 engine property sweep | `06f9fc55db62e2a24b13c09bdd47b06f1bebfbad` | 61 passed, 0 failed, 1 warning, 321.74 s |
| G3 — 400 × 150 pure-core sweep | `c0293c71a95046d0a6e7f95efc2114c50ba33601` | 3 passed, 1000 subtests passed, 96.55 s; pure core после этого не менялся |
| Финальный полный web suite | `7f8346ccdb37347bc17e1538ad2666bba00d79be` | 162 passed, 0 skipped/failed, 4 warnings, 177.07 s |
| Headless browser в финальном web suite | `7f8346ccdb37347bc17e1538ad2666bba00d79be` | 9 cases реально выполнены через Chrome for Testing; skip отсутствует |
| G4 | `7f8346ccdb37347bc17e1538ad2666bba00d79be` | py_compile + flake8 по 104 changed `.py`, diff-check и node-check: PASS |

Логи: `/tmp/ng-codex-final-g1.log`, `/tmp/ng-codex-final-g2.log`,
`/tmp/ng-codex-final-web-delta.log`, `/tmp/ng-codex-final-g4-web-delta.log`. G4 собирал union changed и
untracked Python при запуске; после commit тот же набор воспроизводится через diff
`ba597722d6619fea2f38d647b7cc31d3b9b7f818..7f8346ccdb37347bc17e1538ad2666bba00d79be`.

G1/G2 запускались точными pytest-командами из следующего раздела на commit `06f9fc55…`. Финальная web/G4
дельта проверена так:

```bash
PY=/Users/mikhail/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python
$PY -m pytest -q -p no:cacheprovider -rs test/web/neutral_grid

git diff --name-only --diff-filter=d \
  ba597722d6619fea2f38d647b7cc31d3b9b7f818 7f8346ccdb37347bc17e1538ad2666bba00d79be -- '*.py' \
  | tr '\n' '\0' | xargs -0 $PY -m py_compile
git diff --name-only --diff-filter=d \
  ba597722d6619fea2f38d647b7cc31d3b9b7f818 7f8346ccdb37347bc17e1538ad2666bba00d79be -- '*.py' \
  | tr '\n' '\0' | xargs -0 $PY -m flake8
git diff --check ba597722d6619fea2f38d647b7cc31d3b9b7f818 7f8346ccdb37347bc17e1538ad2666bba00d79be
node --check web/neutral_grid/static/app.js
```

Исправления сохранены в указанных коммитах. Merge и live API/orders не выполнялись.

## Исторические gates на исходном отклонённом HEAD

Использован Python `/Users/mikhail/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python`.

```bash
PY=/Users/mikhail/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python

# G1
$PY -m pytest -q -p no:cacheprovider -rs \
  test/hummingbot/strategy_v2/executors/neutral_grid_executor test/controllers/generic test/web/neutral_grid \
  test/hummingbot/connector/derivative/lighter_perpetual \
  test/scripts/test_lighter_robinhood_grid_risk.py test/scripts/test_lighter_robinhood_neutral_grid.py \
  test/hummingbot/strategy_v2/executors/grid_executor

# G2
NG_PROPERTY_SEEDS=60 NG_PROPERTY_STEPS=120 $PY -m pytest -q -p no:cacheprovider \
  test/hummingbot/strategy_v2/executors/neutral_grid_executor/engine/test_ng_engine_properties.py

# G3
NG_CORE_PROPERTY_SEEDS=400 NG_CORE_PROPERTY_STEPS=150 $PY -m pytest -q -p no:cacheprovider \
  test/hummingbot/strategy_v2/executors/neutral_grid_executor/core/test_properties.py

# G4
git diff --name-only --diff-filter=d ba597722d6619fea2f38d647b7cc31d3b9b7f818 HEAD -- '*.py' \
  | tr '\n' '\0' | xargs -0 $PY -m py_compile
git diff --name-only --diff-filter=d ba597722d6619fea2f38d647b7cc31d3b9b7f818 HEAD -- '*.py' \
  | tr '\n' '\0' | xargs -0 $PY -m flake8
git diff --check ba597722d6619fea2f38d647b7cc31d3b9b7f818 HEAD
node --check web/neutral_grid/static/app.js
```

| Gate | Свежий результат |
|---|---|
| G1 | 1084 passed, 285 subtests passed, 0 skipped, 0 failed, 563.74 s |
| Headless browser внутри G1 | 7 browser cases реально выполнены через Chrome for Testing; skip отсутствует |
| G2 | 61 passed, 330.38 s |
| G3 | 3 passed, 1000 subtests passed, 96.55 s |
| G4 | py_compile + flake8 по 101 changed `.py`, `git diff --check`, `node --check`: PASS |

Логи: `/tmp/ng-codex-g1-web.log`, `/tmp/ng-codex-g2-web.log`, `/tmp/ng-codex-g3-web.log`,
`/tmp/ng-codex-g4-web.log`. Эти локальные пути — evidence текущей сессии, не часть репозитория.

Все 57 AC имеют producer trace coverage, но это не означает, что независимая проверка заново доказала каждый AC.
Findings выше показывают, что зелёный trace/gate не покрывал три конкретных контрпримера.

## Дополнительная ручная и интеграционная проверка

В офлайн UI на fake exchange независимо пройдено: Start → baseline confirmation → `NORMAL`, 40 active из 55;
SELL entry partial `3 + 3` создал TP `6` при live remainder entry `4`; TP `2 + 2 + 2` завершил TP без rearm,
с тем же generation/CID и entry remainder `4`; Stop завершился `STOPPED`, owned/unknown orders — `0`.

Отдельный actual-executor probe проверил web `STOP` → live control loop → fresh web `START`: команда применена,
состояние вернулось в `NORMAL`. Partial TP сохранил `E=10`, `X=4`, remaining `6`, generation `1`; risk caps прошли.

Экспериментальный core sweep `500 × 250` упёрся в установленный per-test timeout 120 s на seed 292. Это не failure
официального gate и не обнаруженный invariant failure: официальный G3 `400 × 150` завершился успешно. Более тяжёлый
sweep требует отдельного согласованного timeout/budget.

## Воспроизведение исторических probes

Эти `.py.txt` сохраняют контрпримеры для исходного `c0293c71a95046d0a6e7f95efc2114c50ba33601` и не являются
post-fix acceptance tests. После исправлений старое ожидаемое поведение probe может больше не воспроизводиться.
Каждый файл намеренно не собирается pytest и не входит в lint. Для исторического запуска checkout исходный commit,
затем скопируйте probe во временный `.py`:

```bash
BASELINE_REPO="/path/to/c029-checkout"  # checkout c0293c71a95046d0a6e7f95efc2114c50ba33601
REVIEW_REPO="/path/to/current-review-checkout"  # contains this report and its probe copies
PY=/Users/mikhail/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python
PROBES="$REVIEW_REPO/docs/neutral-grid/reviews/probes/codex-acceptance-2026-09-24"
RUN_DIR="$(mktemp -d /tmp/ng-codex-probes.XXXXXX)"
HOST_DIR="$(mktemp -d /tmp/ng-codex-host.XXXXXX)"

for source in "$PROBES"/*.py.txt; do
  target="$RUN_DIR/$(basename "$source" .txt)"
  cp "$source" "$target"
  PYTHONPATH="$BASELINE_REPO" HUMMINGBOT_NEUTRAL_GRID_HOST_DIR="$HOST_DIR" "$PY" "$target"
done
```

Post-fix acceptance опирается на regression tests и финальные gates выше, а исторические probes сохраняются как
аудируемые контрпримеры исходного отклонённого HEAD. Автоматического принятия только по зелёным тестам нет.
