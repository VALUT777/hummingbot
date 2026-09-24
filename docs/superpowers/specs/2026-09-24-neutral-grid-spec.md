# Техническое задание: постоянная двусторонняя neutral grid для Robinhood Lighter

**Статус:** готово к передаче Claude; реализация подлежит независимому review Codex.
**База:** commit `76411e04b`, branch `codex/lighter-robinhood`.
**Рынок:** `lighter_perpetual_robinhood`, `LIT-USDG`, Robinhood Lighter chain ID `466324`, market ID определяется в runtime, текущее наблюдаемое значение — `5`, collateral — USDG asset `3`.
**Режим:** perpetual `ONEWAY`, leverage `5x`, short позиции разрешены.

## 1. Цель и границы

Нужно добавить в Hummingbot V2 постоянную двустороннюю сетку с фиксированными ячейками. Каждая ячейка самостоятельно покупает на нижней границе и продаёт на верхней либо продаёт на верхней и откупает на нижней. Цены не переносятся за рынком, а ячейка не начинает новый цикл, пока предыдущий цикл не доказан по истории биржи.

Первая версия ограничена одним account/market/venue и одним процессом-владельцем. Обязателен локальный web UI из раздела 10. Multi-venue, multi-market, adaptive/recentered grid, гарантия эксклюзивности между разными хостами, stop-loss, reward eligibility, автоматическая ликвидация, market fallback, автосоздание стартового инвентаря и точная PnL/наградная атрибуция не входят в scope. Live testing и trading для Claude не авторизованы; browser acceptance выполняется только с fake exchange.

Существующие два незакоммиченных тестовых файла от прежнего эксперимента нужно сохранить без изменений; они не являются источником требований:

- `test/controllers/generic/test_lighter_robinhood_multi_grid_strike.py`;
- `test/hummingbot/strategy_v2/executors/grid_executor/test_grid_executor.py`.

## 2. Модель сетки

### NG-GRID-001. Цены и ячейки

Входные границы сначала проверяются как точные кратные runtime tick и переводятся в integer ticks `L_tick`, `U_tick`. Для `N=cell_count`, `T=U_tick-L_tick` требуется `T >= N`; затем без float строится `P_i_tick = L_tick + floor(i*T/N)`, `i=0..N`. Получаются `N+1` строго возрастающих цен и `N` ячеек `[P[i], P[i+1]]`. Некратные bounds, коллапс, нестрогий порядок или нехватка тиков — ошибка валидации, а не silent quantization.

Целевой пример: диапазон 5–6 USDG, `N=55` ячеек, 56 price lines, `order_amount_base=10 LIT`; это пример, а не зашитые константы. При anchor `5.4` формула integer ticks даёт 22 initial BUY и 33 initial SELL ячейки.

### NG-GRID-002. Якорь и первая сторона

При первом запуске после полной reconciliation фиксируется `anchor = clamp(mid_price, lower_price, upper_price)`. Для ячейки:

- если `P[i] < anchor`, первый entry — BUY по `P[i]`, а TP — SELL по `P[i+1]`;
- иначе первый entry — SELL по `P[i+1]`, а TP — BUY по `P[i]`.

Якорь и цены ячеек после старта не меняются. Нет recenter, trailing, clamp ордера к текущей цене или замены целевой цены после скачка рынка.

### NG-GRID-003. Размер, minimums и округление

`order_amount_base` — плановый размер нового цикла. Он должен быть точным кратным runtime size step; иначе config отклоняется и preview показывает ошибку, без silent resize. Tick, size increment, min size, min notional, max size и доступность limit orders берутся из свежих trading rules. Наблюдаемый минимум 5 LIT нельзя зашивать.

Объём никогда не округляется вверх. Комиссии не уменьшают base quantity для perpetual парной ноги. Если точный остаток меньше runtime minimum, он сохраняется как `DUST` и виден оператору. Агрегация допустима только для остатков с одинаковыми target price и side; в ledger сохраняется allocation каждой ячейке. Нельзя менять цену, добавлять объём или закрывать dust market-ордером.

## 3. Семантика ордеров в ONEWAY

### NG-ORD-001. Типы и цены

- Entry по умолчанию — `LIMIT_MAKER`/post-only по точной цене границы.
- TP по умолчанию — обычный `LIMIT` по точной ценовой границе. Lighter SDK 1.1.4 поддерживает `GoodTillTime` (`GTT=1`), а не буквальный GTC, поэтому «постоянный TP» реализуется через GTT и durable renewal: новый ордер разрешён только после доказанного terminal старого, полного history scan и вычисления точного остатка. Maker-only для TP выключен на уровне ключа/поддерживаемой настройки: если рынок уже перешёл target, limit может исполниться как taker.
- Entry order type и TP order type могут быть явно настраиваемы в разрешённом безопасном подмножестве, но запрещены `MARKET`, price clamping и TP distance/activation gate.

### NG-ORD-002. Виртуальные ячейки и reduce-only

На Lighter есть одна net-позиция. Ячейки — виртуальный ledger, а не exchange hedge legs. Пример: long-ячейка `+10` и short-ячейка `-10` дают venue net `0`; SELL TP long-ячейки закрывает её в ledger, но на бирже создаёт net `-10`.

Поэтому entry и paired TP по умолчанию являются обычными grid orders с `reduce_only=false`. Нельзя слепо использовать `PositionAction.CLOSE`: текущий connector превращает его в `reduce_only=true`. Сторону, target и принадлежность ноги определяет durable ledger.

### NG-ORD-003. Self-trade prevention

Перед каждым submit router проверяет все owned live/pending/unknown orders, включая entry против TP и TP против TP. TP имеет приоритет: конфликтующий entry переводится в cancel-pending, а TP публикуется после доказанного terminal этого entry. TP–TP конфликты сериализуются FIFO; внутренний netting obligations без exchange orders запрещён. Обычный partial fill сам по себе не отменяет исходный entry. Стратегия не создаёт wash/self-match намеренно и не считает exchange self-trade cancellation исполнением.

## 4. Жизненный цикл ячейки

### NG-CELL-001. Блокировка целой ячейки

Ячейка имеет один durable cycle. Она заблокирована с момента entry intent до момента, когда одновременно доказано:

1. entry terminal;
2. весь фактический entry quantity закрыт paired TP;
3. нет активных, cancel-pending, submission-unknown или history-unknown ордеров этого cycle;
4. нет `DUST`;
5. account position и durable ledger согласованы.

Только после этого начинается новый цикл полного `order_amount_base` с исходной entry-side этой ячейки: BUY-ячейка снова выставляет BUY, SELL-ячейка снова выставляет SELL.

### NG-CELL-002. Частичный entry

Каждое исполнение entry из history увеличивает `entry_filled`. Остаток исходного entry продолжает стоять, даже если partial remainder меньше текущего minimum: это уже принятый exchange order, а не новый submit. Для каждой новой авторитетно подтверждённой доли движок немедленно добавляет TP obligation того же base quantity. `ENTRY_LIVE` и `TP_LIVE` допустимы одновременно. В здоровом локальном состоянии durable TP intent передаётся router не позднее 2 секунд после commit history fill, если доступны reserved capacity, throttler и transport и нет self-trade/cap/minimum blocker. Это SLO локального dispatch, не обещание exchange acceptance. При blocker status/UI показывает queue age и точную причину; history lag измеряется отдельно от WS event.

В MVP один cycle имеет один physical entry order; нельзя склеивать entries или physical entry с TP. Он может иметь несколько TP child orders. Каждое исполнение атрибутировано lot/cell. Инвариант `confirmed_exit + reserved_TP_unfilled <= confirmed_entry` обязателен, а незакрытая величина `E-X` разбивается на непересекающиеся buckets: live TP remainder, undispatched/unknown reserved TP remainder и unassigned/dust. Сумма исходных TP requests не заменяет эту формулу. Unknown TP outcome сохраняет reservation; status lag не создаёт duplicate TP.

### NG-CELL-003. Частичный TP и terminal partial entry

Частичный TP только уменьшает обязательство; он не разблокирует ячейку и в нормальном ходе остаётся ждать остаток по той же fixed price, без cancel/repost. Если entry завершился canceled/failed после partial fill, движок закрывает точно фактический cumulative quantity, ждёт полного завершения TP, затем начинает новый cycle полного `order_amount_base`. Late fill после cancel event создаёт новую TP obligation старого cycle; никакой локальный terminal event не фиксирует cumulative без history.

### NG-CELL-004. Минимальные durable states

Модель должна различать `IDLE`, `ENTRY_INTENT`, `ENTRY_LIVE`, `ENTRY_TERMINAL_UNKNOWN`, `TP_REQUIRED`, `TP_INTENT`, `TP_LIVE`, `TP_TERMINAL_UNKNOWN`, `DUST`, `COMPLETE`, `PAUSED`, а также order-level submission/cancel ambiguity. Это не одна взаимоисключающая enum: cell aggregate state и leg/order states должны представлять `ENTRY_LIVE + TP_LIVE` одновременно. Названия могут отличаться, но эти состояния нельзя склеивать так, чтобы потерять различие между intent, активным order и неизвестным результатом.

## 5. Авторитетная история и connector

### NG-HIST-001. Источник истины

Приватный WebSocket — сигнал для быстрого polling, но не окончательное доказательство. Для execution quantity, terminal status, late fills и restart авторитетны cursor-paginated `/api/v1/trades`, `/api/v1/accountInactiveOrders`, текущие active orders и account net position. Исчезновение ордера из active list, локальный cancel/completed event и отсутствие в одной странице не являются terminal proof.

Поля execution identity:

- order: exact string `client_order_id`/`client_order_id_str`, string `order_id`, numeric `order_index`, `nonce`, market/account identity, `filled_base_amount`, `remaining_base_amount`, `status`, timestamps, `reduce_only`;
- trade: `trade_id` и `trade_id_str`, `ask_account_id`/`bid_account_id`, `ask_client_id_str`/`bid_client_id_str`, `ask_id_str`/`bid_id_str`, `size`, `price`, timestamp, maker side.

Идентификаторы никогда не проходят через float. Принадлежность не угадывается по price/size/time.

Фактический REST contract SDK 1.1.4, который connector обязан сохранить:

- `accountInactiveOrders`: обязательны `authorization`, `account_index`, `limit`; доступны `market_id`, `ask_filter`, `between_timestamps`, `cursor`, `market_type`;
- `trades`: обязательны `sort_by`, `limit`; доступны `authorization`, market/account/order filters, `sort_dir`, `cursor`, `from`, `ask_filter`, `role`, `type`, `aggregate`, skip-order filters;
- обе выдачи возвращают `next_cursor`, `limit` не больше 100; exact имена подтверждаются generated client, а не переизобретаются в strategy;
- own trade может присутствовать обеими account legs; нельзя выбросить вторую leg только из-за совпадения account или допущения, что self-trade невозможен.

### NG-HIST-002. Pagination и completeness

Оба history endpoint имеют максимальный `limit=100`, optional `cursor` и response `next_cursor`. Текущий connector берёт одну страницу и игнорирует `next_cursor`; это ограничение нужно исправить в scope connector-метода, не переписывая весь connector.

Обход идёт от новых к старым с `limit=100` и непреобразованным opaque cursor. Для обоих endpoint scan продолжается до естественного конца либо полной проверенной overlap boundary с durable high-water mark. Нахождение unresolved ID или первого duplicate не является условием остановки: неизвестная поздняя запись может находиться на следующей странице. Канонический trade dedupe key: `(domain, account, market, trade_id_str, own_side, own_exchange_order_id)`; fallback разрешён только на точные typed SDK IDs с тем же scope. Float запрещён. Один key с разным payload даёт history conflict и pause. Duplicate boundary row с идентичным payload идемпотентен. Repeated/malformed cursor, schema conflict или обрыв до доказанной boundary делают history incomplete.

Terminal release требует одновременно exact terminal order row, полного scan по правилу выше, равенства суммы owned executions terminal cumulative filled и configurable settlement policy: выдержка плюс повторный scan. Late evidence после reuse относится к старому cycle и переводит весь market в freeze до аудита. При неограниченной задержке истории биржи абсолютную гарантию обнаружения late fill дать нельзя; отсутствие строки само по себе не является proof.

После падения cursor не продвигается отдельно от записи inbox, fills и ledger. Cursor/high-water mark, raw normalized inbox rows, dedupe keys и resulting ledger transition коммитятся одной SQLite transaction.

### NG-HIST-003. Вес запросов и cadence

Для Standard account текущие weights: trades `600`, inactive orders `100`, прочие read endpoints `300`, pool `18000/min`. Poller coalesces simultaneous WS wakeups, не запускает parallel duplicate scans и не читает всю историю каждые 10 секунд. Startup/restart выполняет bounded page work под throttler и остаётся `RECONCILING` между тиками. Steady state сканирует новые страницы до durable overlap boundary. Лимит веса и backoff настраиваются с безопасными defaults; exhaustion задерживает новую exposure, а не пропускает proof.

### NG-HIST-004. Client ID, signer и nonce

Уникальный numeric client order ID выделяется до API side effect, вместе с intent фиксируется в outbox, и только затем без замены передаётся connector. SDK/connector ограничивают его 48 битами (`MAX_CLIENT_ORDER_ID_BIT_COUNT=48`, connector вызывает `int(order_id)`). Durable mapping связывает ID с полным `(grid_id, cell_id, generation, role, revision)`. Переполнение, exhaustion или collision дают fail-closed; нельзя молча truncate/hash без проверки коллизии. После timeout/crash новый ID запрещён. Повтор того же ID допустим только при документированной venue idempotence; иначе outcome остаётся `UNKNOWN` до evidence или ручного аудита.

Текущий lighter SDK выдаёт transaction nonce внутри async signer call; текущий connector не даёт стратегии pre-send nonce. Поэтому order `nonce` является вторичным corroborating field, а primary key — заранее сохранённый client ID в контексте exact account/market. Если реализация делает nonce первичным proof, connector должен атомарно выделить и сохранить `(api_key_index, nonce)` под тем же transaction lock до signing; простое чтение nonce после вызова не приемлемо.

## 6. Риск, baseline и внешние действия

### NG-RISK-001. Baseline

Только при первом bootstrap оператор задаёт signed `expected_initial_position = B` и явно подтверждает, что фактическая позиция после стабильного snapshot и полного history cut совпадает с ней. Бот не принимает текущую позицию автоматически. Bootstrap cut фиксируется durable, чтобы pre-bootstrap executions не прибавились к `B` второй раз. На restart `B` всегда загружается из ledger и никогда не переснимается. Он может быть long, flat или short. Бот не создаёт для baseline ячейки, TP, flatten, seed или покупку начального запаса.

### NG-RISK-002. Net и reservations

Confirmed venue position должна равняться `P = B + confirmed_buy_fills - confirmed_sell_fills` с учётом всех durable cycles. Для pending/active/ambiguous orders считаются консервативные endpoints:

- `P_max = P + sum(all possibly executable BUY remainder)`;
- `P_min = P - sum(all possibly executable SELL remainder)`.

Полный submitted remainder учитывается, пока terminal и cumulative fill не доказаны. Для текущего профиля `max_abs_net_position = 1000 LIT` — явная положительная конфигурация, не hardcoded максимум и не 1000 USD/USDG. При 55×10 и anchor 5.4: для `B=0` reachable interval `[-330,+220]`, для `B=330` — `[0,+550]`. `gross = sum(abs(unpaired virtual quantity))`; worst case добавляет возможно исполнимые entry remainders, а unknown entry role считается консервативно. Baseline учитывается отдельно. TP меняет net risk, но не создаёт новую virtual gross obligation. `max_gross_position` обязателен и настраивается.

TP obligations имеют приоритет над entries. Чтобы освободить net-risk headroom `P_max` перед BUY TP, router отменяет pending BUY entries; чтобы освободить `P_min` перед SELL TP — pending SELL entries, затем ждёт history terminal. Уже confirmed position отменой освободить нельзя. Если cap всё равно несовместим с обязательством, состояние `RISK_BLOCKED` требует оператора; market close запрещён. Регулярный partial fill не является причиной отменять entry. Emergency cancel entries допустим только при непредвиденном уменьшении venue order cap или hard-risk conflict, а не для освобождения слотов, которые должны быть зарезервированы заранее.

### NG-RISK-005. Admission и TP order slots

На bootstrap полный `order_amount_base` должен удовлетворять minimum base/notional и size step как по entry price, так и по paired TP target каждой ячейки; иначе config отклоняется, а ячейка не вооружается в заведомо постоянный `DUST`. До submit entry ячейка резервирует один entry slot и консервативное число TP child slots. Для её target price вычисляется `min_valid_TP_qty = quantize_up(max(runtime_min_base, runtime_min_notional / target_price), size_step)`, затем `required_slots = 1 + ceil(order_amount_base / min_valid_TP_qty)`. Live partially filled TP child сохраняет свой один physical slot до terminal всего исходного order quantity; UNKNOWN slot также удерживается. Slot ledger разделяет actual orders и ещё не использованные reservations, чтобы один slot не считался дважды.

Если effective `max_active_orders`/venue cap не покрывает все ячейки, движок вооружает только допустимое число, остальные остаются в bounded durable queue. Exit obligations всегда обслуживаются первыми. Eligible queued cells выбираются по расстоянию fixed entry price до текущей цены, затем по стабильному `cell_id`; цена не переносится, resting entry не отменяется для погони за приоритетом. UI показывает armed/queued counts, slots actual/reserved/free и blocker reason. При sample `Q=10`, floor `5`, три slots на ячейку означают, что cap `120` одновременно допускает 40 из 55 ячеек. Пользователь может увеличить cap только в пределах venue limit.

Изменение runtime minimum/step/notional повторно валидирует admission и ограничивает новые entries. Уже принятые exchange orders и их valid remainders сохраняются; новые obligations используют актуальные floors без silent resize. Capacity reservation должна позволять всем одновременно подтверждённым minimum-eligible partial fills вооружённых ячеек получить TP без oversubscription, cancel spam или starvation.

### NG-RISK-003. Ручные ордера и сделки

На startup любой active order на market, не принадлежащий durable ledger, блокирует старт. Бот его не отменяет и не присваивает. Ручная сделка или дрейф position во время работы останавливает новые entries, но не бросает TP obligations. Возобновление требует явного operator baseline audit/rebase, которое не меняет cell obligations и не маскирует неизвестные fills.

### NG-RISK-004. Margin и unknown data

Сравнение available USDG с консервативной оценкой margin — информационное, если оба числа известны, finite и неотрицательны. Неизвестные position, leverage, mode, market state, trading rules, min/max, history completeness, account identity или freshness блокируют новую exposure. Известный numeric shortfall показывает warning и позволяет exchange самой применить margin rules.

## 7. Хранение, crash consistency и restart

### NG-DB-001. SQLite ledger

Состояние хранится в dedicated SQLite database в Hummingbot data path. Минимальная модель включает:

- engine identity/config fingerprint, account index, connector domain, pair, fixed grid, anchor, baseline;
- cells, cycles, legs, exact quantities/prices/sides, obligations, allocations и state transitions;
- orders с client/exchange ID, nonce если известен, requested/executed quantity, submission/cancel state;
- normalized history inbox, dedupe keys, durable cursors/high-water marks;
- transactional outbox для submit/cancel side effects;
- pause/stop reason, timestamps, reconciliation revision и operator audit events.

Все decimal и ID хранятся без float loss. Schema versioned, migration fail-closed. Один single writer владеет DB. Лок account+market не даёт запустить второй движок на том же хосте. Документация прямо говорит, что это не распределённый lock.

### NG-DB-002. Intent before side effect

Любой submit/cancel требует сначала durable transaction: state transition + exact outbox intent + client ID + risk reservation, затем API call, затем durable recording response. Crash в любом промежутке оставляет durable recovery question, но не обещает автоматически различимый API outcome. Unknown не приводит к blind retry с новым ID. Exactly-once гарантируется для ledger application; exchange placement зависит от доказанного ответа/evidence либо документированной idempotence того же CID.

### NG-DB-003. Restart

До новых entries restart обязан:

1. взять single-writer lock и проверить identity/config;
2. загрузить ledger/outbox/inbox/cursors;
3. прочитать account position, active orders и paginated inactive/trades с overlap;
4. восстановить каждую leg только по ID и history;
5. сверить net/gross/reservations и unresolved intents;
6. восстановить TP obligations;
7. остаться paused при любой неоднозначности.

Текущая цена и её движение во время downtime не участвуют в recovery inference.

### NG-DB-004. Потеря persistence и retention gap

Отсутствующая/повреждённая DB при признаках прежнего запуска не создаётся как «чистая» и не принимает текущую position за новый baseline. Движок остаётся fail-closed до восстановления backup либо явной ручной reconciliation с сохранением audit trail. Если требуемая boundary старше retention доступной history, автоматического proof нет: нужен manual reconcile, без reset/rebaseline.

Disk full, fsync/commit failure или read-only persistence переводят engine в degraded/paused до любых новых side effects. Ни submit, ни cancel не отправляются, если их intent/reservation нельзя надёжно записать. Уже известные live orders продолжают conservatively учитываться; UI показывает persistence failure.

### NG-DB-005. Definitive rejection

Локальная pre-send validation rejection с доказательством, что transport не вызывался, освобождает unsent intent. Новый revision допустим после исправления config/rules. После вызова transport только документированный definitive venue rejection с zero fill позволяет terminal и новый revision. Timeout, not-found, исчезновение из active или обычный exception не равны rejection и сохраняют `UNKNOWN`/reservation.

## 8. Pause, bounds и stop

### NG-OPS-001. Pause

Pause запрещает новые entries и новые cycles, но продолжает reconciliation и поддержку уже подтверждённых TP obligations, если это не нарушает hard safety constraint. Fill gap/unknown блокирует новую exposure затронутой ячейки; account-level drift, incomplete global history, unknown order ownership, stale account или hard-cap conflict блокируют все entries.

### NG-OPS-002. Выход за bounds

Если book целиком выходит за configured bounds, новые entries замораживаются, свои active entries cancel-requested, и движок ждёт их history terminal. TP с fixed target сохраняются. Позиция не ликвидируется, bounds не переносятся.

### NG-OPS-003. Stop

Stop переводит engine в draining, запрещает новые entries, cancel-request только своим active orders и сохраняет position/obligations. После доказанного terminal всех своих orders engine завершается как `STOPPED` или `STOPPED_WITH_INVENTORY`. Если отмена или history неопределенны, итог — `STOP_UNCERTAIN`, а не успешный stop. Нет auto-flatten при stop/error/risk violation.

## 9. Архитектурные границы

### NG-ARCH-001. Компоненты

Реализация должна быть native Hummingbot V2 persistent neutral engine:

- `hummingbot/strategy_v2/executors/neutral_grid_executor/` — generic cell state machine, risk, durable ledger/outbox/inbox и executor API;
- `controllers/generic/neutral_grid.py` — generic configuration, fixed grid construction и lifecycle;
- тонкий Robinhood launcher/config adapter, который фиксирует connector/pair/5x/cap defaults и использует существующие encryption/order plumbing;
- узкое расширение `lighter_perpetual_derivative.py` для paginated authoritative history и pre-persistable client ID submission contract.
- локальный API/web adapter в `web/neutral_grid/` и launcher `bin/lighter_robinhood_neutral_grid_web.py`; UI вызывает только command queue единственного engine и читает committed snapshots.

Нельзя создавать 55 `PositionExecutor`, глобально переписывать `GridExecutor`, копировать signer в strategy или обходить Hummingbot order tracker/encrypted credentials.

### NG-ARCH-002. Текущие точки интеграции

- Domain/chain/collateral/endpoints: `hummingbot/connector/derivative/lighter_perpetual/lighter_perpetual_constants.py`.
- Client IDs, `PositionAction -> reduce_only`, signer calls, tx lock: `hummingbot/connector/derivative/lighter_perpetual/lighter_perpetual_derivative.py:296-343`; one-page history limitation: `:729-738`, snapshot path `:797-811`.
- Exact trade-side/client/exchange ID mapping: `lighter_perpetual_api_utils.py:204-220` (`own_trade_details`).
- Numeric CID conversion: connector `:317`; SDK limits `MAX_CLIENT_ORDER_ID_BIT_COUNT=48` и `MAX_ORDER_ID_LEN=19` подтверждены constants `:154`, `:348`. SDK nonce сейчас выделяется внутри async signer call (`signer_client.py:211-243`), поэтому стратегия не может считать его pre-send identity.
- Private WS signals: `lighter_perpetual_user_stream_data_source.py` и `_user_stream_event_listener`.
- Пример fsync/replace durability и conservative exposure: `scripts/lighter_robinhood_grid_risk.py`.
- Пример fail-closed account snapshot/restart checks: `scripts/lighter_robinhood_neutral_grid.py`.
- Новый отдельный launch script: `scripts/lighter_robinhood_fixed_neutral_grid.py`; новый disabled example config. Старые `lighter_robinhood_neutral_grid.py` и MultiGrid entrypoints не менять до отдельной миграции.
- Контракт SDK: [lighter-python v1.1.4](https://github.com/elliottech/lighter-python/tree/v1.1.4). Не придумывать методы вне фактической версии.

### NG-ARCH-003. Конфигурация

Disabled example config и preview используют явную таблицу:

| Поле | Default/example | Проверка |
|---|---:|---|
| `enabled` | `false` | Live start невозможен без смены и explicit confirmation. |
| `lower_price`, `upper_price` | required; sample `5`, `6` | Exact tick multiples, lower < upper, integer-tick formula NG-GRID-001. |
| `cell_count` | `55` | Positive integer; sample создаёт 56 boundaries. |
| `order_amount_base` | `10 LIT` | Positive exact runtime size-step multiple; min/max/notional validation. |
| `leverage` | `5` | Positive и не выше свежего runtime venue maximum. |
| `expected_initial_position` | required signed Decimal | Только первый bootstrap, explicit operator confirmation. |
| `max_abs_net_position` | `1000 LIT` | Positive user limit, не product ceiling. |
| `max_gross_position` | `1000 LIT` | Positive, проверяется вместе с worst-case entries. |
| `max_active_orders` | example `120` | Positive; effective admission не выше venue limit; slot formula может вооружить только 40/55 sample cells, что preview показывает заранее. |
| `history_freshness` | recommended `10s` | Configurable positive; stale blocks exposure. |
| `settlement_delay` / scans | recommended `5s` / `2` full scans | Design safety defaults, не гарантия venue consistency. |
| `history_overlap` | at least `60s` plus all-unresolved lookback | Нельзя завершить scan только потому, что найден ID. |
| `poll_interval` | coalesced, at least `5s` | Дополнительно подчиняется connector throttler/weight budget. |
| entry/TP policy | post-only entry; LIMIT GTT TP | Только разрешённое безопасное подмножество без MARKET. |

Значения 5–6, 10, 5x и caps — профиль, а не универсальные константы. Defaults freshness/settlement/overlap/polling — проверяемые design choices, не заявление о максимальной задержке биржи. 3000 USDG, если отображается, является balance/notional context, не дополнительным hard budget; UI показывает estimated notional (до примерно 550×mark в примере) и margin warning.

Grid dimensions/Q нельзя менять у работающего grid. Новый grid разрешён после закрытия всех owned orders/obligations либо явной audited migration, сохраняющей старые cycles; reset DB запрещён. Credentials/URLs с секретами в config отсутствуют. Existing encrypted keystore и валидаторы setup переиспользуются: API private key — 80 hex (optional `0x`), не wallet key 64 hex.

## 10. Локальный web UI и operator status

### NG-UI-001. Backend boundary и безопасность

Обязателен responsive Russian UI для desktop/mobile, по умолчанию bind только `127.0.0.1`. Browser не имеет exchange credentials, не вызывает биржу и не пишет state напрямую. API возвращает versioned committed snapshot с `config_revision`, `engine_revision`, snapshot timestamp и freshness. Каждая state-changing команда `start/pause/resume/stop` содержит unique idempotency key и ожидаемые revisions, затем идёт через durable command queue единственного engine. Refresh/retry возвращает прежний command result, не повторяет действие. Команда со stale preview/revision получает conflict и свежий preview; она не применяется автоматически. UI не выводит cell state из текущей цены.

Профиль credentials выбирается из existing Hummingbot encrypted keystore. Unlock выполняется локальным backend без password/API key в argv, logs, browser localStorage или API response; UI показывает только masked presence и не поддерживает secret readback. Command endpoints имеют session, CSRF и Origin protection. Public bind запрещён по умолчанию; будущий server deployment требует documented authentication + TLS или SSH tunnel. Нельзя тянуть runtime assets с внешнего CDN. Тяжёлый новый dashboard stack не обязателен; deprecated Hummingbot Dashboard не является зависимостью.

### NG-UI-002. Экраны и честные состояния

До start UI показывает config preview: динамические `N+1` boundaries/`N` cells (для sample — 56/55), initial BUY/SELL и armed/queued counts, actual/reserved/free order slots, signed baseline, reachable `P_min/P_max`, gross/net caps, leverage, notional/margin advisory, runtime floors и validation errors. Первый live start требует отдельного явного подтверждения `expected_initial_position` и риска; это runtime UX requirement, а не разрешение Claude на live запуск.

Во время работы UI показывает `BOOTSTRAPPING`, `RECONCILING`, `NORMAL`, `DEGRADED`, `PAUSED`, `RISK_BLOCKED`, `STOPPING`, `STOPPED_WITH_INVENTORY`, `STOP_UNCERTAIN` и причины без optimistic mapping «процесс жив = engine NORMAL» или fake STOP. Browser close/restart не останавливает engine. Snapshot age/staleness видимы.

Grid/table отображают cell/price levels, initial side, generation, entry и TP requested/filled/remaining, concurrent live legs, client/exchange IDs, unmatched evidence, dust, blockers. Сводка показывает baseline, authoritative net, `P_min/P_max`, gross/caps, reservations, owned/unknown orders, history lag/cursor/completeness, margin advisory и последние ошибки/commands. Доступен drill-down по order/trade IDs и audit history без секретов.

### NG-UI-003. Command semantics

`Pause` прекращает entries, сохраняя TP/reconciliation; `Resume` проходит freshness/risk/reconciliation gate; `Stop` следует NG-OPS-003 и может честно завершиться `STOP_UNCERTAIN`. Duplicate/concurrent Start для одной identity возвращает существующую command/engine identity или conflict и никогда не запускает второй engine. Все ответы отражают committed command state, а не предполагаемый exchange outcome.

## 11. Operator status

CLI status сохраняется и соответствует тем же committed snapshots, что UI. Status должен показывать engine state/reason, baseline, authoritative net, `P_min/P_max`, gross obligations/cap, freshness/history lag/cursor progress, owned active/unknown orders, unmatched evidence, margin warning, а для каждой не-idle ячейки: prices, direction, entry/TP requested/filled/remaining, client/exchange IDs, state, dust, blocker. Секреты и auth token не выводятся.

## 12. Приёмочные сценарии

Все сценарии выполняются офлайн на deterministic fake exchange; живые ключи, authenticated calls и ордера запрещены.

| ID | Сценарий | Критерий |
|---|---|---|
| AC-01 | Normal BUY cycle | BUY стоит на low, history fill создаёт SELL LIMIT GTT на high; после полного TP следующий cycle снова BUY. |
| AC-02 | Normal SELL cycle | SELL стоит на high, history fill создаёт BUY LIMIT GTT на low; после полного TP следующий cycle снова SELL. |
| AC-03 | Grid arithmetic | Integer-tick formula даёт `N+1` fixed prices и 22/33 sides в sample; некратные bounds/quantity step и full Q ниже minimum на любой entry/TP цене отклоняются, price bounce ничего не меняет. |
| AC-04 | 55 cells | При `N=55` нет 55 executor tasks/потоков и polling storm; один engine пакетирует работу. |
| AC-05 | Partial entry 2+3+5, floor 5 | Entry остаётся live: fill 2 хранится как undispatched below-min obligation, после +3 выставляется TP 5, после +5 total TP obligation 10; allocation не теряется. |
| AC-06 | Partial TP | Частичное TP не unlock/rearm; remaining сохраняет exact target. |
| AC-07 | Terminal partial entry | Canceled entry с fill закрывает фактический quantity; новый cycle имеет полный configured Q. |
| AC-08 | Late fill after cancel | Cancel event не unlock; late history fill увеличивает obligation и покрывается TP. |
| AC-09 | WS duplicate/reorder | Duplicate/reordered events не удваивают fills и не дают terminal proof. |
| AC-10 | More than 100 inactive orders | Искомый ID на поздней cursor page найден; первая page не признаётся полной. |
| AC-11 | More than 100 trades | All pages до watermark прочитаны, duplicate boundary deduped, cumulative exact. |
| AC-12 | Bad pagination | Repeated/malformed cursor, conflicting duplicate или missing required page переводят history в incomplete; entries blocked. |
| AC-13 | History lag | WS fill будит poller, но TP ждёт history; lag виден в status. |
| AC-14 | TP dispatch SLO | При history commit и здоровых capacity/throttler/transport durable TP intent достигает router за ≤2 s; blocker показывает queue age/reason, exchange acceptance не обещается. |
| AC-15 | Crash before intent commit | API не вызван; restart не видит phantom order. |
| AC-16 | Crash after intent, before API | Restart видит outbox ID и не выдаёт новый ID; без venue idempotence ambiguity не выдаётся за доказанное отсутствие. |
| AC-17 | Crash during/after API timeout | Active/inactive/trades по saved ID разрешают outcome; до proof reservation и `UNKNOWN` сохраняются. |
| AC-18 | Crash after API, before response commit | Exact saved ID сопоставляет evidence; тест доказывает exactly-once ledger effect, не недоказуемый exactly-once API outcome. |
| AC-19 | Crash before cursor commit | Inbox/ledger/cursor transaction rollback/replay идемпотентен; fill не теряется и не удваивается. |
| AC-20 | Restart after price bounce | Cells/orders/TP восстанавливаются по DB+history, а не по current price. |
| AC-21 | Cancel timeout ambiguity | Order и full remainder остаются reserved, ячейка locked, duplicate exposure не создаётся. |
| AC-22 | String ID precision | Trade/exchange ID выше safe JS/float range проходит DB, API/UI pagination и matching как string; отдельно client ID проверяется в 48-bit bound. |
| AC-23 | Rounding and fees | Quantity не округляется вверх; quote/base fees не меняют perpetual TP obligation. |
| AC-24 | Net long cap | Pending BUY отклоняется, если `P_max > +1000`; other safe work не обходит cap. |
| AC-25 | Net short cap | Pending SELL отклоняется, если `P_min < -1000`; shorts в пределах cap работают. |
| AC-26 | Gross cap with net zero | Offset long/short cells не обходят gross obligation cap при venue net 0. |
| AC-27 | Virtual TP crosses zero | SELL TP virtual long при venue net 0 создаёт net short; non-reduce-only семантика и ledger остаются корректными. |
| AC-28 | Initial manual baseline | Positive/zero/negative B принимается в cap; нет seed/TP/flatten для B. |
| AC-29 | Manual order at startup | Unknown active order блокирует старт и не отменяется ботом. |
| AC-30 | Manual trade while running | Position drift freezes entries, TP obligations/reconciliation continue, resume требует baseline audit. |
| AC-31 | Self-trade conflict | Каждый submit проверен против live/pending/unknown; conflicting entry cancel-requested, TP ждёт history terminal и не self-matches. |
| AC-32 | Outside bounds | Entries freeze/cancel with terminal proof; fixed TP и obligations сохраняются; no recenter/flatten. |
| AC-33 | Dust | Below-min remainder виден, durable, блокирует reset; aggregate только по same side+target. |
| AC-34 | Runtime minimum changes | Новый floor блокирует невалидный submit без rounding/price shift; невыполнимый TP становится visible blocker. |
| AC-35 | Stop success with inventory | Свои orders terminal, position не flatten; outcome `STOPPED_WITH_INVENTORY`, obligations сохранены. |
| AC-36 | Stop cancellation failure | Unknown cancel даёт `STOP_UNCERTAIN`, не false success; restart продолжает reconciliation. |
| AC-37 | Known low margin | Finite shortfall даёт warning; unknown/malformed margin blocks new exposure. |
| AC-38 | No market fallback | Post-only rejection, dust, stale data или TP blocker не создают MARKET order. |
| AC-39 | Same-price aggregation | Один aggregate TP точно распределяет partial fills между cell obligations идемпотентно. |
| AC-40 | History conflict | Trade cumulative > order cumulative, duplicate ID с разным size или unknown owned fill останавливают exposure; quantity не угадывается. |
| AC-41 | Full overlap | Duplicate/unresolved ID на странице не останавливает scan; читается полная verified boundary обоих endpoint. |
| AC-42 | Settlement and late evidence | Cell release ждёт terminal+cumulative equality+delay+repeat scan; сверхзадержанный fill назначается старому cycle и freezes market. |
| AC-43 | Client ID allocation | Durable unique 48-bit CID maps to full leg identity; collision/exhaustion fail closed, no truncate/new-ID retry. |
| AC-44 | TP capacity | При заполненном `max_active_orders` TP priority отменяет cap-consuming entry; TP–TP conflict идёт FIFO без internal netting. |
| AC-45 | Stable bootstrap cut | Signed expected B подтверждается один раз; pre-cut fills не double count, restart не recaptures baseline. |
| AC-46 | UI preview | Responsive Russian UI показывает grid/counts/baseline/range/caps/floors/advisory и validation до start. |
| AC-47 | UI commands | Idempotent start/pause/resume/stop queue; refresh и concurrent Start не создают duplicate engine/order. |
| AC-48 | UI truthful state | Stale snapshot, DEGRADED, PAUSED и STOP_UNCERTAIN отображаются без optimistic NORMAL/STOPPED. |
| AC-49 | UI security | Default loopback, CSRF/Origin/session tests; secrets не возвращаются, не логируются и не попадают в localStorage/argv. |
| AC-50 | UI browser flow | Offline fake exchange: partial entry+TP, dust, history lag, pause/resume/stop; keyboard access, contrast и desktop/mobile table проверены. |
| AC-51 | GTT renewal | Expiry обновляется только после terminal+complete history; точный остаток сохраняется, duplicate TP невозможен. |
| AC-52 | Config mutation | Running grid dimensions/Q отклоняются; audited migration сохраняет старые cycles, DB не reset. |
| AC-53 | History retention gap | Нужная overlap boundary старше доступной history: start/reuse заблокирован, требуется audited manual reconciliation без reset/rebaseline. |
| AC-54 | Missing/corrupt database | Признаки прежнего engine с missing/corrupt DB не превращаются в fresh bootstrap и не принимают текущую position за новый B. |
| AC-55 | Disk full/commit failure | Ни submit, ни cancel не уходят без committed intent; state честно показывает persistence failure, неизвестные live orders остаются reserved. |
| AC-56 | Zero-fill rejection | Pre-send validation release допустим только при no-transport proof; venue reject — только при documented definitive zero-fill. Timeout/not-found остаются UNKNOWN и не создают новый CID/revision. |
| AC-57 | Simultaneous partial-fill capacity | Многие armed cells одновременно получают minimum-eligible partial fills: все TP dispatch используют заранее reserved slots без oversubscription, регулярной отмены entries, cancel spam или starvation; queued cells продвигаются детерминированно. |

## 13. Тестирование и приёмка

Нужны:

1. pure unit tests cell FSM, risk endpoints, exact Decimal/ID, aggregation и SQLite transitions;
2. deterministic fake exchange с управляемыми WS/history lag, pagination, duplicate/reorder, timeout и crash injection;
3. property tests инвариантов: `P_min <= actual outcome <= P_max`, cap никогда не нарушен любым fill/cancel/restart ordering, confirmed quantities не убывают, cell не reset с obligation/unknown/dust, ledger effect идемпотентен; тест не заявляет exactly-once exchange placement при недокументированной venue idempotence;
4. connector tests cursor pagination, later-page ID, >100 rows, boundary duplicates, malformed/repeated cursor и exact field mapping;
5. controller/executor integration tests всех AC и current Lighter regression;
6. web API/browser tests command idempotency, single-engine ownership, auth/CSRF/Origin/default loopback, no secret readback, committed snapshot semantics и доступного responsive UI на fake exchange.

Минимальная приёмочная последовательность:

- targeted new executor/controller/connector tests;
- existing `test/hummingbot/connector/derivative/lighter_perpetual/test_lighter_perpetual_derivative.py`;
- committed baseline neutral/risk и relevant controller/executor regressions в clean implementation worktree; два dirty test files старого checkout исключаются из gate и только сохраняются без изменений;
- compile/import check и lint всех changed Python files;
- `git diff --check`.

Codex принимает изменения по фактическому diff, свежим логам этих проверок и traceability AC → tests, а не по заявлению исполнителя. Ни одна проверка не использует live credentials, authenticated Robinhood API или реальные ордера.
