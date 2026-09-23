# Lighter Robinhood Multi Grid Strike на macOS

Эта сборка работает с Robinhood Lighter mainnet, доменом
`lighter_perpetual_robinhood` и рынком `LIT-USDG`. Текущая конфигурация — LONG
Multi Grid Strike. Подготовка файлов сама по себе не запускает торговлю.

## Запуск двойным щелчком

Откройте **«Запустить LIT бота.command»** в корне проекта. Перед запуском помощник
проверяет оба YAML-файла и показывает итоговые параметры. Затем штатный Hummingbot
просит только скрытый пароль локального хранилища:

```text
Keystore password:
```

Terminal не показывает при вводе даже точки — это нормально. Account Index, API
Key Index и API Private Key повторно вводить не нужно: они уже находятся в
зашифрованном хранилище Hummingbot. Пароль и ключи не передаются в аргументах
процесса и не записываются в открытые YAML-файлы.

Помощник запускает именно `conf/scripts/lighter_robinhood_multi_grid_strike.yml`
как V2 script. Этот файл подключает безопасный runner и контроллер
`conf/controllers/lighter_robinhood_multi_grid_strike.yml`. Запуск контроллера
напрямую с `--controller` запрещён: штатный автоматически созданный runner имеет
другое поведение при остановке.

После запуска бот работает отдельно от окна Terminal. Закрытие окна его не
останавливает. Для проверки и остановки используйте **«Статус LIT бота.command»**
и **«Остановить LIT бота.command»**. Одновременно допускается только один бот из
этого checkout; помощник не применяет автоматический `--replace`.

`status` может показывать старую запись об ошибке от предыдущего нейтрального
бота. Смотрите время события и имя текущего script config: историческая ошибка не
означает, что новый Multi Grid Strike сейчас работает или упал.

## Текущие торговые параметры

- LONG/BUY `LIT-USDG`, Robinhood Lighter, ONEWAY, плечо 5×.
- Общий номинальный бюджет сеток: 3000 USDG; лимит абсолютной позиции: 1000 LIT.
- Диапазон `lit-main`: 5…6, нижний предел 4.9999, доля общего бюджета 100%.
- Минимальный шаг между уровнями: 1.8%; минимальный размер уровня: 250 USDG.
- Получается примерно 11 уровней и около 273 USDG на уровень. Реальное число и
  размер уточняет нативный executor по правилам рынка и текущей цене.
- `max_open_orders: 2` ограничивает открытые ордера входа **для каждого
  диапазона отдельно**. При нескольких диапазонах их общий максимум
  складывается. Ордер take-profit для уже исполненного входа учитывается
  дополнительно.
- Размер пакета и частота также применяются к каждому диапазону отдельно. В
  начальной конфигурации за цикл создаётся не более одного входа на диапазон,
  интервал — 3 секунды; `activation_bounds` отключён.
- Вход и take-profit используют `LIMIT_MAKER`; take-profit — 1.8%. Stop-loss,
  time-limit и trailing-stop не заданы.

Это начальные значения, а не жёстко зашитый размер сетки. Можно менять общий
бюджет, шаг, минимальный размер уровня, число открытых входов, размер пакета,
частоту, `activation_bounds` и take-profit, пока итоговая конфигурация проходит
валидацию и худшая возможная LONG-позиция остаётся в пределах 1000 LIT. Помощник
показывает значения из фактического локального YAML перед каждым запуском.

Перед созданием нового поколения сетки runner требует свежие данные выбранного
аккаунта, отсутствие неизвестных активных ордеров и отсутствие ранее удерживаемой
или неразобранной позиции. Это проверка именно Multi Grid Strike; старый отчёт
`LIVE READY` нейтральной стратегии для этого запуска не используется.

Лимит 1000 LIT рассчитан для первоначально пустой позиции и исключительного
управления этой стратегией. Ручные сделки или другой бот на том же аккаунте во
время работы могут нарушить этот предел; одновременно торговать ими нельзя.

Ручная остановка и срабатывание нижнего предела сохраняют уже набранную позицию,
снимая заявки. Нативный выход выше диапазона (`TAKE_PROFIT`) может закрыть
позицию. После остановки всегда проверьте статус аккаунта: сохранённая позиция не
исчезает вместе с процессом. При следующем запуске любая сохранённая ненулевая
позиция блокирует создание свежей сетки, пока пользователь вручную не сверит и не
урегулирует состояние аккаунта.

Чтобы позже добавить LONG-диапазоны, добавьте элементы в `grids`. Их
`amount_quote_pct` делят те же 3000 USDG; новый диапазон не увеличивает общий
бюджет. Сумма долей включённых диапазонов должна быть не больше 1. Каждый
диапазон обязан оставаться BUY и проходить проверку лимита 1000 LIT.

## Конфигурационные файлы

Отслеживаемые примеры:

```text
conf/controllers/lighter_robinhood_multi_grid_strike.yml.example
conf/scripts/lighter_robinhood_multi_grid_strike.yml.example
```

Локальные рабочие копии имеют те же имена без `.example`, игнорируются Git и на
этом Mac уже созданы с правами `0600`. При восстановлении не перезаписывайте
существующие файлы:

```bash
test -e conf/controllers/lighter_robinhood_multi_grid_strike.yml || \
  cp conf/controllers/lighter_robinhood_multi_grid_strike.yml.example \
     conf/controllers/lighter_robinhood_multi_grid_strike.yml
test -e conf/scripts/lighter_robinhood_multi_grid_strike.yml || \
  cp conf/scripts/lighter_robinhood_multi_grid_strike.yml.example \
     conf/scripts/lighter_robinhood_multi_grid_strike.yml
chmod 600 conf/controllers/lighter_robinhood_multi_grid_strike.yml \
  conf/scripts/lighter_robinhood_multi_grid_strike.yml
```

## Проверенная среда

На этом Mac используется отдельная arm64-среда Python 3.12.14 с
`lighter-sdk==1.1.4`:

```bash
cd "/Users/mikhail/Documents/AI workshop/hummingbot-robinhood"
export HB_RUNTIME_ROOT="$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d"
export MAMBA_ROOT_PREFIX="$HB_RUNTIME_ROOT/mamba-root"
"$HB_RUNTIME_ROOT/tools/bin/micromamba" run -p "$HB_RUNTIME_ROOT/env" hbot --help
```

Повторная установка не меняет системный Python или shell-профиль:

```bash
cd "/Users/mikhail/Documents/AI workshop/hummingbot-robinhood"
export HB_RUNTIME_ROOT="$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d"
mkdir -p "$HB_RUNTIME_ROOT/tools"
curl -Ls https://micro.mamba.pm/api/micromamba/osx-arm64/latest \
  | tar -xj -C "$HB_RUNTIME_ROOT/tools" bin/micromamba
export MAMBA_ROOT_PREFIX="$HB_RUNTIME_ROOT/mamba-root"
"$HB_RUNTIME_ROOT/tools/bin/micromamba" create -y -p "$HB_RUNTIME_ROOT/env" \
  -f setup/environment.yml python=3.12
"$HB_RUNTIME_ROOT/env/bin/python" -m pip install \
  'numpy>=2.2.6,<2.3' 'numba==0.61.2' 'cryptography>=48.0.1,<49'
"$HB_RUNTIME_ROOT/env/bin/python" -m pip install --no-deps -r setup/pip_packages.txt
"$HB_RUNTIME_ROOT/env/bin/python" -m pip check
"$HB_RUNTIME_ROOT/env/bin/python" setup.py build_ext --inplace
ln -sfn "$PWD/bin/hbot" "$HB_RUNTIME_ROOT/env/bin/hbot"
```

`setup/pip_packages.txt` включает macOS-зависимость `appnope`, необходимую для
штатной остановки. Для Linux-сервера создайте отдельную среду и повторите сборку;
автозапуск сервисом здесь не настраивается.

## Ключи

Ключ должен принадлежать Robinhood-развёртыванию Lighter; ключи Core с
`app.lighter.xyz` не взаимозаменяемы. Общая последовательность описана в
[руководстве Hummingbot](https://hummingbot.org/exchanges/lighter/), официальный
Robinhood-интерфейс — [robinhoodchain.lighter.xyz](https://robinhoodchain.lighter.xyz/).
Не используйте сторонние генераторы ключей.

Для первичного сохранения ключа используйте штатное шифрование Hummingbot:

```bash
"$HB_RUNTIME_ROOT/tools/bin/micromamba" run -p "$HB_RUNTIME_ROOT/env" \
  hbot connect lighter_perpetual_robinhood
```

Не передавайте API Private Key в argv и не записывайте его в YAML. Повторные
запуски используют сохранённые зашифрованные данные и просят только пароль
хранилища.

Robinhood rewards — отдельная программа. Для этой интеграции не подтверждены ни
участие/атрибуция API-сделок в программе, ни множитель 2×. Совпадение аккаунта или
адреса кошелька само по себе не доказывает Wallet-атрибуцию. См.
[Lighter on Robinhood Chain Points](https://docs.lighter.xyz/points-program/lighter-on-robinhood-chain-points)
и [Robinhood Lighter Domains](https://docs.robinhood.com/chain/lighter-domains/).
