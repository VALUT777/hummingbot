# GUI references: neutral-grid

Дата проверки: 24 сентября 2026 года.

## Что берём в продукт

Для графика в браузере используется локально поставляемая **TradingView Lightweight Charts 5.2.1**. Это не CDN и не build-зависимость: production standalone-файл лежит в `web/neutral_grid/static/vendor/lightweight-charts-5.2.1/`.

Проверенный API v5 для экрана сетки:

```js
const chart = LightweightCharts.createChart(container, options);
const price = chart.addSeries(LightweightCharts.LineSeries, seriesOptions);
price.createPriceLine({ price: level, title: 'L12' });
chart.timeScale().fitContent();
```

Он покрывает цену и горизонтальные границы ячеек (для 24 ячеек — 25 границ). Свечи добавляются через `chart.addSeries(LightweightCharts.CandlestickSeries)`. См. [официальную документацию Lightweight Charts](https://tradingview.github.io/lightweight-charts/) и [v5 API](https://tradingview.github.io/lightweight-charts/docs/api).

### Provenance и лицензия

| Поле | Значение |
| --- | --- |
| Версия | `5.2.1` |
| Официальный registry | `https://registry.npmjs.org/lightweight-charts` |
| Tarball | `https://registry.npmjs.org/lightweight-charts/-/lightweight-charts-5.2.1.tgz` |
| NPM SHA-1 | `5a27ead719573607d4d3483ad2728c055ccea8fd` |
| SHA-256 tarball | `64310292298df4dd527494865e5d91d447f8bb397b68de3912c2a6465025ee5e` |
| SHA-256 production JS | `e21cc5caa0226ef30bd8549c50b9ef926615f2a4ee6b4e486353477a55f598cf` |
| Лицензия | Apache-2.0; копия: `LICENSE` |
| Copyright notice | копия официального `NOTICE` тега `v5.2.1` |

Репозиторий требует указывать TradingView как создателя и размещать текст из `NOTICE` вместе со ссылкой на `https://www.tradingview.com/` на доступной пользователю странице. В UI следует включить `attributionLogo` либо обеспечить эквивалентное видимое уведомление и ссылку. Источник: [LICENSE/README проекта](https://github.com/tradingview/lightweight-charts).

## Визуальные ориентиры, не код для копирования

**FreqUI / Freqtrade** — ориентир для плотного операционного dashboard: статус, trade view, график и разделённые представления. Его официальная страница содержит скриншоты login, trade view, dashboard и settings, а также описывает start/stop в trade view. [FreqUI docs](https://docs.freqtrade.io/en/stable/freq-ui/). Код Freqtrade/FreqUI лицензирован GPL-3.0, поэтому это UX-вдохновение, а не источник кода для включения.

**Hummingbot Dashboard** — не использовать как свежий implementation-reference: Hummingbot прямо помечает Streamlit Dashboard как «Not Actively Maintained» и рекомендует Condor. [Официальная страница Dashboard](https://hummingbot.org/dashboard/). Condor полезен лишь как ориентир «сначала статус и наблюдаемость»: его текущий web dashboard охватывает PNL, состояние ботов и live trade panel, но его мультиагентская, multi-venue и key-management архитектура избыточна для одного фиксированного grid-бота. [Condor](https://hummingbot.org/condor/), [репозиторий](https://github.com/hummingbot/condor).
