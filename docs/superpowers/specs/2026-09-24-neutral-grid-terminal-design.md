# Neutral Grid Terminal Backend Design

## Goal

Provide one read-only API for the combined operator terminal: real Robinhood LIT/USDG candles, committed grid and position state, committed nonfinal owned orders, and history-confirmed fills. Existing command, revision, recovery, authentication, redaction, and CSP behavior remains unchanged.

## Contract

`GET /api/terminal?interval=5m&candle_limit=200&fill_limit=100`

The endpoint accepts intervals `1m`, `5m`, `15m`, `30m`, `1h`, and `4h`; candle limits 20–500; and fill limits 1–200. Invalid parameters return the existing JSON 400 error shape.
The browser may also pass the positive `snapshot_version` string from its immediately preceding `/api/state`
read. The endpoint then selects that exact retained snapshot; a missing or pruned version returns
`409 snapshot_unavailable` and never falls forward to a newer snapshot.

The response contains `served_at`, `snapshot`, `market`, `grid`, `position`, `orders`, and `fills`. Prices, quantities, identifiers, market IDs, snapshot versions, and cursors are strings. Unix timestamps and durations are JSON numbers.

`snapshot`, `grid`, `position`, and `orders` derive from one latest committed snapshot. `fills` contains only append-only history fills whose `applied_at_ms` is strictly earlier than that snapshot's `committed_at_ms`; this conservative boundary can delay a same-millisecond fill by one snapshot. Both tables name the snapshot version they are consistent with. Public candles carry their own fetch timestamp and are never presented as revision-consistent.

## Market data

Only the committed identity `lighter_perpetual_robinhood` plus `LIT-USDG` maps to the server-owned allowlist entry `https://api.rh.lighter.xyz/api/v1/candles`, market ID `5`. Browser parameters cannot select a URL, host, pair, connector, or market ID. The request is unauthenticated, bounded to 500 rows, times out, and is cached for ten seconds with single-flight request coalescing.

The offline demo makes no network requests and returns deterministic candles labeled `demo_fixture`. An unsupported identity or upstream failure returns HTTP 200 with `market.source="unavailable"`, an empty candle list, and a redacted `unavailable_reason`; ledger data remains usable.

## Ledger data

Orders are the snapshot's entry and TP legs except terminal and rejected states. Unknown and cancel-pending states remain visible because they are operationally important. The source label is `committed_snapshot_nonfinal_legs`.

Fills come from the append-only `fills` table in newest-confirmed ingestion order (`rowid DESC`), with a strict `applied_at_ms` cutoff at the selected snapshot commit. The displayed `trade_at` remains the venue event time. The query reads at most `fill_limit + 1` rows to report truncation and does not require a schema migration.

## Failure behavior

No snapshot produces empty grid/order/fill collections. Attach mode does not fetch public data until a committed, allowlisted identity exists. Candle failure never changes engine state or command availability. All output continues through the existing JSON-safe redaction boundary.

## Verification

Tests cover parameter rejection, no-snapshot behavior, demo no-network output, nonfinal order filtering, fill cutoff and bounds, exact strings/IDs, public-cache single flight, and visible candle failure. Existing state, command, security, attach, and preview tests remain the regression gate.
