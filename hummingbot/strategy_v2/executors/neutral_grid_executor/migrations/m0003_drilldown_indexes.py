"""Schema v3: indexes for exact-id drill-down queries of the local UI (AC-22, NG-UI-002).

``orders.exchange_order_id`` (``orders_by_exchange_id``) and every ``id`` primary key are already indexed; this adds
the trade id of fills and the venue order index. Frozen once released; add ``m0004_*`` for further changes.
"""

VERSION = 3
NAME = "drilldown_indexes"

SQL = r"""
CREATE INDEX fills_by_trade_id ON fills(trade_id_str);
CREATE INDEX orders_by_order_index ON orders(order_index);
"""
