"""Schema v5: one durable owner for every known venue exchange order id.

The partial unique index permits many orders whose exchange id is still unknown (NULL), while preventing one
venue order from being bound to multiple CIDs. Creating the index deliberately fails when a legacy database
already contains duplicates: ownership cannot be repaired or reassigned automatically.
"""

VERSION = 5
NAME = "unique_exchange_order_id"

SQL = r"""
DROP INDEX orders_by_exchange_id;
CREATE UNIQUE INDEX orders_by_exchange_id
ON orders(exchange_order_id)
WHERE exchange_order_id IS NOT NULL;
"""
