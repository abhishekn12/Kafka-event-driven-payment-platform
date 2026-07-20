# Saga Design — Order Fulfillment

## Why choreography, not orchestration

No new orchestrator service. Each service reacts to the previous step's outcome by consuming its event and publishing its own — same choreographed-saga style already used for `order.created` fan-out, just chained instead of parallel.

## Flow

```
POST /orders
     |
     v
[order-service]  Order(PENDING) + OutboxEvent written in one txn
     |
     v  order.created
[payment-service]  stub charge
     |                         \
     v payment.processed        v payment.failed
[inventory-service]        [order-service]
  stub stock check          Order -> CANCELLED
     |            \          + order.cancelled event
     v inv.updated  v inv.failed
[order-service]  [order-service]
Order->CONFIRMED   Order->CANCELLED
                    + order.cancelled event
```

Inventory is triggered by `payment.processed`, **not** `order.created` — this is a deliberate change from the current parallel fan-out. Payment must succeed before we commit to shipping/deducting stock; the old parallel model could deduct stock for an order whose payment later failed.

`notification-service` keeps its existing `order.created` listener ("order received") and gains listeners on the three terminal events to send a stub confirmation/failure notification.

## Events

All new topics follow the existing one-topic-per-event-type convention (see `order.created` / `order.cancelled`), 3 partitions, replication 1.

| Topic | Producer | Consumers | Payload |
|---|---|---|---|
| `payment.processed` | payment-service | inventory-service, order-service, notification-service | orderId, customerId, productId, quantity, amount, paymentId, occurredAt |
| `payment.failed` | payment-service | order-service, notification-service | orderId, customerId, amount, paymentId, reason, occurredAt |
| `inventory.updated` | inventory-service | order-service, notification-service | orderId, customerId, productId, quantity, occurredAt |
| `inventory.failed` | inventory-service | order-service, notification-service | orderId, customerId, productId, quantity, reason, occurredAt |
| `order.cancelled` | order-service | *(none yet)* | orderId, reason, occurredAt |

`order.cancelled`'s topic bean already existed (unused) from day one — it's finally wired up as the compensation event. It has no consumers today: notification-service gets its failure notification directly from `payment.failed`/`inventory.failed` instead (more specific — it carries the actual decline reason), so also listening to `order.cancelled` would just be a redundant duplicate notification. It's published for audit purposes and as the natural extension point for a future consumer — e.g. a payment-refund flow reacting to it after an inventory-failure compensation (see below).

## Compensation

Two failure points, both handled by order-service transitioning `Order.status` to `CANCELLED` and emitting `order.cancelled`:

- **`payment.failed`**: inventory was never reached (it only starts after payment succeeds), so there's nothing to unwind there. Compensation is just: cancel the order.
- **`inventory.failed`**: payment already succeeded. Compensation here is: cancel the order and emit `order.cancelled`. A real system would also need to refund the captured payment at this point — that's a `payment-service` concern (a fourth service reacting to `order.cancelled` to issue a refund) and isn't implemented here, since no refund flow/topic was in scope. Flagging it explicitly rather than pretending it's handled.

### Why no `InventoryRolledBack` event

The original plan named `InventoryRolledBack` as a compensation event. In a linear saga where inventory only runs *after* payment succeeds, there's no scenario where inventory itself needs to undo a deduction it already made — nothing runs after inventory that could fail and require unwinding it. That event would only make sense under the *old* parallel model (inventory and payment racing independently, inventory having to roll back if payment later failed) — which this design deliberately replaces. Implementing an event with no real trigger path would be dead code, so it's omitted; `order.cancelled` covers both failure points instead.

### Idempotency / ordering safety

`order-service`'s new consumers guard every status transition: they only apply `PENDING -> CONFIRMED` or `PENDING -> CANCELLED`; if the order is already in a terminal state, the event is logged and skipped. This makes the transition naturally idempotent without needing Redis-based dedup (unlike payment/inventory, a repeated status write has no side effect beyond the write itself) and protects against any out-of-order redelivery during retries.

## Failure simulation (stub-only, deterministic)

Real payment/inventory checks don't exist yet, so failure paths need a deterministic trigger for testing — not randomness, which would make the compensation tests flaky:

- **payment-service**: declines (stub) if `amount > 5000` — framed as a simulated fraud/limit check.
- **inventory-service**: declines (stub) if `quantity > 50` — framed as simulated insufficient stock.

## DLQ / retry

Same pattern as the existing three consumers (Day 5): `ExponentialBackOffWithMaxRetries(4)` (1s/2s/4s/8s) + per-service DLQ topic, extended to order-service's new listeners since a lost compensation event is arguably worse than a lost happy-path one (an order stuck in `PENDING` forever after a payment was declined or a refund never gets triggered).

## Metrics

Micrometer counters added via `DefaultErrorHandler.setRetryListeners(...)`:
- `kafka.consumer.retry` — incremented on every retry attempt, tagged by service/topic
- `kafka.consumer.dlq` — incremented when a record is finally recovered to the DLQ
- `payment.processed` / `payment.failed`, `inventory.updated` / `inventory.failed` — explicit success/failure counters in each consumer

Consumer lag is already exposed automatically via spring-kafka's Micrometer binder — verified present in `/actuator/prometheus` output, not custom-added.

No Grafana dashboard — the plan explicitly allows "wire up to a basic Grafana dashboard **or just verify metrics endpoint**"; verifying the Prometheus endpoint satisfies that or-clause without adding a UI layer with unclear scope.
