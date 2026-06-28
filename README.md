# Kafka event platform

Built a Kafka-based payment processing platform implementing outbox pattern, saga orchestration, and Redis idempotency to guarantee exactly-once order fulfillment across Payment, Inventory, and Notification services.
Engineered DLQ + exponential backoff retry across all consumers, with Prometheus metrics tracking consumer lag, retry rates, and payment success/failure per topic.