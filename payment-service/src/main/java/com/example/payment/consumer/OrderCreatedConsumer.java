package com.example.payment.consumer;

import com.example.payment.dto.OrderCreatedEvent;
import com.example.payment.dto.PaymentFailedEvent;
import com.example.payment.dto.PaymentProcessedEvent;
import com.example.payment.entity.Payment;
import com.example.payment.entity.PaymentStatus;
import com.example.payment.repository.PaymentRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedConsumer {

    private static final String IDEMPOTENCY_KEY_PREFIX = "payment:processed-order:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    // Stub decline threshold — deterministic so failure-path tests aren't flaky.
    // Framed as a simulated fraud/limit check; real gateway integration comes later.
    private static final BigDecimal DECLINE_ABOVE_AMOUNT = new BigDecimal("5000");

    private final PaymentRepository paymentRepository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MeterRegistry meterRegistry;

    private Counter paymentProcessedCounter;
    private Counter paymentFailedCounter;

    @PostConstruct
    void initMetrics() {
        paymentProcessedCounter = Counter.builder("payment.processed")
                .description("Payments successfully processed")
                .register(meterRegistry);
        paymentFailedCounter = Counter.builder("payment.failed")
                .description("Payments declined")
                .register(meterRegistry);
    }

    @KafkaListener(topics = "order.created")
    public void onOrderCreated(String payload) throws Exception {
        OrderCreatedEvent event = objectMapper.readValue(payload, OrderCreatedEvent.class);

        // Correlation ID for every log line this record touches — this consumer's,
        // plus any framework logging (e.g. Hibernate SQL) — for the rest of this
        // listener invocation. Removed in finally so it never leaks onto the next
        // record handled by this pooled thread.
        MDC.put("orderId", event.getOrderId());
        try {
            // Idempotency guard keyed on orderId, not eventId: this protects against ANY
            // duplicate payment for the same order — not just redelivery of the same Kafka
            // message, but also a second, distinct OrderCreated event that references an
            // order we already charged (e.g. a bug upstream). eventId-based dedup would miss
            // that case. Key is set at the end, after processing succeeds — not here — so
            // that a retry (the error handler re-invokes this method with the same record
            // after a failure) still sees no key and actually retries, instead of being
            // silently swallowed as a "duplicate" before it ever reaches the DLQ.
            String idempotencyKey = IDEMPOTENCY_KEY_PREFIX + event.getOrderId();
            // hasKey() returns Boolean, not boolean — Boolean.TRUE.equals(...) avoids an
            // NPE from auto-unboxing if it ever returns null, unlike `if (hasKey(...))`.
            if (Boolean.TRUE.equals(redisTemplate.hasKey(idempotencyKey))) {
                log.info("[OrderCreatedConsumer] Duplicate order, skipping: eventId={}, orderId={}",
                        event.getEventId(), event.getOrderId());
                return;
            }

            log.info("[OrderCreatedConsumer] Received OrderCreated: eventId={}, orderId={}, amount={}",
                    event.getEventId(), event.getOrderId(), event.getAmount());

            // Recovery path: the DB save and the Kafka send below are two separate
            // operations (no outbox table here, unlike order-service). A retry can land
            // here after the save succeeded but publishing failed. orderId is unique on
            // Payment, so re-inserting would throw and loop forever — reuse the existing
            // row instead.
            Payment payment = paymentRepository.findByOrderId(event.getOrderId())
                    .orElseGet(() -> {
                        boolean declined = event.getAmount().compareTo(DECLINE_ABOVE_AMOUNT) > 0;
                        Payment newPayment = Payment.builder()
                                .orderId(event.getOrderId())
                                .customerId(event.getCustomerId())
                                .amount(event.getAmount())
                                .status(declined ? PaymentStatus.FAILED : PaymentStatus.COMPLETED)
                                .processedAt(LocalDateTime.now())
                                .build();
                        return paymentRepository.save(newPayment);
                    });

            if (payment.getStatus() == PaymentStatus.FAILED) {
                publishPaymentFailed(event, payment);
            } else {
                publishPaymentProcessed(event, payment);
            }

            // Only mark as fully processed once publishing has actually succeeded.
            redisTemplate.opsForValue().set(idempotencyKey, "1", IDEMPOTENCY_TTL);

            log.info("[OrderCreatedConsumer] Payment processed: paymentId={}, orderId={}, status={}",
                    payment.getPaymentId(), payment.getOrderId(), payment.getStatus());
        } finally {
            MDC.remove("orderId");
        }
    }

    private void publishPaymentProcessed(OrderCreatedEvent event, Payment payment) throws Exception {
        PaymentProcessedEvent processedEvent = PaymentProcessedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(event.getOrderId())
                .customerId(event.getCustomerId())
                .productId(event.getProductId())
                .quantity(event.getQuantity())
                .amount(event.getAmount())
                .paymentId(payment.getPaymentId())
                .occurredAt(LocalDateTime.now())
                .build();

        kafkaTemplate.send("payment.processed", event.getOrderId(),
                objectMapper.writeValueAsString(processedEvent)).get();
        paymentProcessedCounter.increment();
    }

    private void publishPaymentFailed(OrderCreatedEvent event, Payment payment) throws Exception {
        PaymentFailedEvent failedEvent = PaymentFailedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(event.getOrderId())
                .customerId(event.getCustomerId())
                .amount(event.getAmount())
                .paymentId(payment.getPaymentId())
                .reason("Amount exceeds simulated approval limit ($" + DECLINE_ABOVE_AMOUNT + ")")
                .occurredAt(LocalDateTime.now())
                .build();

        kafkaTemplate.send("payment.failed", event.getOrderId(),
                objectMapper.writeValueAsString(failedEvent)).get();
        paymentFailedCounter.increment();
    }
}
