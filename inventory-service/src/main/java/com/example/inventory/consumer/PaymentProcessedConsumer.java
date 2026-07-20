package com.example.inventory.consumer;

import com.example.inventory.dto.InventoryFailedEvent;
import com.example.inventory.dto.InventoryUpdatedEvent;
import com.example.inventory.dto.PaymentProcessedEvent;
import com.example.inventory.entity.InventoryDeduction;
import com.example.inventory.entity.InventoryStatus;
import com.example.inventory.repository.InventoryDeductionRepository;
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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

// Triggered by PaymentProcessed, not OrderCreated — inventory only runs after payment
// succeeds (see SAGA_DESIGN.md for why this replaced the old parallel fan-out).
@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentProcessedConsumer {

    private static final String IDEMPOTENCY_KEY_PREFIX = "inventory:processed-order:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    // Stub decline threshold — deterministic so failure-path tests aren't flaky.
    // Framed as simulated insufficient stock; real stock tracking comes later.
    private static final int DECLINE_ABOVE_QUANTITY = 50;

    private final InventoryDeductionRepository inventoryDeductionRepository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MeterRegistry meterRegistry;

    private Counter inventoryUpdatedCounter;
    private Counter inventoryFailedCounter;

    @PostConstruct
    void initMetrics() {
        inventoryUpdatedCounter = Counter.builder("inventory.updated")
                .description("Inventory deductions successfully applied")
                .register(meterRegistry);
        inventoryFailedCounter = Counter.builder("inventory.failed")
                .description("Inventory deductions declined")
                .register(meterRegistry);
    }

    @KafkaListener(topics = "payment.processed")
    public void onPaymentProcessed(String payload) throws Exception {
        PaymentProcessedEvent event = objectMapper.readValue(payload, PaymentProcessedEvent.class);

        // Correlation ID for every log line this record touches, for the rest of this
        // listener invocation. Removed in finally so it never leaks onto the next
        // record handled by this pooled thread.
        MDC.put("orderId", event.getOrderId());
        try {
            // Idempotency guard keyed on orderId, not eventId — see payment-service's
            // consumer for the full reasoning (protects against any duplicate deduction
            // for the same order, not just redelivery of the same Kafka message). Key is
            // set only after publishing succeeds below, not here.
            String idempotencyKey = IDEMPOTENCY_KEY_PREFIX + event.getOrderId();
            // hasKey() returns Boolean, not boolean — Boolean.TRUE.equals(...) avoids an
            // NPE from auto-unboxing if it ever returns null, unlike `if (hasKey(...))`.
            if (Boolean.TRUE.equals(redisTemplate.hasKey(idempotencyKey))) {
                log.info("[PaymentProcessedConsumer] Duplicate order, skipping: eventId={}, orderId={}",
                        event.getEventId(), event.getOrderId());
                return;
            }

            log.info("[PaymentProcessedConsumer] Received PaymentProcessed: eventId={}, orderId={}, productId={}, quantity={}",
                    event.getEventId(), event.getOrderId(), event.getProductId(), event.getQuantity());

            // Recovery path: the DB save and the Kafka send below are two separate
            // operations (no outbox table here). A retry can land here after the save
            // succeeded but publishing failed. orderId is unique on InventoryDeduction,
            // so re-inserting would throw and loop forever — reuse the existing row
            // instead.
            InventoryDeduction deduction = inventoryDeductionRepository.findByOrderId(event.getOrderId())
                    .orElseGet(() -> {
                        boolean declined = event.getQuantity() > DECLINE_ABOVE_QUANTITY;
                        InventoryDeduction newDeduction = InventoryDeduction.builder()
                                .orderId(event.getOrderId())
                                .productId(event.getProductId())
                                .quantity(event.getQuantity())
                                .status(declined ? InventoryStatus.FAILED : InventoryStatus.DEDUCTED)
                                .processedAt(LocalDateTime.now())
                                .build();
                        return inventoryDeductionRepository.save(newDeduction);
                    });

            if (deduction.getStatus() == InventoryStatus.FAILED) {
                publishInventoryFailed(event, deduction);
            } else {
                publishInventoryUpdated(event, deduction);
            }

            // Only mark as fully processed once publishing has actually succeeded.
            redisTemplate.opsForValue().set(idempotencyKey, "1", IDEMPOTENCY_TTL);

            log.info("[PaymentProcessedConsumer] Inventory processed: deductionId={}, orderId={}, status={}",
                    deduction.getDeductionId(), deduction.getOrderId(), deduction.getStatus());
        } finally {
            MDC.remove("orderId");
        }
    }

    private void publishInventoryUpdated(PaymentProcessedEvent event, InventoryDeduction deduction) throws Exception {
        InventoryUpdatedEvent updatedEvent = InventoryUpdatedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(event.getOrderId())
                .customerId(event.getCustomerId())
                .productId(event.getProductId())
                .quantity(event.getQuantity())
                .occurredAt(LocalDateTime.now())
                .build();

        kafkaTemplate.send("inventory.updated", event.getOrderId(),
                objectMapper.writeValueAsString(updatedEvent)).get();
        inventoryUpdatedCounter.increment();
    }

    private void publishInventoryFailed(PaymentProcessedEvent event, InventoryDeduction deduction) throws Exception {
        InventoryFailedEvent failedEvent = InventoryFailedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(event.getOrderId())
                .customerId(event.getCustomerId())
                .productId(event.getProductId())
                .quantity(event.getQuantity())
                .reason("Requested quantity exceeds simulated available stock (" + DECLINE_ABOVE_QUANTITY + ")")
                .occurredAt(LocalDateTime.now())
                .build();

        kafkaTemplate.send("inventory.failed", event.getOrderId(),
                objectMapper.writeValueAsString(failedEvent)).get();
        inventoryFailedCounter.increment();
    }
}
