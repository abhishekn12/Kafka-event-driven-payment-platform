package com.example.inventory.consumer;

import com.example.inventory.dto.OrderCreatedEvent;
import com.example.inventory.entity.InventoryDeduction;
import com.example.inventory.entity.InventoryStatus;
import com.example.inventory.repository.InventoryDeductionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedConsumer {

    private static final String IDEMPOTENCY_KEY_PREFIX = "inventory:processed-order:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    private final InventoryDeductionRepository inventoryDeductionRepository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "order.created")
    public void onOrderCreated(String payload) throws Exception {
        OrderCreatedEvent event = objectMapper.readValue(payload, OrderCreatedEvent.class);

        // Idempotency guard keyed on orderId, not eventId: this protects against ANY
        // duplicate deduction for the same order — not just redelivery of the same Kafka
        // message, but also a second, distinct OrderCreated event that references an
        // order we already deducted stock for (e.g. a bug upstream). eventId-based dedup
        // would miss that case. Key is set at the end, after processing succeeds — not
        // here — so that a retry (the error handler re-invokes this method with the same
        // record after a failure) still sees no key and actually retries, instead of
        // being silently swallowed as a "duplicate" before it ever reaches the DLQ.
        String idempotencyKey = IDEMPOTENCY_KEY_PREFIX + event.getOrderId();
        if (Boolean.TRUE.equals(redisTemplate.hasKey(idempotencyKey))) {
            log.info("[OrderCreatedConsumer] Duplicate order, skipping: eventId={}, orderId={}",
                    event.getEventId(), event.getOrderId());
            return;
        }

        log.info("[OrderCreatedConsumer] Received OrderCreated: eventId={}, orderId={}, productId={}, quantity={}",
                event.getEventId(), event.getOrderId(), event.getProductId(), event.getQuantity());

        // Stub inventory deduction — always succeeds. Real stock-check comes later.
        InventoryDeduction deduction = InventoryDeduction.builder()
                .orderId(event.getOrderId())
                .productId(event.getProductId())
                .quantity(event.getQuantity())
                .status(InventoryStatus.DEDUCTED)
                .processedAt(LocalDateTime.now())
                .build();

        inventoryDeductionRepository.save(deduction);

        // Only mark as processed once the save has actually succeeded.
        redisTemplate.opsForValue().set(idempotencyKey, "1", IDEMPOTENCY_TTL);

        log.info("[OrderCreatedConsumer] Inventory deducted: deductionId={}, orderId={}, status={}",
                deduction.getDeductionId(), deduction.getOrderId(), deduction.getStatus());
    }
}
