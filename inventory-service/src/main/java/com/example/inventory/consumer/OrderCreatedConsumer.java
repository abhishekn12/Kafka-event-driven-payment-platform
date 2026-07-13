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

    private static final String IDEMPOTENCY_KEY_PREFIX = "inventory:processed-event:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    private final InventoryDeductionRepository inventoryDeductionRepository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "order.created")
    public void onOrderCreated(String payload) throws Exception {
        OrderCreatedEvent event = objectMapper.readValue(payload, OrderCreatedEvent.class);

        // Idempotency guard: eventId key claimed atomically in Redis.
        // If another instance (or a redelivery) already claimed it, skip processing.
        String idempotencyKey = IDEMPOTENCY_KEY_PREFIX + event.getEventId();
        Boolean firstSeen = redisTemplate.opsForValue()
                .setIfAbsent(idempotencyKey, "1", IDEMPOTENCY_TTL);

        if (Boolean.FALSE.equals(firstSeen)) {
            log.info("[OrderCreatedConsumer] Duplicate delivery, skipping: eventId={}, orderId={}",
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

        log.info("[OrderCreatedConsumer] Inventory deducted: deductionId={}, orderId={}, status={}",
                deduction.getDeductionId(), deduction.getOrderId(), deduction.getStatus());
    }
}
