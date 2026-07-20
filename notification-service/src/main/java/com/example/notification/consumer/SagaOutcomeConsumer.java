package com.example.notification.consumer;

import com.example.notification.dto.InventoryFailedEvent;
import com.example.notification.dto.InventoryUpdatedEvent;
import com.example.notification.dto.PaymentFailedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// Reacts to the saga's terminal events with a stub confirmation/failure notification,
// on top of the "order received" notification OrderCreatedConsumer already sends.
// Same fire-and-forget stance as OrderCreatedConsumer — no DB, no idempotency guard.
@Component
@RequiredArgsConstructor
@Slf4j
public class SagaOutcomeConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "payment.failed")
    public void onPaymentFailed(String payload) throws Exception {
        PaymentFailedEvent event = objectMapper.readValue(payload, PaymentFailedEvent.class);
        log.info("[SagaOutcomeConsumer] EMAIL stub -> customerId={}: \"Your order {} could not be processed: {}\"",
                event.getCustomerId(), event.getOrderId(), event.getReason());
    }

    @KafkaListener(topics = "inventory.failed")
    public void onInventoryFailed(String payload) throws Exception {
        InventoryFailedEvent event = objectMapper.readValue(payload, InventoryFailedEvent.class);
        log.info("[SagaOutcomeConsumer] EMAIL stub -> customerId={}: \"Your order {} was cancelled: {}\"",
                event.getCustomerId(), event.getOrderId(), event.getReason());
    }

    @KafkaListener(topics = "inventory.updated")
    public void onInventoryUpdated(String payload) throws Exception {
        InventoryUpdatedEvent event = objectMapper.readValue(payload, InventoryUpdatedEvent.class);
        log.info("[SagaOutcomeConsumer] EMAIL stub -> customerId={}: \"Your order {} is confirmed and on its way!\"",
                event.getCustomerId(), event.getOrderId());
        log.info("[SagaOutcomeConsumer] SMS stub -> customerId={}: \"Order {} confirmed.\"",
                event.getCustomerId(), event.getOrderId());
    }
}
