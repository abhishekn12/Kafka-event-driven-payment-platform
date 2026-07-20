package com.example.order.consumer;

import com.example.order.dto.InventoryFailedEvent;
import com.example.order.dto.InventoryUpdatedEvent;
import com.example.order.dto.PaymentFailedEvent;
import com.example.order.service.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// Listens to the three saga terminal events and drives Order.status accordingly.
// Kept thin — actual transition + compensation logic lives in OrderService.
@Component
@RequiredArgsConstructor
@Slf4j
public class SagaOutcomeConsumer {

    private final OrderService orderService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "payment.failed")
    public void onPaymentFailed(String payload) throws Exception {
        PaymentFailedEvent event = objectMapper.readValue(payload, PaymentFailedEvent.class);
        log.info("[SagaOutcomeConsumer] Received PaymentFailed: orderId={}, reason={}",
                event.getOrderId(), event.getReason());
        orderService.cancelOrder(event.getOrderId(), "Payment failed: " + event.getReason());
    }

    @KafkaListener(topics = "inventory.failed")
    public void onInventoryFailed(String payload) throws Exception {
        InventoryFailedEvent event = objectMapper.readValue(payload, InventoryFailedEvent.class);
        log.info("[SagaOutcomeConsumer] Received InventoryFailed: orderId={}, reason={}",
                event.getOrderId(), event.getReason());
        orderService.cancelOrder(event.getOrderId(), "Inventory failed: " + event.getReason());
    }

    @KafkaListener(topics = "inventory.updated")
    public void onInventoryUpdated(String payload) throws Exception {
        InventoryUpdatedEvent event = objectMapper.readValue(payload, InventoryUpdatedEvent.class);
        log.info("[SagaOutcomeConsumer] Received InventoryUpdated: orderId={}", event.getOrderId());
        orderService.confirmOrder(event.getOrderId());
    }
}
