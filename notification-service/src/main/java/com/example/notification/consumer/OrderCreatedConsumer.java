package com.example.notification.consumer;

import com.example.notification.dto.OrderCreatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedConsumer {

    private final ObjectMapper objectMapper;

    // Fire-and-forget — no DB, no idempotency guard. A duplicate notification
    // is a minor annoyance, not a correctness bug (unlike a double payment/deduction).
    @KafkaListener(topics = "order.created")
    public void onOrderCreated(String payload) throws Exception {
        OrderCreatedEvent event = objectMapper.readValue(payload, OrderCreatedEvent.class);

        // Correlation ID for every log line this record touches. Removed in finally
        // so it never leaks onto the next record handled by this pooled thread.
        MDC.put("orderId", event.getOrderId());
        try {
            log.info("[OrderCreatedConsumer] Received OrderCreated: eventId={}, orderId={}, customerId={}",
                    event.getEventId(), event.getOrderId(), event.getCustomerId());

            // Stub notification dispatch — real email/SMS provider integration comes later.
            log.info("[OrderCreatedConsumer] EMAIL stub -> customerId={}: \"Your order {} for {} x{} has been received.\"",
                    event.getCustomerId(), event.getOrderId(), event.getProductId(), event.getQuantity());
            log.info("[OrderCreatedConsumer] SMS stub -> customerId={}: \"Order {} confirmed, total ${}.\"",
                    event.getCustomerId(), event.getOrderId(), event.getAmount());
        } finally {
            MDC.remove("orderId");
        }
    }
}
