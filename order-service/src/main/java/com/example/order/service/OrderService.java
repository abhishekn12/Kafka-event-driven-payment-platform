package com.example.order.service;

import com.example.order.dto.CreateOrderRequest;
import com.example.order.dto.CreateOrderResponse;
import com.example.order.dto.OrderCancelledEvent;
import com.example.order.dto.OrderCreatedEvent;
import com.example.order.entity.Order;
import com.example.order.entity.OrderStatus;
import com.example.order.entity.OutboxEvent;
import com.example.order.repository.OrderRepository;
import com.example.order.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor  // Lombok: generates constructor for all final fields (used for injection)
@Slf4j                    // Lombok: generates log variable (log.info, log.error, etc.)
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;   // for JSON serialization

    // @Transactional is the critical annotation here.
    // It wraps BOTH the order INSERT and outbox INSERT in one database transaction.
    // If anything fails — even after the order is saved — both writes roll back together.
    // This is the core guarantee of the Outbox Pattern.
    @Transactional
    public CreateOrderResponse createOrder(CreateOrderRequest request) {
        // 1. Build and save the Order
        Order order = Order.builder()
                .orderId(UUID.randomUUID().toString())
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .amount(request.getAmount())
                .status(OrderStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();

        // Correlation ID for every log line touched by this order, across this whole
        // method — not just the ones that explicitly interpolate orderId into the
        // message. Removed in finally so it never leaks onto the next record handled
        // by this pooled thread.
        MDC.put("orderId", order.getOrderId());
        try {
            orderRepository.save(order);
            log.info("[OrderService] Order saved: orderId={}, customerId={}",
                    order.getOrderId(), order.getCustomerId());

            // 2. Build the event payload
            OrderCreatedEvent event = OrderCreatedEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .orderId(order.getOrderId())
                    .customerId(order.getCustomerId())
                    .productId(order.getProductId())
                    .quantity(order.getQuantity())
                    .amount(order.getAmount())
                    .occurredAt(LocalDateTime.now())
                    .build();

            // 3. Serialize event to JSON string for outbox storage
            String payload;
            try {
                payload = objectMapper.writeValueAsString(event);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize OrderCreatedEvent", e);
            }

            // 4. Write outbox event in the SAME transaction as the order save
            // If Kafka is down right now — doesn't matter. Event is safe in DB.
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .eventId(event.getEventId())
                    .topic("order.created")
                    .aggregateId(order.getOrderId())   // used as Kafka message key
                    .payload(payload)
                    .published(false)
                    .createdAt(LocalDateTime.now())
                    .build();

            outboxEventRepository.save(outboxEvent);
            log.info("[OrderService] Outbox event saved: eventId={}, topic=order.created",
                    event.getEventId());

            // 5. Return immediately — don't wait for Kafka publish or payment processing
            return CreateOrderResponse.builder()
                    .orderId(order.getOrderId())
                    .status(OrderStatus.PENDING)
                    .message("Order accepted. Processing initiated.")
                    .build();
        } finally {
            MDC.remove("orderId");
        }
    }

    // Saga happy-path terminal state, triggered by InventoryUpdated.
    // Guarded to only apply PENDING -> CONFIRMED: if the order is already in a
    // terminal state (e.g. a retry redelivers this after we already transitioned),
    // skip rather than overwrite. This is what makes the transition idempotent
    // without needing a separate Redis dedup layer like payment/inventory have.
    // MDC is not managed here — this is only ever called from SagaOutcomeConsumer,
    // which already establishes the orderId correlation context for the whole
    // listener invocation. Managing it here too would risk this method's own
    // finally-remove wiping the consumer's still-active context if it logs again
    // after this call returns.
    @Transactional
    public void confirmOrder(String orderId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            log.error("[OrderService] Cannot confirm — order not found: orderId={}", orderId);
            return;
        }
        if (order.getStatus() != OrderStatus.PENDING) {
            log.info("[OrderService] Order already in terminal state, skipping confirm: orderId={}, status={}",
                    orderId, order.getStatus());
            return;
        }

        order.setStatus(OrderStatus.CONFIRMED);
        orderRepository.save(order);
        log.info("[OrderService] Order confirmed: orderId={}", orderId);
    }

    // Saga compensation, triggered by PaymentFailed or InventoryFailed.
    // Same PENDING-only guard as confirmOrder. Writes the order.cancelled outbox
    // event in the SAME transaction as the status flip — unlike payment/inventory's
    // consumers (which publish directly after their DB save, and so need a
    // find-or-create recovery path for the gap between the two), this reuses the
    // existing transactional-outbox guarantee: a retry either did both the status
    // update and the outbox write, or neither.
    // MDC is not managed here — see confirmOrder for why (only ever called from
    // SagaOutcomeConsumer, which owns the correlation context for this invocation).
    @Transactional
    public void cancelOrder(String orderId, String reason) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            log.error("[OrderService] Cannot cancel — order not found: orderId={}", orderId);
            return;
        }
        if (order.getStatus() != OrderStatus.PENDING) {
            log.info("[OrderService] Order already in terminal state, skipping cancel: orderId={}, status={}",
                    orderId, order.getStatus());
            return;
        }

        order.setStatus(OrderStatus.CANCELLED);
        orderRepository.save(order);

        OrderCancelledEvent event = OrderCancelledEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(orderId)
                .reason(reason)
                .occurredAt(LocalDateTime.now())
                .build();

        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize OrderCancelledEvent", e);
        }

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .eventId(event.getEventId())
                .topic("order.cancelled")
                .aggregateId(orderId)
                .payload(payload)
                .published(false)
                .createdAt(LocalDateTime.now())
                .build();

        outboxEventRepository.save(outboxEvent);
        log.info("[OrderService] Order cancelled: orderId={}, reason={}", orderId, reason);
    }
}