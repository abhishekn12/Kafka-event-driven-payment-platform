package com.example.order.service;

import com.example.order.dto.CreateOrderRequest;
import com.example.order.dto.CreateOrderResponse;
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
    }
}