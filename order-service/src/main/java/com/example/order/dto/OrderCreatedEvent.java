package com.example.order.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

// This is what other services (Payment, Inventory, Notification) will receive
// When you change this, ALL consuming services must be updated — treat it like an API contract
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {

    private String eventId;       // unique event ID — used for idempotency checks in consumers
    private String orderId;       // links back to the order
    private String customerId;
    private String productId;
    private Integer quantity;
    private BigDecimal amount;
    private LocalDateTime occurredAt;  // when the event was created
}