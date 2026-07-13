package com.example.payment.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

// Local copy of order-service's event contract (com.example.order.dto.OrderCreatedEvent).
// No shared module between services, so each consumer owns its own copy —
// keep field names/types in sync with the producer by hand.
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {

    private String eventId;
    private String orderId;
    private String customerId;
    private String productId;
    private Integer quantity;
    private BigDecimal amount;
    private LocalDateTime occurredAt;
}
