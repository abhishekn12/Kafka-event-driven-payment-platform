package com.example.payment.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

// Published when a payment succeeds. Carries the order details forward
// (inventory-service no longer sees OrderCreated directly — it's triggered
// by this event instead) since inventory needs productId/quantity to act.
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentProcessedEvent {

    private String eventId;
    private String orderId;
    private String customerId;
    private String productId;
    private Integer quantity;
    private BigDecimal amount;
    private String paymentId;
    private LocalDateTime occurredAt;
}
