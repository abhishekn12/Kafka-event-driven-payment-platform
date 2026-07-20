package com.example.payment.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

// Published when a payment is declined. order-service consumes this to trigger
// the OrderCancelled compensation — inventory never runs for this order since
// it's only triggered by PaymentProcessed.
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentFailedEvent {

    private String eventId;
    private String orderId;
    private String customerId;
    private BigDecimal amount;
    private String paymentId;
    private String reason;
    private LocalDateTime occurredAt;
}
