package com.example.order.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

// Local copy of payment-service's event contract.
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
