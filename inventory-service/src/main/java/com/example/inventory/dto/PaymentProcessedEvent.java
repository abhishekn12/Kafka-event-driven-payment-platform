package com.example.inventory.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

// Local copy of payment-service's event contract. This is now inventory-service's
// trigger event (replacing OrderCreated) — inventory only runs after payment succeeds.
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
