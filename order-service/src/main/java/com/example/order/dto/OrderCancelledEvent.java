package com.example.order.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// order-service's own compensation event — published via the outbox pattern,
// same as OrderCreatedEvent.
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCancelledEvent {

    private String eventId;
    private String orderId;
    private String reason;
    private LocalDateTime occurredAt;
}
