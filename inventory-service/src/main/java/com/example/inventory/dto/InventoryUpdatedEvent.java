package com.example.inventory.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// Published when stock is successfully deducted. order-service consumes this to
// transition the order to CONFIRMED — the saga's happy-path terminal event.
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryUpdatedEvent {

    private String eventId;
    private String orderId;
    private String customerId;
    private String productId;
    private Integer quantity;
    private LocalDateTime occurredAt;
}
