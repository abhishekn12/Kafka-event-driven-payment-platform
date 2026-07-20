package com.example.order.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// Local copy of inventory-service's event contract.
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryFailedEvent {

    private String eventId;
    private String orderId;
    private String customerId;
    private String productId;
    private Integer quantity;
    private String reason;
    private LocalDateTime occurredAt;
}
