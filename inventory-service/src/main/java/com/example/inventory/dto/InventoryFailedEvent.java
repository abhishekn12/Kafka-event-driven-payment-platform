package com.example.inventory.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// Published when stock deduction fails. order-service consumes this to trigger the
// OrderCancelled compensation — note payment already succeeded at this point, so a
// real system would also need to refund it (out of scope here, see SAGA_DESIGN.md).
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
