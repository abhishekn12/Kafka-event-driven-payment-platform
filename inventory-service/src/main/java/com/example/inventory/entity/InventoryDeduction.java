package com.example.inventory.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "inventory_deductions")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryDeduction {

    @Id
    @Column(name = "deduction_id", updatable = false, nullable = false)
    private String deductionId;

    // orderId the stock was deducted for — unique so we never double-deduct an order
    @Column(name = "order_id", nullable = false, unique = true)
    private String orderId;

    @Column(name = "product_id", nullable = false)
    private String productId;

    @Column(nullable = false)
    private Integer quantity;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private InventoryStatus status;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;

    @PrePersist
    public void prePersist() {
        if (this.deductionId == null) {
            this.deductionId = UUID.randomUUID().toString();
        }
    }
}
