package com.example.order.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "orders")          // "order" is a reserved SQL keyword — always use "orders"
@Data                            // Lombok: generates getters, setters, equals, hashCode, toString
@Builder                         // Lombok: enables Order.builder().orderId(...).build() pattern
@NoArgsConstructor               // Lombok: generates no-arg constructor (required by JPA)
@AllArgsConstructor              // Lombok: generates all-args constructor (used by @Builder)
public class Order {

    @Id
    @Column(name = "order_id", updatable = false, nullable = false)
    private String orderId;      // UUID as String — set manually before save, not auto-generated

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Column(name = "product_id", nullable = false)
    private String productId;

    @Column(nullable = false)
    private Integer quantity;

    @Column(nullable = false, precision = 10, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING) // store enum as "PENDING", not 0/1/2
    @Column(nullable = false)
    private OrderStatus status;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    // Called automatically by JPA before first INSERT
    @PrePersist
    public void prePersist() {
        if (this.orderId == null) {
            this.orderId = UUID.randomUUID().toString();
        }
        this.createdAt = LocalDateTime.now();
        this.status = OrderStatus.PENDING;
    }

    @PreUpdate
    public void preUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
}