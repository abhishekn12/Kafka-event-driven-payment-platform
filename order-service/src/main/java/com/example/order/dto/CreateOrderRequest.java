package com.example.order.dto;

import lombok.Data;

import java.math.BigDecimal;

// What the client sends in the POST /orders request body
@Data
public class CreateOrderRequest {
    private String customerId;
    private String productId;
    private Integer quantity;
    private BigDecimal amount;
}