package com.example.order.dto;

import com.example.order.entity.OrderStatus;
import lombok.Builder;
import lombok.Data;

// What we return to the client after POST /orders
@Data
@Builder
public class CreateOrderResponse {
    private String orderId;
    private OrderStatus status;
    private String message;
}