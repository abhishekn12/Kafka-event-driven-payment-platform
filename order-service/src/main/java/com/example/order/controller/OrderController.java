package com.example.order.controller;

import com.example.order.dto.CreateOrderRequest;
import com.example.order.dto.CreateOrderResponse;
import com.example.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<CreateOrderResponse> createOrder(@RequestBody CreateOrderRequest request) {
        log.info("[OrderController] Received order request: customerId={}, productId={}",
                request.getCustomerId(), request.getProductId());

        CreateOrderResponse response = orderService.createOrder(request);

        // 202 Accepted (not 201 Created) — the order is accepted but not yet fulfilled
        // The client should poll GET /orders/{id} to check final status
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<String> getOrderStatus(@PathVariable String orderId) {
        // Placeholder — returns orderId for now
        // Full implementation: query OrderRepository, return status
        return ResponseEntity.ok("Order " + orderId + " status: PENDING");
    }
}