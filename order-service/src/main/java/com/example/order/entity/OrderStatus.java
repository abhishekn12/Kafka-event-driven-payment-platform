package com.example.order.entity;

public enum OrderStatus {
    PENDING,        // order placed, waiting for payment
    CONFIRMED,      // payment succeeded, inventory reserved
    CANCELLED,      // payment failed or inventory failed
    FAILED          // unexpected system failure
}