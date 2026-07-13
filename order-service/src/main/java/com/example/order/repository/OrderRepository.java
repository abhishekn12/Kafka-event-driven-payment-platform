package com.example.order.repository;

import com.example.order.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderRepository extends JpaRepository<Order, String> {
    // JpaRepository gives you: save(), findById(), findAll(), delete() for free
    // String = type of the primary key (orderId is a String/UUID)
    // No extra methods needed for now
}