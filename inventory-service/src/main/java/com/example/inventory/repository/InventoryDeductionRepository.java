package com.example.inventory.repository;

import com.example.inventory.entity.InventoryDeduction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface InventoryDeductionRepository extends JpaRepository<InventoryDeduction, String> {

    Optional<InventoryDeduction> findByOrderId(String orderId);
}
