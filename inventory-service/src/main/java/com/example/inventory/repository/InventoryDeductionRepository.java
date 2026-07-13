package com.example.inventory.repository;

import com.example.inventory.entity.InventoryDeduction;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InventoryDeductionRepository extends JpaRepository<InventoryDeduction, String> {
}
