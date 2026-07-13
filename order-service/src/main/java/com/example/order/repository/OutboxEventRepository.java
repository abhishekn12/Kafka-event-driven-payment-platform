package com.example.order.repository;

import com.example.order.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    // Spring Data JPA generates this SQL automatically from the method name:
    // SELECT * FROM outbox_events WHERE published = false ORDER BY created_at ASC
    List<OutboxEvent> findByPublishedFalseOrderByCreatedAtAsc();
}