package com.example.order.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    @Id
    @Column(name = "event_id")
    private String eventId;

    // Which Kafka topic this event should be published to
    @Column(name = "topic", nullable = false)
    private String topic;

    // The orderId — used as the Kafka message key
    // Kafka guarantees ordering within a partition for the same key
    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    // The full event payload as a JSON string
    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    // Has this event been published to Kafka yet?
    @Column(name = "published", nullable = false)
    private boolean published;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "published_at")
    private LocalDateTime publishedAt;

    @PrePersist
    public void prePersist() {
        if (this.eventId == null) {
            this.eventId = UUID.randomUUID().toString();
        }
        this.createdAt = LocalDateTime.now();
        this.published = false;
    }
}