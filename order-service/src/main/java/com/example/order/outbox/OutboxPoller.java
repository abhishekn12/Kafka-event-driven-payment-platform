package com.example.order.outbox;

import com.example.order.entity.OutboxEvent;
import com.example.order.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPoller {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    // Runs every 5000ms (5 seconds) automatically — no manual trigger needed
    // fixedDelay = wait 5s AFTER the previous run finishes (not a strict schedule)
    // This prevents overlap if a run takes longer than 5s
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void pollAndPublish() {

        // Fetch all unpublished events, oldest first (preserves ordering)
        List<OutboxEvent> unpublishedEvents =
                outboxEventRepository.findByPublishedFalseOrderByCreatedAtAsc();

        if (unpublishedEvents.isEmpty()) {
            return;  // nothing to do — skip logging to keep logs clean
        }

        log.info("[OutboxPoller] Found {} unpublished event(s)", unpublishedEvents.size());

        for (OutboxEvent event : unpublishedEvents) {
            // aggregateId is the orderId for every event this poller handles — set per
            // iteration (one batch can span multiple orders) and cleared after, so it
            // never leaks onto the next event in this same loop.
            MDC.put("orderId", event.getAggregateId());
            try {
                // Send to Kafka synchronously (get() blocks until broker confirms)
                // Key = orderId: ensures all events for the same order go to the same partition
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload())
                        .get();  // blocks — we want to confirm delivery before marking published

                // Mark as published only AFTER successful Kafka delivery
                event.setPublished(true);
                event.setPublishedAt(LocalDateTime.now());
                outboxEventRepository.save(event);

                log.info("[OutboxPoller] Published event: eventId={}, topic={}, orderId={}",
                        event.getEventId(), event.getTopic(), event.getAggregateId());

            } catch (Exception e) {
                // Don't mark as published — it will be retried on the next poll cycle
                log.error("[OutboxPoller] Failed to publish event: eventId={}, error={}",
                        event.getEventId(), e.getMessage());
                // Don't rethrow — we want to continue processing other events in the batch
            } finally {
                MDC.remove("orderId");
            }
        }
    }
}