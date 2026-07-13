package com.example.notification.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlingConfig {

    // Per-service DLQ, not Spring's default "<topic>.DLT" — payment, inventory, and
    // notification are three independent consumer groups on order.created and fail
    // independently. A shared .DLT topic would mix failures from all three together
    // and you'd lose which service actually failed to process a given message.
    private static final String DLQ_TOPIC = "order.created.dlq.notification-service";

    @Bean
    public NewTopic notificationDlqTopic() {
        return TopicBuilder.name(DLQ_TOPIC)
                .partitions(1)   // DLQ is low-volume by design — only failed messages land here
                .replicas(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(DLQ_TOPIC, -1));  // -1 = let Kafka pick the partition

        // 2 retries, 1s apart (3 attempts total) — then give up and route to the DLQ.
        // No exponential backoff here: notifications are fire-and-forget and non-critical
        // (see OrderCreatedConsumer), so we don't hold up the consumer group chasing a
        // transient failure as long as payment/inventory do.
        FixedBackOff backOff = new FixedBackOff(1_000L, 2);

        return new DefaultErrorHandler(recoverer, backOff);
    }
}
