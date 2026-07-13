package com.example.payment.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaErrorHandlingConfig {

    // Per-service DLQ, not Spring's default "<topic>.DLT" — payment, inventory, and
    // notification are three independent consumer groups on order.created and fail
    // independently. A shared .DLT topic would mix failures from all three together
    // and you'd lose which service actually failed to process a given message.
    private static final String DLQ_TOPIC = "order.created.dlq.payment-service";

    @Bean
    public NewTopic paymentDlqTopic() {
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

        // 4 retries at 1s, 2s, 4s, 8s (5 attempts total) — then give up and route to the DLQ.
        // Payments are money-moving, so retry aggressively enough to ride out a transient
        // blip (DB hiccup, broker rebalance) before giving up.
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(4);
        backOff.setInitialInterval(1_000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000L);

        return new DefaultErrorHandler(recoverer, backOff);
    }
}
