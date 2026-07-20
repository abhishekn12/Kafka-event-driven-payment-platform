package com.example.notification.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlingConfig {

    // One DLQ for the whole service, not per-source-topic — notification-service now
    // listens to four topics (order.created plus the three saga outcomes), and a
    // shared .DLT-per-topic scheme would scatter its failures across four topics for
    // no benefit. Per-service (not the platform default) still keeps it separate from
    // payment/inventory/order-service's own DLQs.
    private static final String DLQ_TOPIC = "notification-service.dlq";

    @Bean
    public NewTopic notificationDlqTopic() {
        return TopicBuilder.name(DLQ_TOPIC)
                .partitions(1)   // DLQ is low-volume by design — only failed messages land here
                .replicas(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                                  MeterRegistry meterRegistry) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(DLQ_TOPIC, -1));  // -1 = let Kafka pick the partition

        // 2 retries, 1s apart (3 attempts total) — then give up and route to the DLQ.
        // No exponential backoff here: notifications are fire-and-forget and non-critical
        // (see OrderCreatedConsumer), so we don't hold up the consumer group chasing a
        // transient failure as long as payment/inventory do.
        FixedBackOff backOff = new FixedBackOff(1_000L, 2);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.setRetryListeners(retryMetricsListener(meterRegistry));
        return errorHandler;
    }

    private RetryListener retryMetricsListener(MeterRegistry meterRegistry) {
        Counter retryCounter = Counter.builder("kafka.consumer.retry")
                .description("Kafka listener retry attempts")
                .tag("service", "notification-service")
                .register(meterRegistry);
        Counter dlqCounter = Counter.builder("kafka.consumer.dlq")
                .description("Records recovered to the DLQ after exhausting retries")
                .tag("service", "notification-service")
                .register(meterRegistry);

        return new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                if (deliveryAttempt > 1) {
                    retryCounter.increment();
                }
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                dlqCounter.increment();
            }
        };
    }
}
