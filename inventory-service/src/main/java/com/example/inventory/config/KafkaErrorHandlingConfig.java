package com.example.inventory.config;

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
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaErrorHandlingConfig {

    // Per-service DLQ, not Spring's default "<topic>.DLT" — each consuming service
    // fails independently, and a shared .DLT topic would mix failures together and
    // lose which service actually failed. Named after payment.processed, not
    // order.created — inventory's trigger event changed with the saga (see
    // SAGA_DESIGN.md): it now only runs after payment succeeds.
    private static final String DLQ_TOPIC = "payment.processed.dlq.inventory-service";

    @Bean
    public NewTopic inventoryDlqTopic() {
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

        // 4 retries at 1s, 2s, 4s, 8s (5 attempts total) — then give up and route to the DLQ.
        // Same policy as payment-service: stock deductions are also correctness-critical,
        // so retry aggressively enough to ride out a transient DB/broker blip.
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(4);
        backOff.setInitialInterval(1_000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.setRetryListeners(retryMetricsListener(meterRegistry));
        return errorHandler;
    }

    private RetryListener retryMetricsListener(MeterRegistry meterRegistry) {
        Counter retryCounter = Counter.builder("kafka.consumer.retry")
                .description("Kafka listener retry attempts")
                .tag("service", "inventory-service")
                .register(meterRegistry);
        Counter dlqCounter = Counter.builder("kafka.consumer.dlq")
                .description("Records recovered to the DLQ after exhausting retries")
                .tag("service", "inventory-service")
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
