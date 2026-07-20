package com.example.order.config;

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

    // One DLQ for the whole service, not per-source-topic — order-service now consumes
    // three different topics (payment.failed, inventory.failed, inventory.updated) that
    // all feed the same saga-outcome handling. A lost compensation event here is
    // arguably worse than a lost happy-path one (an order stuck in PENDING forever
    // after a payment was declined), so it gets the same aggressive retry policy.
    private static final String DLQ_TOPIC = "order-service.dlq";

    @Bean
    public NewTopic orderServiceDlqTopic() {
        return TopicBuilder.name(DLQ_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                                  MeterRegistry meterRegistry) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(DLQ_TOPIC, -1));

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
                .tag("service", "order-service")
                .register(meterRegistry);
        Counter dlqCounter = Counter.builder("kafka.consumer.dlq")
                .description("Records recovered to the DLQ after exhausting retries")
                .tag("service", "order-service")
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
