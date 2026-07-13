package com.example.payment.consumer;

import com.example.payment.dto.OrderCreatedEvent;
import com.example.payment.entity.Payment;
import com.example.payment.entity.PaymentStatus;
import com.example.payment.repository.PaymentRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedConsumer {

    private static final String IDEMPOTENCY_KEY_PREFIX = "payment:processed-event:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    private final PaymentRepository paymentRepository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "order.created")
    public void onOrderCreated(String payload) throws Exception {
        OrderCreatedEvent event = objectMapper.readValue(payload, OrderCreatedEvent.class);

        // Idempotency guard: eventId key claimed atomically in Redis.
        // If another instance (or a redelivery) already claimed it, skip processing.
        String idempotencyKey = IDEMPOTENCY_KEY_PREFIX + event.getEventId();
        Boolean firstSeen = redisTemplate.opsForValue()
                .setIfAbsent(idempotencyKey, "1", IDEMPOTENCY_TTL);

        if (Boolean.FALSE.equals(firstSeen)) {
            log.info("[OrderCreatedConsumer] Duplicate delivery, skipping: eventId={}, orderId={}",
                    event.getEventId(), event.getOrderId());
            return;
        }

        log.info("[OrderCreatedConsumer] Received OrderCreated: eventId={}, orderId={}, amount={}",
                event.getEventId(), event.getOrderId(), event.getAmount());

        // Stub payment processing — always approves. Real gateway integration comes later.
        Payment payment = Payment.builder()
                .orderId(event.getOrderId())
                .customerId(event.getCustomerId())
                .amount(event.getAmount())
                .status(PaymentStatus.COMPLETED)
                .processedAt(LocalDateTime.now())
                .build();

        paymentRepository.save(payment);

        log.info("[OrderCreatedConsumer] Payment processed: paymentId={}, orderId={}, status={}",
                payment.getPaymentId(), payment.getOrderId(), payment.getStatus());
    }
}
