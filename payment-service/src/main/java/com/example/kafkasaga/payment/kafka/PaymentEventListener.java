package com.example.kafkasaga.payment.kafka;

import com.example.kafkasaga.events.InventoryReservedEvent;
import com.example.kafkasaga.events.PaymentCompletedEvent;
import com.example.kafkasaga.events.PaymentFailedEvent;
import com.example.kafkasaga.events.Topics;
import com.example.kafkasaga.payment.domain.Payment;
import com.example.kafkasaga.payment.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Component
public class PaymentEventListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventListener.class);

    /** 데모용 결제 실패 임계값(원). 이 금액을 초과하는 주문은 결제 승인 거절로 시뮬레이션한다. */
    private static final long DECLINE_THRESHOLD = 500_000L;

    private final PaymentRepository paymentRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final PaymentEventProducer paymentEventProducer;

    public PaymentEventListener(PaymentRepository paymentRepository,
                                 ProcessedEventRepository processedEventRepository,
                                 PaymentEventProducer paymentEventProducer) {
        this.paymentRepository = paymentRepository;
        this.processedEventRepository = processedEventRepository;
        this.paymentEventProducer = paymentEventProducer;
    }

    @KafkaListener(topics = Topics.INVENTORY_RESERVED, groupId = "payment-service")
    @Transactional
    public void onInventoryReserved(InventoryReservedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            log.info("Skip already-processed event {} ({})", event.eventId(),
                    event.getClass().getSimpleName());
            return;
        }

        boolean approved = event.amount() <= DECLINE_THRESHOLD;
        String reason = approved ? "결제 승인" : "결제 한도 초과 (금액 " + event.amount() + "원)";

        paymentRepository.save(new Payment(event.orderId(), event.amount(), approved, reason));

        if (approved) {
            paymentEventProducer.publishCompleted(new PaymentCompletedEvent(
                    UUID.randomUUID().toString(), event.orderId(), event.productId(),
                    event.quantity(), event.amount(), Instant.now()));
        } else {
            paymentEventProducer.publishFailed(new PaymentFailedEvent(
                    UUID.randomUUID().toString(), event.orderId(), event.productId(),
                    event.quantity(), event.amount(), reason, Instant.now()));
        }

        processedEventRepository.save(new ProcessedEvent(event.eventId()));
    }
}
