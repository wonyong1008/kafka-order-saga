package com.example.kafkasaga.payment.kafka;

import com.example.kafkasaga.events.PaymentCompletedEvent;
import com.example.kafkasaga.events.PaymentFailedEvent;
import com.example.kafkasaga.events.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventProducer.class);

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public PaymentEventProducer(KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishCompleted(PaymentCompletedEvent event) {
        kafkaTemplate.send(Topics.PAYMENT_COMPLETED, event.orderId(), event);
        log.info("Published {} for orderId={}", Topics.PAYMENT_COMPLETED, event.orderId());
    }

    public void publishFailed(PaymentFailedEvent event) {
        kafkaTemplate.send(Topics.PAYMENT_FAILED, event.orderId(), event);
        log.info("Published {} for orderId={} ({})", Topics.PAYMENT_FAILED, event.orderId(), event.reason());
    }
}
