package com.example.kafkasaga.order.kafka;

import com.example.kafkasaga.events.OrderCreatedEvent;
import com.example.kafkasaga.events.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderCreated(OrderCreatedEvent event) {
        // 동일 orderId를 key로 사용 -> 같은 주문의 이벤트는 항상 같은 파티션에 순서대로 적재된다.
        kafkaTemplate.send(Topics.ORDER_CREATED, event.orderId(), event);
        log.info("Published {} for orderId={}", Topics.ORDER_CREATED, event.orderId());
    }
}
