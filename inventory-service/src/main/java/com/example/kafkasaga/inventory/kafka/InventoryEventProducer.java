package com.example.kafkasaga.inventory.kafka;

import com.example.kafkasaga.events.InventoryFailedEvent;
import com.example.kafkasaga.events.InventoryReservedEvent;
import com.example.kafkasaga.events.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventProducer.class);

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public InventoryEventProducer(KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishReserved(InventoryReservedEvent event) {
        kafkaTemplate.send(Topics.INVENTORY_RESERVED, event.orderId(), event);
        log.info("Published {} for orderId={}", Topics.INVENTORY_RESERVED, event.orderId());
    }

    public void publishFailed(InventoryFailedEvent event) {
        kafkaTemplate.send(Topics.INVENTORY_FAILED, event.orderId(), event);
        log.info("Published {} for orderId={} ({})", Topics.INVENTORY_FAILED, event.orderId(), event.reason());
    }
}
