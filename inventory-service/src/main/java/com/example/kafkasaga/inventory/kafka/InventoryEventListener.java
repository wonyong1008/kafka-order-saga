package com.example.kafkasaga.inventory.kafka;

import com.example.kafkasaga.events.DomainEvent;
import com.example.kafkasaga.events.InventoryFailedEvent;
import com.example.kafkasaga.events.InventoryReservedEvent;
import com.example.kafkasaga.events.OrderCreatedEvent;
import com.example.kafkasaga.events.PaymentFailedEvent;
import com.example.kafkasaga.events.Topics;
import com.example.kafkasaga.inventory.domain.ProductStock;
import com.example.kafkasaga.inventory.repository.ProductStockRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Component
public class InventoryEventListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventListener.class);

    private final ProductStockRepository productStockRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final InventoryEventProducer inventoryEventProducer;

    public InventoryEventListener(ProductStockRepository productStockRepository,
                                   ProcessedEventRepository processedEventRepository,
                                   InventoryEventProducer inventoryEventProducer) {
        this.productStockRepository = productStockRepository;
        this.processedEventRepository = processedEventRepository;
        this.inventoryEventProducer = inventoryEventProducer;
    }

    @KafkaListener(topics = Topics.ORDER_CREATED, groupId = "inventory-service")
    @Transactional
    public void onOrderCreated(OrderCreatedEvent event) {
        withIdempotency(event, () -> {
            Optional<ProductStock> stock = productStockRepository.findById(event.productId());

            if (stock.isEmpty()) {
                publishFailed(event, "존재하지 않는 상품: " + event.productId());
                return;
            }
            if (!stock.get().hasEnoughStock(event.quantity())) {
                publishFailed(event, "재고 부족 (요청 %d / 보유 %d)"
                        .formatted(event.quantity(), stock.get().getAvailableQuantity()));
                return;
            }

            stock.get().decrease(event.quantity());
            productStockRepository.save(stock.get());

            var reserved = new InventoryReservedEvent(
                    UUID.randomUUID().toString(), event.orderId(), event.productId(),
                    event.quantity(), event.amount(), Instant.now());
            inventoryEventProducer.publishReserved(reserved);
        });
    }

    /** 결제 실패에 대한 보상 트랜잭션: 확보했던 재고를 복원한다. */
    @KafkaListener(topics = Topics.PAYMENT_FAILED, groupId = "inventory-service")
    @Transactional
    public void onPaymentFailed(PaymentFailedEvent event) {
        withIdempotency(event, () -> {
            productStockRepository.findById(event.productId()).ifPresent(stock -> {
                stock.restore(event.quantity());
                productStockRepository.save(stock);
                log.info("Restored {} units of {} for orderId={} (payment failed)",
                        event.quantity(), event.productId(), event.orderId());
            });
        });
    }

    private void publishFailed(OrderCreatedEvent event, String reason) {
        var failed = new InventoryFailedEvent(
                UUID.randomUUID().toString(), event.orderId(), event.productId(),
                event.quantity(), reason, Instant.now());
        inventoryEventProducer.publishFailed(failed);
    }

    private void withIdempotency(DomainEvent event, Runnable action) {
        if (processedEventRepository.existsById(event.eventId())) {
            log.info("Skip already-processed event {} ({})", event.eventId(), event.getClass().getSimpleName());
            return;
        }
        action.run();
        processedEventRepository.save(new ProcessedEvent(event.eventId()));
    }
}
