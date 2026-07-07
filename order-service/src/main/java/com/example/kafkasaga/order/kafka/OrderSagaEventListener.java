package com.example.kafkasaga.order.kafka;

import com.example.kafkasaga.events.DomainEvent;
import com.example.kafkasaga.events.InventoryFailedEvent;
import com.example.kafkasaga.events.PaymentCompletedEvent;
import com.example.kafkasaga.events.PaymentFailedEvent;
import com.example.kafkasaga.events.Topics;
import com.example.kafkasaga.order.domain.Order;
import com.example.kafkasaga.order.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.NoSuchElementException;

/**
 * Choreography Saga의 마지막 단계: 재고/결제 서비스가 발행한 이벤트를 구독해
 * 주문 상태를 최종 확정(CONFIRMED) 또는 취소(CANCELLED)로 갱신한다.
 */
@Component
public class OrderSagaEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaEventListener.class);

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;

    public OrderSagaEventListener(OrderRepository orderRepository,
                                   ProcessedEventRepository processedEventRepository) {
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
    }

    @KafkaListener(topics = Topics.INVENTORY_FAILED, groupId = "order-service")
    @Transactional
    public void onInventoryFailed(InventoryFailedEvent event) {
        withIdempotency(event, () -> {
            Order order = findOrder(event.orderId());
            order.cancel("재고 부족: " + event.reason());
            log.info("Order {} cancelled (inventory failed: {})", event.orderId(), event.reason());
        });
    }

    @KafkaListener(topics = Topics.PAYMENT_COMPLETED, groupId = "order-service")
    @Transactional
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        withIdempotency(event, () -> {
            Order order = findOrder(event.orderId());
            order.confirm();
            log.info("Order {} confirmed (payment completed)", event.orderId());
        });
    }

    @KafkaListener(topics = Topics.PAYMENT_FAILED, groupId = "order-service")
    @Transactional
    public void onPaymentFailed(PaymentFailedEvent event) {
        withIdempotency(event, () -> {
            Order order = findOrder(event.orderId());
            order.cancel("결제 실패: " + event.reason());
            log.info("Order {} cancelled (payment failed: {})", event.orderId(), event.reason());
        });
    }

    private Order findOrder(String orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new NoSuchElementException("주문을 찾을 수 없습니다: " + orderId));
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
