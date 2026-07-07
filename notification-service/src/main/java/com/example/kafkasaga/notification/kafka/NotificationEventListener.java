package com.example.kafkasaga.notification.kafka;

import com.example.kafkasaga.events.DomainEvent;
import com.example.kafkasaga.events.InventoryFailedEvent;
import com.example.kafkasaga.events.PaymentCompletedEvent;
import com.example.kafkasaga.events.PaymentFailedEvent;
import com.example.kafkasaga.events.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 실제 이메일/SMS/푸시 발송 대신, 알림 내용을 로그로 출력해 발송을 시뮬레이션한다.
 */
@Component
public class NotificationEventListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationEventListener.class);

    private final ProcessedEventRepository processedEventRepository;

    public NotificationEventListener(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    @KafkaListener(topics = Topics.INVENTORY_FAILED, groupId = "notification-service")
    @Transactional
    public void onInventoryFailed(InventoryFailedEvent event) {
        withIdempotency(event, () ->
                sendNotification(event.orderId(), "주문 취소 안내",
                        "재고 부족으로 주문이 취소되었습니다. (" + event.reason() + ")"));
    }

    @KafkaListener(topics = Topics.PAYMENT_COMPLETED, groupId = "notification-service")
    @Transactional
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        withIdempotency(event, () ->
                sendNotification(event.orderId(), "주문 확정 안내",
                        "결제가 완료되어 주문이 확정되었습니다. (" + event.amount() + "원)"));
    }

    @KafkaListener(topics = Topics.PAYMENT_FAILED, groupId = "notification-service")
    @Transactional
    public void onPaymentFailed(PaymentFailedEvent event) {
        withIdempotency(event, () ->
                sendNotification(event.orderId(), "주문 취소 안내",
                        "결제 실패로 주문이 취소되었습니다. (" + event.reason() + ")"));
    }

    private void sendNotification(String orderId, String title, String message) {
        log.info("[알림 발송] orderId={} | {} | {}", orderId, title, message);
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
