package com.example.kafkasaga.events;

import java.time.Instant;

public record PaymentFailedEvent(
        String eventId,
        String orderId,
        String productId,
        int quantity,
        long amount,
        String reason,
        Instant occurredAt
) implements DomainEvent {
}
