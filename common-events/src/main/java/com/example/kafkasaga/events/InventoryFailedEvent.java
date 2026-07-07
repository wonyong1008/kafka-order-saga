package com.example.kafkasaga.events;

import java.time.Instant;

public record InventoryFailedEvent(
        String eventId,
        String orderId,
        String productId,
        int quantity,
        String reason,
        Instant occurredAt
) implements DomainEvent {
}
