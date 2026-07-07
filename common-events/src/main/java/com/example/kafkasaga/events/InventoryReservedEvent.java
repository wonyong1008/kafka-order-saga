package com.example.kafkasaga.events;

import java.time.Instant;

public record InventoryReservedEvent(
        String eventId,
        String orderId,
        String productId,
        int quantity,
        long amount,
        Instant occurredAt
) implements DomainEvent {
}
