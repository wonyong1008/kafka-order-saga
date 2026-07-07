package com.example.kafkasaga.events;

import java.time.Instant;

public interface DomainEvent {

    String eventId();

    String orderId();

    Instant occurredAt();
}
