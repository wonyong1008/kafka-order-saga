package com.example.kafkasaga.order.web;

import com.example.kafkasaga.order.domain.Order;
import com.example.kafkasaga.order.domain.OrderStatus;

import java.time.Instant;

public record OrderResponse(
        String orderId,
        String productId,
        int quantity,
        long amount,
        OrderStatus status,
        String statusReason,
        Instant createdAt,
        Instant updatedAt
) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getProductId(),
                order.getQuantity(),
                order.getAmount(),
                order.getStatus(),
                order.getStatusReason(),
                order.getCreatedAt(),
                order.getUpdatedAt());
    }
}
