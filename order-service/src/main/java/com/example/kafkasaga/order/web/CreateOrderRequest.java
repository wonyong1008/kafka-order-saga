package com.example.kafkasaga.order.web;

public record CreateOrderRequest(String productId, int quantity) {
}
