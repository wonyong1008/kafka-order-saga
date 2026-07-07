package com.example.kafkasaga.order.service;

import com.example.kafkasaga.events.OrderCreatedEvent;
import com.example.kafkasaga.order.domain.Order;
import com.example.kafkasaga.order.kafka.OrderEventProducer;
import com.example.kafkasaga.order.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.UUID;

@Service
public class OrderService {

    /** 데모용 고정 단가(원). */
    private static final long UNIT_PRICE = 10_000L;

    private final OrderRepository orderRepository;
    private final OrderEventProducer orderEventProducer;

    public OrderService(OrderRepository orderRepository, OrderEventProducer orderEventProducer) {
        this.orderRepository = orderRepository;
        this.orderEventProducer = orderEventProducer;
    }

    @Transactional
    public Order createOrder(String productId, int quantity) {
        String orderId = UUID.randomUUID().toString();
        long amount = UNIT_PRICE * quantity;

        Order order = new Order(orderId, productId, quantity, amount);
        orderRepository.save(order);

        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID().toString(), orderId, productId, quantity, amount, Instant.now());
        orderEventProducer.publishOrderCreated(event);

        return order;
    }

    public Order getOrder(String orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new NoSuchElementException("주문을 찾을 수 없습니다: " + orderId));
    }
}
