package com.example.kafkasaga.payment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/** 결제 처리 이력(원장). 실제 PG 연동 대신 결정론적 규칙으로 성공/실패를 시뮬레이션한다. */
@Entity
@Table(name = "payments")
public class Payment {

    @Id
    @Column(length = 36)
    private String orderId;

    @Column(nullable = false)
    private long amount;

    @Column(nullable = false)
    private boolean succeeded;

    @Column(nullable = false)
    private String reason;

    @Column(nullable = false)
    private Instant processedAt;

    protected Payment() {
    }

    public Payment(String orderId, long amount, boolean succeeded, String reason) {
        this.orderId = orderId;
        this.amount = amount;
        this.succeeded = succeeded;
        this.reason = reason;
        this.processedAt = Instant.now();
    }

    public String getOrderId() {
        return orderId;
    }

    public long getAmount() {
        return amount;
    }

    public boolean isSucceeded() {
        return succeeded;
    }

    public String getReason() {
        return reason;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
