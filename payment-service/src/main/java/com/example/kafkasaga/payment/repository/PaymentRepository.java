package com.example.kafkasaga.payment.repository;

import com.example.kafkasaga.payment.domain.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment, String> {
}
