package com.br.payment.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.br.payment.model.Payment;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, Long> {

	Boolean existsByOrderIdAndTransactionId(String orderId, String transactionId);

	Optional<Payment> findByOrderIdAndTransactionId(String orderId, String transactionId);

}
