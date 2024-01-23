package com.br.payment.service;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.br.payment.dtos.Event;
import com.br.payment.dtos.History;
import com.br.payment.dtos.OrderProducts;
import com.br.payment.enums.PaymentStatusEnum;
import com.br.payment.enums.SagaStatusEnum;
import com.br.payment.kafka.producer.OrchestratorProducer;
import com.br.payment.model.Payment;
import com.br.payment.repository.PaymentRepository;
import com.br.payment.utils.JsonUtil;

import jakarta.validation.ValidationException;

@Service
public class PaymentService {

	private Logger logger = LoggerFactory.getLogger(PaymentService.class);

	private static final String CURRENT_SOURCE = "PAYMENT_SERVICE";

	@Autowired
	private JsonUtil jsonUtil;

	@Autowired
	private PaymentRepository paymentRepository;

	@Autowired
	private OrchestratorProducer orchestratorProducer;

	public void trySuccessEvent(Event event) {

		try {
			this.checkCurrentValidation(event);
			this.createPendingPayment(event);
			var payment = this.findByOrderIdAndTransactionId(event);
			this.validateAmount(payment.getTotalAmount());
			this.changePaymentSuccess(payment);
			this.handleSuccess(event);
		} catch (Exception e) {
			this.logger.error("Error trying make payment!", e);
			this.handleFailCurrentNotExecuted(event, e.getMessage());

		}

		// Envia para o orquestrador a informação de sucesso para atualização do evento
		this.orchestratorProducer.sendEvent(jsonUtil.toJson(event));
	}

	public void rollbackEvent(Event event) {

		event.setStatus(SagaStatusEnum.FAIL);

		event.setSource(CURRENT_SOURCE);

		try {
			this.changePaymentStatusToRefund(event);
			this.addHistory(event, "Rollback executed!");
		} catch (Exception e) {
			addHistory(event, "Rollback not executed for payment: " + e.getMessage());
		}

		this.orchestratorProducer.sendEvent(jsonUtil.toJson(event));

	}

	private void changePaymentStatusToRefund(Event event) {

		var payment = this.findByOrderIdAndTransactionId(event);

		payment.setStatus(PaymentStatusEnum.REFUND);
		setEventAmountItems(event, payment);

		this.paymentRepository.save(payment);

	}

	private void createPendingPayment(Event event) {

		var totalAmount = this.calculateTotalAmount(event);
		var totalItens = this.calculateTotalItens(event);

		var payment = new Payment();

		payment.setOrderId(event.getPayload().getId());
		payment.setTransactionId(event.getTransactionId());
		payment.setTotalAmount(totalAmount);
		payment.setTotalItems(totalItens);

		this.paymentRepository.save(payment);

		this.setEventAmountItems(event, payment);

	}

	private void changePaymentSuccess(Payment payment) {
		payment.setStatus(PaymentStatusEnum.SUCCESS);

		this.paymentRepository.save(payment);
	}

	private int calculateTotalItens(Event event) {
		return event.getPayload().getProducts().stream().map(OrderProducts::getQuantity).reduce(0, Integer::sum);
	}

	private double calculateTotalAmount(Event event) {

		return event.getPayload().getProducts().stream().map(p -> p.getQuantity() * p.getProduct().getUnitValue())
				.reduce(0.0, Double::sum);
	}

	private void validateAmount(double amount) {
		if (amount < 0.1) {
			throw new ValidationException("The minimum amount available is 0.1");
		}
	}

	private void setEventAmountItems(Event event, Payment payment) {
		event.getPayload().setTotalAmount(payment.getTotalAmount());
		event.getPayload().setTotalItens(payment.getTotalItems());

	}

	private Payment findByOrderIdAndTransactionId(Event event) {
		return this.paymentRepository
				.findByOrderIdAndTransactionId(event.getPayload().getId(), event.getTransactionId())
				.orElseThrow(() -> new ValidationException("Payment not found by ordeId and transactionId"));
	}

	private void checkCurrentValidation(Event event) {

		// Faz o tratamento para evitar inconsistência de dados referente ao kafka
		// enviar mais de uma
		/// vez a mesma mensagem
		if (this.paymentRepository.existsByOrderIdAndTransactionId(event.getPayload().getId(),
				event.getTransactionId())) {
			throw new ValidationException("There's another transactionId for this validation!");
		}

	}

	private void handleSuccess(Event event) {

		event.setStatus(SagaStatusEnum.SUCCESS);
		event.setSource(CURRENT_SOURCE);

		this.addHistory(event, "Payment success!");

	}

	private void addHistory(Event event, String message) {

		History h = new History();
		h.setCreatedAt(LocalDateTime.now());
		h.setSource(event.getSource());
		h.setStatus(event.getStatus());
		h.setMessage(message);

		event.addToHistory(h);
	}

	private void handleFailCurrentNotExecuted(Event event, String message) {

		event.setStatus(SagaStatusEnum.ROLLBACK_PENDING);
		event.setSource(CURRENT_SOURCE);

		this.addHistory(event, "Failed to realize payment: " + message);

	}

}
