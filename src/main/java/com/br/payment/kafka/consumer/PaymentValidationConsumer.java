package com.br.payment.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.br.payment.service.PaymentService;
import com.br.payment.utils.JsonUtil;

@Component
public class PaymentValidationConsumer {

	private Logger logger = LoggerFactory.getLogger(PaymentValidationConsumer.class);
	
	@Autowired
	private PaymentService paymentService;

	@Autowired
	private JsonUtil jsonUtil;
	
	@KafkaListener(
			groupId = "${spring.kafka.consumer.group-id}", 
			topics = "${spring.kafka.topic.payment-success}"
	)
	public void consumeSuccessEvent(String payload) {
		
		this.logger.info("Receiving success event {} from notify payment-success topic", payload);
		
		var event = jsonUtil.toEvent(payload);
		
		this.logger.info(event.toString());
		
		this.paymentService.trySuccessEvent(event);
	}
	
	@KafkaListener(
			groupId = "${spring.kafka.consumer.group-id}", 
			topics = "${spring.kafka.topic.payment-fail}"
	)
	public void consumeFailEvent(String payload) {
		
		this.logger.info("Receiving rollback event {} from notify payment-fail topic", payload);
		
		var event = jsonUtil.toEvent(payload);
		
		this.logger.info(event.toString());
		
		this.paymentService.rollbackEvent(event);
	}
	
	
 

}
