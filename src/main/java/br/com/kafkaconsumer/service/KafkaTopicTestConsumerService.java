package br.com.kafkaconsumer.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import br.com.kafkaconsumer.dto.Cancellation;
import br.com.kafkaconsumer.dto.Message;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Service
public class KafkaTopicTestConsumerService {
	
	@KafkaListener(topics = "${KAFKA_TOPIC_TEST}", 
				   groupId = "group1",
				   containerFactory = "kafkaListener")
	public void consumer(Cancellation cancellation) {
		log.info("document: {}", cancellation.getCnpjCpf());
	}
	
	@KafkaListener(topics = "${KAFKA_MESSAGE_TEST}",
			       groupId = "group2",
			       containerFactory = "kafkaListenerMessage")
	public void consumer2(Message message) {
		log.info("mensagem: {}", message.getMessage());
	}

}
