package br.com.kafkaconsumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import br.com.kafkaconsumer.dto.Cancellation;
import br.com.kafkaconsumer.dto.Message;
import lombok.extern.log4j.Log4j2;

@Component
@Log4j2
@KafkaListener(id = "multiGroup",
			   groupId = "groupMulti",
			   topics = {"${KAFKA_TOPIC_TEST}", "${KAFKA_MESSAGE_TEST}"})
public class KafkaMultiTopicsConsumerService {

	@KafkaHandler
	public void consumerCancellation(
			@Payload Cancellation cancellation,
			@Header(KafkaHeaders.NATIVE_HEADERS) ConsumerRecord<String, Cancellation> consumerRecords,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partitionId) {
		log.info("document: {}, partitionId: {}, nativeHeaders: {}",
				cancellation.getCnpjCpf(), partitionId, consumerRecords);
		consumerRecords.headers().forEach(header -> {
			log.info("key:{} - value:{}", header.key(), new String(header.value()));
		});
	}
	
	@KafkaHandler
	public void consumerMessage(Message message) {
		log.info("Listener multigroup - mensagem: {}", message.getMessage());
	}
}
