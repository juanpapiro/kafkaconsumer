package br.com.kafkaconsumer.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper.TypePrecedence;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import br.com.kafkaconsumer.dto.Cancellation;
import br.com.kafkaconsumer.dto.Message;

@Configuration
public class KafkaTopicTestConsumerConfig {
	
	@Value("${KAFKA_BOOTSTRAP_SERVERS}")
	private String bootstrapServers;
	
	@Bean
	public ConsumerFactory<String, Cancellation> consumerFactoryCancellation() {
		Map<String, Object> props = propertiesConfig();
		props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Cancellation.class.getName());
		return new DefaultKafkaConsumerFactory<>(props);
	}
	
	@Bean
	public ConsumerFactory<String, Cancellation> consumerFactoryMessage() {
		Map<String, Object> props = propertiesConfig();
		props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Message.class.getName());
		return new DefaultKafkaConsumerFactory<>(props);
	}
	
	@Bean
	public ConsumerFactory<String, Object> multiConsumerFactory() {
		return new DefaultKafkaConsumerFactory<>(propertiesConfigMulti());
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Cancellation> kafkaListener() {
		ConcurrentKafkaListenerContainerFactory<String, Cancellation> factory = 
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactoryCancellation());
		return factory;
	}
	
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Cancellation> kafkaListenerMessage() {
		ConcurrentKafkaListenerContainerFactory<String, Cancellation> factory = 
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactoryMessage());
		return factory;
	}
	
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Object> multiKafkaListener() {
		ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(multiConsumerFactory());
		factory.setMessageConverter(multiTypeConverter());
		return factory;
	}
	
	@Bean
	public RecordMessageConverter multiTypeConverter() {
	    StringJsonMessageConverter converter = new StringJsonMessageConverter();
	    DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
	    typeMapper.setTypePrecedence(TypePrecedence.TYPE_ID);
	    typeMapper.addTrustedPackages("*");
	    Map<String, Class<?>> mappings = new HashMap<>();
	    mappings.put("br.com.kafkaproducer.dto.Cancellation", Cancellation.class);
	    mappings.put("br.com.kafkaproducer.dto.Message", Message.class);
	    typeMapper.setIdClassMapping(mappings);
	    converter.setTypeMapper(typeMapper);
	    return converter;
	}
	
	
	private Map<String, Object> propertiesConfig() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS,false);
		props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		return props;
	}
	
	private Map<String, Object> propertiesConfigMulti() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		return props;
	}
}
