package br.com.kafkaconsumer.dto;

import lombok.Data;

@Data
public class Message {
	
	private Long messageId;
	private String message;

}
