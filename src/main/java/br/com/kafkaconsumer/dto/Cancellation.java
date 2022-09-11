package br.com.kafkaconsumer.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.Data;

@Data
public class Cancellation {

	private Long cancellationId;
	private String cnpjCpf;
	private BigDecimal cancelAmount;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss")
	private LocalDateTime cancelDate;
	private String cancelStatus;
	
}
