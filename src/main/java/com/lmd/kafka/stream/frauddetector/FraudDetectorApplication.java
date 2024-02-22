package com.lmd.kafka.stream.frauddetector;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class FraudDetectorApplication {

	public static void main(String[] args) {
		SpringApplication.run(FraudDetectorApplication.class, args);
	}

	@Bean
	NewTopic fraudDetected() {
		return new NewTopic("credit-card-transactions", 12, (short) 1);
	}

}
