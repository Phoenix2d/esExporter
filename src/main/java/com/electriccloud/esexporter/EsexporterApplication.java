package com.electriccloud.esexporter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
public class EsexporterApplication {

	public static void main(String[] args) {
		SpringApplication.run(EsexporterApplication.class, args).close();
		System.exit(0);
	}
}
