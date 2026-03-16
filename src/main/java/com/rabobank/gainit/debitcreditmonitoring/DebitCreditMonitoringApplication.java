package com.rabobank.gainit.debitcreditmonitoring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DebitCreditMonitoringApplication {

    public static void main(String[] args) {
        SpringApplication.run(DebitCreditMonitoringApplication.class, args);
    }

}