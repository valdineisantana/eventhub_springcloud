package com.rabobank.gainit.debitcreditmonitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class EventMonitoringConfig {

    private static final Logger logger = LoggerFactory.getLogger(EventMonitoringConfig.class);

    @Bean
    public Consumer<String> monitorEvents() {
        return event -> {
            logger.info("Received debit/credit event: {}", event);
            // Add monitoring logic here, e.g., parse event, check thresholds, send alerts, etc.
        };
    }
}