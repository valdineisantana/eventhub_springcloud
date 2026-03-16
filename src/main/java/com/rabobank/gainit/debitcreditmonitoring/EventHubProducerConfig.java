package com.rabobank.gainit.debitcreditmonitoring;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventHubProducerConfig {

    @Bean("creditEventProducer")
    public EventHubProducer creditEventProducer(
            @Value("${spring.cloud.azure.eventhubs.connection-string}") String connectionString,
            @Value("${spring.cloud.stream.bindings.monitorCredit-in-0.destination}") String creditHub) {
        return new EventHubProducer(connectionString, creditHub);
    }

    @Bean("debitEventProducer")
    public EventHubProducer debitEventProducer(
            @Value("${spring.cloud.azure.eventhubs.connection-string}") String connectionString,
            @Value("${spring.cloud.stream.bindings.monitorDebit-in-0.destination}") String debitHub) {
        return new EventHubProducer(connectionString, debitHub);
    }
}
