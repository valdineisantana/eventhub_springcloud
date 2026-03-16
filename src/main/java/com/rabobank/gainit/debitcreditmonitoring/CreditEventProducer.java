package com.rabobank.gainit.debitcreditmonitoring;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class CreditEventProducer {

    private static final String EVENT_HUB_NAME = "debit-credit-monitoring-credit-process";

    private final EventHubProducerClient producer;

    public CreditEventProducer(@Value("${spring.cloud.azure.eventhubs.connection-string}") String connectionString) {
        this.producer = new EventHubClientBuilder()
                .connectionString(connectionString, EVENT_HUB_NAME)
                .buildProducerClient();
    }

    public void send(String payload) {
        EventDataBatch batch = producer.createBatch();
        try {
            batch.tryAdd(new EventData(payload));
            producer.send(batch);
            log.info("Sent credit event to {}: {}", EVENT_HUB_NAME, payload);
        } catch (Exception e) {
            log.error("Failed to send credit event", e);
        }
    }

    @PreDestroy
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
