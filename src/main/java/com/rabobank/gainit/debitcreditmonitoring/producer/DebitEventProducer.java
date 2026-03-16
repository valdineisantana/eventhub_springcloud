package com.rabobank.gainit.debitcreditmonitoring.producer;

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
public class DebitEventProducer {

    private final String eventHubName;
    private final EventHubProducerClient producer;

    public DebitEventProducer(
            @Value("${spring.cloud.azure.eventhubs.connection-string}") String connectionString,
            @Value("${spring.cloud.stream.bindings.monitorDebit-in-0.destination}") String eventHubName) {
        this.eventHubName = eventHubName;
        this.producer = new EventHubClientBuilder()
                .connectionString(connectionString, this.eventHubName)
                .buildProducerClient();
    }

    public void send(String payload) {
        EventDataBatch batch = producer.createBatch();
        try {
            batch.tryAdd(new EventData(payload));
            producer.send(batch);
            log.info("Sent debit event to {}: {}", eventHubName, payload);
        } catch (Exception e) {
            log.error("Failed to send debit event", e);
        }
    }

    @PreDestroy
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
