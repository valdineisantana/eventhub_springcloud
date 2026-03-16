package com.rabobank.gainit.debitcreditmonitoring;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventHubProducer {

    private final String eventHubName;
    private final EventHubProducerClient producerClient;

    public EventHubProducer(String connectionString, String eventHubName) {
        this.eventHubName = eventHubName;
        this.producerClient = new EventHubClientBuilder()
                .connectionString(connectionString, eventHubName)
                .buildProducerClient();
    }

    public void send(String payload) {
        EventDataBatch batch = producerClient.createBatch();
        try {
            batch.tryAdd(new EventData(payload));
            producerClient.send(batch);
            log.info("Sent event to {}: {}", eventHubName, payload);
        } catch (Exception e) {
            log.error("Failed to send event to {}", eventHubName, e);
        }
    }

    public void close() {
        if (producerClient != null) {
            producerClient.close();
        }
    }
}
