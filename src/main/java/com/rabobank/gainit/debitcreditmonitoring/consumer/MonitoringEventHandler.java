package com.rabobank.gainit.debitcreditmonitoring.consumer;

import com.azure.spring.messaging.checkpoint.Checkpointer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MonitoringEventHandler {

    private final SequentialProcessingService sequentialProcessingService;

    public MonitoringEventHandler(SequentialProcessingService sequentialProcessingService) {
        this.sequentialProcessingService = sequentialProcessingService;
    }

    public void process(String payload, Checkpointer checkpointer) {
        log.info("Processing monitoring event from debit-credit-monitoring: {}", payload);

        // Notify that main queue has activity
        sequentialProcessingService.onMainQueueMessage();

        // Add business logic here, e.g., parse payload, validate, send alerts, etc.

        if (checkpointer != null) {
            log.info("Starting manual checkpoint for monitoring event: {}", payload);
            checkpointer.success()
                    .doOnSuccess(success -> log.info("Monitoring event '{}' successfully checkpointed", payload))
                    .doOnError(error -> log.error("Error checkpointing monitoring event", error))
                    .block();
        } else {
            log.warn("Checkpointer not found for monitoring event: {}", payload);
        }
    }
}