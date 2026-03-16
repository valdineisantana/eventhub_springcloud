package com.rabobank.gainit.debitcreditmonitoring.consumer;

import com.azure.spring.messaging.checkpoint.Checkpointer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DebitEventHandler {

    private final SequentialProcessingService sequentialProcessingService;

    public DebitEventHandler(SequentialProcessingService sequentialProcessingService) {
        this.sequentialProcessingService = sequentialProcessingService;
    }

    public void process(String payload, Checkpointer checkpointer) {
        // Check if main queue is drained before processing debit events
        if (!sequentialProcessingService.canProcessSecondaryQueues()) {
            log.warn("Skipping debit process event - main queue (debit-credit-events) not yet drained: {}", payload);
            return;
        }

        log.info("Processing debit process event: {}", payload);
        // Add business logic here, e.g., parse payload, validate debit rules, etc.

        if (checkpointer != null) {
            log.info("Starting manual checkpoint for debit event: {}", payload);
            checkpointer.success()
                    .doOnSuccess(success -> log.info("Debit event '{}' successfully checkpointed", payload))
                    .doOnError(error -> log.error("Error checkpointing debit event", error))
                    .block();
        } else {
            log.warn("Checkpointer not found for debit event: {}", payload);
        }
    }
}