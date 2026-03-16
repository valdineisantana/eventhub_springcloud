package com.rabobank.gainit.debitcreditmonitoring.consumer;

import com.azure.spring.messaging.checkpoint.Checkpointer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CreditEventHandler {

    private final SequentialProcessingService sequentialProcessingService;

    public CreditEventHandler(SequentialProcessingService sequentialProcessingService) {
        this.sequentialProcessingService = sequentialProcessingService;
    }

    public void process(String payload, Checkpointer checkpointer) {
        log.info("Received credit process event: {}", payload);

        // Always checkpoint the message to avoid losing it
        if (checkpointer != null) {
            checkpointer.success()
                    .doOnSuccess(success -> log.info("Credit event checkpointed: {}", payload))
                    .doOnError(error -> log.error("Error checkpointing credit event", error))
                    .block();
        } else {
            log.warn("Checkpointer not found for credit event: {}", payload);
        }

        // Check if main queue is drained before processing business logic
        if (!sequentialProcessingService.canProcessSecondaryQueues()) {
            log.warn("Main queue (debit-credit-events) not yet drained. Credit event '{}' received but business logic deferred.", payload);
            return;
        }

        // Business logic here - only executed when main queue is drained
        log.info("Processing credit process event business logic: {}", payload);
        // Add business logic here, e.g., parse payload, validate credit rules, etc.
    }
}