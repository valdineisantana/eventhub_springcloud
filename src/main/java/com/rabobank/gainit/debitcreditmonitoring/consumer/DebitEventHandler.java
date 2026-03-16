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
        log.info("Received debit process event: {}", payload);

        // Always checkpoint the message to avoid losing it
        if (checkpointer != null) {
            checkpointer.success()
                    .doOnSuccess(success -> log.info("Debit event checkpointed: {}", payload))
                    .doOnError(error -> log.error("Error checkpointing debit event", error))
                    .block();
        } else {
            log.warn("Checkpointer not found for debit event: {}", payload);
        }

        // Check if main queue is drained before processing business logic
        if (!sequentialProcessingService.canProcessSecondaryQueues()) {
            log.warn("Main queue (debit-credit-events) not yet drained. Debit event '{}' received but business logic deferred.", payload);
            return;
        }

        // Business logic here - only executed when main queue is drained
        log.info("Processing debit process event business logic: {}", payload);
        // Add business logic here, e.g., parse payload, validate debit rules, etc.
    }
}