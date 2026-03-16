package com.rabobank.gainit.debitcreditmonitoring.consumer;

import com.azure.spring.messaging.checkpoint.Checkpointer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DebitEventHandler {

    private final SequentialProcessingController sequentialController;

    public DebitEventHandler(SequentialProcessingController sequentialController) {
        this.sequentialController = sequentialController;
    }

    public void process(String payload, Checkpointer checkpointer) {
        log.info("Processing debit process event: {}", payload);

        // Notify that debit queue has activity
        sequentialController.onDebitQueueMessage();

        // Business logic here - binding control ensures this only runs when appropriate
        // Add business logic here, e.g., parse payload, validate debit rules, etc.

        if (checkpointer != null) {
            checkpointer.success()
                    .doOnSuccess(success -> log.info("Debit event '{}' successfully checkpointed", payload))
                    .doOnError(error -> log.error("Error checkpointing debit event", error))
                    .block();
        } else {
            log.warn("Checkpointer not found for debit event: {}", payload);
        }
    }
}