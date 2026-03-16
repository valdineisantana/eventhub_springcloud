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
        // Check if main queue is drained before processing credit events
        if (!sequentialProcessingService.canProcessSecondaryQueues()) {
            log.warn("Skipping credit process event - main queue (debit-credit-events) not yet drained: {}", payload);
            return;
        }

        log.info("Processing credit process event: {}", payload);
        // Add business logic here, e.g., parse payload, validate credit rules, etc.

        if (checkpointer != null) {
            log.info("Starting manual checkpoint for credit event: {}", payload);
            checkpointer.success()
                    .doOnSuccess(success -> log.info("Credit event '{}' successfully checkpointed", payload))
                    .doOnError(error -> log.error("Error checkpointing credit event", error))
                    .block();
        } else {
            log.warn("Checkpointer not found for credit event: {}", payload);
        }
    }
}