package com.rabobank.gainit.debitcreditmonitoring.consumer;

import com.azure.spring.messaging.checkpoint.Checkpointer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CreditEventHandler {

    public void process(String payload, Checkpointer checkpointer) {
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