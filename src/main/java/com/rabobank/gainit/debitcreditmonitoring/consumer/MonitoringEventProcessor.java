package com.rabobank.gainit.debitcreditmonitoring.consumer;

import com.azure.spring.messaging.AzureHeaders;
import com.azure.spring.messaging.checkpoint.Checkpointer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

@Component
@Slf4j
public class MonitoringEventProcessor implements Consumer<Message<String>> {

    @Override
    public void accept(Message<String> message) {
        log.info("Received monitoring event from debit-credit-monitoring: {}", message.getPayload());
        log.info("Monitoring event headers - Partition Key: {}, Sequence Number: {}, Offset: {}, Enqueued Time: {}",
                 message.getHeaders().get(AzureHeaders.PARTITION_KEY),
                 message.getHeaders().get("x-opt-sequence-number"),
                 message.getHeaders().get("x-opt-offset"),
                 message.getHeaders().get("x-opt-enqueued-time"));

        Checkpointer checkpointer = (Checkpointer) message.getHeaders().get(AzureHeaders.CHECKPOINTER);
        if (checkpointer != null) {
            log.info("Starting manual checkpoint for monitoring event: {}", message.getPayload());
            checkpointer.success()
                    .doOnSuccess(success -> log.info("Monitoring event '{}' successfully checkpointed", message.getPayload()))
                    .doOnError(error -> log.error("Error checkpointing monitoring event", error))
                    .block();
        } else {
            log.warn("Checkpointer not found in message headers for monitoring event: {}", message.getPayload());
        }
    }
}
