package com.rabobank.gainit.debitcreditmonitoring.consumer;

import com.azure.spring.messaging.AzureHeaders;
import com.azure.spring.messaging.checkpoint.Checkpointer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

@Configuration
@Slf4j
public class EventMonitoringConfig {

    @Bean
    public Consumer<Message<String>> monitoring() {
        return message -> {
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
        };
    }

    @Bean
    public Consumer<Message<String>> monitorCredit() {
        return message -> {
            log.info("Received credit process event: {}", message.getPayload());
            log.info("Credit event headers - Partition Key: {}, Sequence Number: {}, Offset: {}, Enqueued Time: {}",
                     message.getHeaders().get(AzureHeaders.PARTITION_KEY),
                     message.getHeaders().get("x-opt-sequence-number"),
                     message.getHeaders().get("x-opt-offset"),
                     message.getHeaders().get("x-opt-enqueued-time"));
            Checkpointer checkpointer = (Checkpointer) message.getHeaders().get(AzureHeaders.CHECKPOINTER);
            if (checkpointer != null) {
                log.info("Starting manual checkpoint for credit event: {}", message.getPayload());
                checkpointer.success()
                        .doOnSuccess(success -> log.info("Credit event '{}' successfully checkpointed", message.getPayload()))
                        .doOnError(error -> log.error("Error checkpointing credit event", error))
                        .block();
            } else {
                log.warn("Checkpointer not found in message headers for credit event: {}", message.getPayload());
            }
        };
    }

    @Bean
    public Consumer<Message<String>> monitorDebit() {
        return message -> {
            log.info("Received debit process event: {}", message.getPayload());
            log.info("Debit event headers - Partition Key: {}, Sequence Number: {}, Offset: {}, Enqueued Time: {}",
                     message.getHeaders().get(AzureHeaders.PARTITION_KEY),
                     message.getHeaders().get("x-opt-sequence-number"),
                     message.getHeaders().get("x-opt-offset"),
                     message.getHeaders().get("x-opt-enqueued-time"));
            Checkpointer checkpointer = (Checkpointer) message.getHeaders().get(AzureHeaders.CHECKPOINTER);
            if (checkpointer != null) {
                log.info("Starting manual checkpoint for debit event: {}", message.getPayload());
                checkpointer.success()
                        .doOnSuccess(success -> log.info("Debit event '{}' successfully checkpointed", message.getPayload()))
                        .doOnError(error -> log.error("Error checkpointing debit event", error))
                        .block();
            } else {
                log.warn("Checkpointer not found in message headers for debit event: {}", message.getPayload());
            }
        };
    }
}