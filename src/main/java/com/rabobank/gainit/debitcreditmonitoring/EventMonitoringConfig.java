package com.rabobank.gainit.debitcreditmonitoring;

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
    public Consumer<Message<String>> monitorEvents() {
        return message -> {
            log.info("Received debit/credit event: {}", message.getPayload());
            Checkpointer checkpointer = (Checkpointer) message.getHeaders().get(AzureHeaders.CHECKPOINTER);
            if (checkpointer != null) {
                checkpointer.success()
                        .doOnSuccess(success -> log.info("Event '{}' successfully checkpointed", message.getPayload()))
                        .doOnError(error -> log.error("Error checkpointing event", error))
                        .block();
            }
        };
    }

    @Bean
    public Consumer<Message<String>> monitorCredit() {
        return message -> {
            log.info("Received credit process event: {}", message.getPayload());
            Checkpointer checkpointer = (Checkpointer) message.getHeaders().get(AzureHeaders.CHECKPOINTER);
            if (checkpointer != null) {
                checkpointer.success()
                        .doOnSuccess(success -> log.info("Credit event '{}' successfully checkpointed", message.getPayload()))
                        .doOnError(error -> log.error("Error checkpointing credit event", error))
                        .block();
            }
        };
    }

    @Bean
    public Consumer<Message<String>> monitorDebit() {
        return message -> {
            log.info("Received debit process event: {}", message.getPayload());
            Checkpointer checkpointer = (Checkpointer) message.getHeaders().get(AzureHeaders.CHECKPOINTER);
            if (checkpointer != null) {
                checkpointer.success()
                        .doOnSuccess(success -> log.info("Debit event '{}' successfully checkpointed", message.getPayload()))
                        .doOnError(error -> log.error("Error checkpointing debit event", error))
                        .block();
            }
        };
    }
}