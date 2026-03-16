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
    public Consumer<Message<String>> monitoring(MonitoringEventHandler handler) {
        return message -> {
            log.info("Received monitoring event from debit-credit-monitoring: {}", message.getPayload());
            log.info("Monitoring event headers - Partition Key: {}, Sequence Number: {}, Offset: {}, Enqueued Time: {}",
                     message.getHeaders().get(AzureHeaders.PARTITION_KEY),
                     message.getHeaders().get("x-opt-sequence-number"),
                     message.getHeaders().get("x-opt-offset"),
                     message.getHeaders().get("x-opt-enqueued-time"));
            Checkpointer checkpointer = (Checkpointer) message.getHeaders().get(AzureHeaders.CHECKPOINTER);
            handler.process(message.getPayload(), checkpointer);
        };
    }

    @Bean
    public Consumer<Message<String>> monitorCredit(CreditEventHandler handler) {
        return message -> {
            log.info("Received credit process event: {}", message.getPayload());
            log.info("Credit event headers - Partition Key: {}, Sequence Number: {}, Offset: {}, Enqueued Time: {}",
                     message.getHeaders().get(AzureHeaders.PARTITION_KEY),
                     message.getHeaders().get("x-opt-sequence-number"),
                     message.getHeaders().get("x-opt-offset"),
                     message.getHeaders().get("x-opt-enqueued-time"));
            Checkpointer checkpointer = (Checkpointer) message.getHeaders().get(AzureHeaders.CHECKPOINTER);
            handler.process(message.getPayload(), checkpointer);
        };
    }

    @Bean
    public Consumer<Message<String>> monitorDebit(DebitEventHandler handler) {
        return message -> {
            log.info("Received debit process event: {}", message.getPayload());
            log.info("Debit event headers - Partition Key: {}, Sequence Number: {}, Offset: {}, Enqueued Time: {}",
                     message.getHeaders().get(AzureHeaders.PARTITION_KEY),
                     message.getHeaders().get("x-opt-sequence-number"),
                     message.getHeaders().get("x-opt-offset"),
                     message.getHeaders().get("x-opt-enqueued-time"));
            Checkpointer checkpointer = (Checkpointer) message.getHeaders().get(AzureHeaders.CHECKPOINTER);
            handler.process(message.getPayload(), checkpointer);
        };
    }
}