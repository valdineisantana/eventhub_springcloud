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
    public Consumer<Message<String>> monitoring(MonitoringEventProcessor processor) {
        return processor;
    }

    @Bean
    public Consumer<Message<String>> monitorCredit(CreditEventProcessor processor) {
        return processor;
    }

    @Bean
    public Consumer<Message<String>> monitorDebit(DebitEventProcessor processor) {
        return processor;
    }
}