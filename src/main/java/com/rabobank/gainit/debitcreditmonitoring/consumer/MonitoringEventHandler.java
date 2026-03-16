package com.rabobank.gainit.debitcreditmonitoring.consumer;

import com.azure.spring.messaging.checkpoint.Checkpointer;
import com.rabobank.gainit.debitcreditmonitoring.producer.CreditEventProducer;
import com.rabobank.gainit.debitcreditmonitoring.producer.DebitEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MonitoringEventHandler {

    private final SequentialProcessingController sequentialController;
    private final CreditEventProducer creditEventProducer;
    private final DebitEventProducer debitEventProducer;

    public MonitoringEventHandler(SequentialProcessingController sequentialController,
                                CreditEventProducer creditEventProducer,
                                DebitEventProducer debitEventProducer) {
        this.sequentialController = sequentialController;
        this.creditEventProducer = creditEventProducer;
        this.debitEventProducer = debitEventProducer;
    }

    public void process(String payload, Checkpointer checkpointer) {
        log.info("Processing monitoring event from debit-credit-monitoring: {}", payload);

        // Notify that main queue has activity - this will pause secondary queues
        sequentialController.onMainQueueMessage();

        // Route message based on content
        routeMessageByContent(payload);

        if (checkpointer != null) {
            log.info("Starting manual checkpoint for monitoring event: {}", payload);
            checkpointer.success()
                    .doOnSuccess(success -> log.info("Monitoring event '{}' successfully checkpointed", payload))
                    .doOnError(error -> log.error("Error checkpointing monitoring event", error))
                    .block();
        } else {
            log.warn("Checkpointer not found for monitoring event: {}", payload);
        }
    }

    /**
     * Routes the message to the appropriate queue based on its content.
     * If content contains 'CREDIT' -> send to credit queue
     * If content contains 'DEBIT' -> send to debit queue
     */
    private void routeMessageByContent(String payload) {
        if (payload == null || payload.trim().isEmpty()) {
            log.warn("Received empty or null payload, skipping routing");
            return;
        }

        String upperPayload = payload.toUpperCase().trim();

        if (upperPayload.contains("CREDIT")) {
            log.info("Routing CREDIT message to credit queue: {}", payload);
            try {
                creditEventProducer.send(payload);
                log.info("Successfully routed credit message to credit queue");
            } catch (Exception e) {
                log.error("Failed to route credit message to credit queue: {}", e.getMessage(), e);
            }
        } else if (upperPayload.contains("DEBIT")) {
            log.info("Routing DEBIT message to debit queue: {}", payload);
            try {
                debitEventProducer.send(payload);
                log.info("Successfully routed debit message to debit queue");
            } catch (Exception e) {
                log.error("Failed to route debit message to debit queue: {}", e.getMessage(), e);
            }
        } else {
            log.warn("Message does not contain 'CREDIT' or 'DEBIT' keyword, skipping routing: {}", payload);
        }
    }
}