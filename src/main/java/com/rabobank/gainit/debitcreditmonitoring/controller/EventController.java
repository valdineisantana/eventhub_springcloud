package com.rabobank.gainit.debitcreditmonitoring.controller;

import com.rabobank.gainit.debitcreditmonitoring.consumer.SequentialProcessingService;
import com.rabobank.gainit.debitcreditmonitoring.producer.EventHubProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
public class EventController {

    private final StreamBridge streamBridge;
    private final EventHubProducer creditEventProducer;
    private final EventHubProducer debitEventProducer;
    private final SequentialProcessingService sequentialProcessingService;

    @PostMapping("/send-event")
    public String sendEvent(@RequestBody String event) {
        streamBridge.send("monitorEvents-in-0", event);
        return "Event sent: " + event;
    }

    @PostMapping("/send-credit-event")
    public String sendCreditEvent(@RequestBody String event) {
        creditEventProducer.send(event);
        return "Credit event sent: " + event;
    }

    @PostMapping("/send-debit-event")
    public String sendDebitEvent(@RequestBody String event) {
        debitEventProducer.send(event);
        return "Debit event sent: " + event;
    }

    @PostMapping("/processing/force-drain")
    public String forceMainQueueDrained() {
        sequentialProcessingService.forceMainQueueDrained();
        return "Main queue marked as drained. Secondary queues can now be processed.";
    }

    @PostMapping("/processing/reset")
    public String resetProcessing() {
        sequentialProcessingService.reset();
        return "Sequential processing state reset. Main queue processing active.";
    }

    @GetMapping("/processing/status")
    public String getProcessingStatus() {
        boolean canProcessSecondary = sequentialProcessingService.canProcessSecondaryQueues();
        return String.format("Main queue drained: %s. Secondary queues can be processed: %s",
                           canProcessSecondary, canProcessSecondary);
    }
}