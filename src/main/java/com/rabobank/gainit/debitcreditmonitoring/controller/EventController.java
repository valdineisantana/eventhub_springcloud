package com.rabobank.gainit.debitcreditmonitoring.controller;

import com.rabobank.gainit.debitcreditmonitoring.consumer.SequentialProcessingController;
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
    private final SequentialProcessingController sequentialController;

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

    @PostMapping("/processing/force-resume-all")
    public String forceResumeAllSecondaryQueues() {
        sequentialController.forceResumeAllSecondaryQueues();
        return "All secondary queues manually resumed for processing.";
    }

    @PostMapping("/processing/force-pause-all")
    public String forcePauseAllSecondaryQueues() {
        sequentialController.forcePauseAllSecondaryQueues();
        return "All secondary queues manually paused.";
    }

    @PostMapping("/processing/force-resume-debit")
    public String forceResumeDebitQueue() {
        sequentialController.forceResumeDebitQueue();
        return "Debit queue manually resumed for processing.";
    }

    @GetMapping("/processing/status")
    public SequentialProcessingController.ProcessingStatus getProcessingStatus() {
        return sequentialController.getStatus();
    }
}