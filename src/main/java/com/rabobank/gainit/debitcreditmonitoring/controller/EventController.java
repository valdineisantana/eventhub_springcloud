package com.rabobank.gainit.debitcreditmonitoring.controller;

import com.rabobank.gainit.debitcreditmonitoring.producer.EventHubProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class EventController {

    private final StreamBridge streamBridge;
    private final EventHubProducer creditEventProducer;
    private final EventHubProducer debitEventProducer;

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
}