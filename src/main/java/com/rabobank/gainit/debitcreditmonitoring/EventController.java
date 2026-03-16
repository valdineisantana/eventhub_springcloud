package com.rabobank.gainit.debitcreditmonitoring;

import lombok.RequiredArgsConstructor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class EventController {

    private final StreamBridge streamBridge;

    @PostMapping("/send-event")
    public String sendEvent(@RequestBody String event) {
        streamBridge.send("monitorEvents-in-0", event);
        return "Event sent: " + event;
    }
}