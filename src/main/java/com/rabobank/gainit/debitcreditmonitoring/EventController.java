package com.rabobank.gainit.debitcreditmonitoring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EventController {

    @Autowired
    private StreamBridge streamBridge;

    @PostMapping("/send-event")
    public String sendEvent(@RequestBody String event) {
        streamBridge.send("monitorEvents-in-0", event);
        return "Event sent: " + event;
    }
}