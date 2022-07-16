package com.damon.kafka.springboot.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/msg")
public class MyKafkaController {
    private final static String TOPIC_NAME = "replica-test";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/send")
    public String send() {
        kafkaTemplate.send(TOPIC_NAME, 0, "key", "this is a msg!");
        return "send success";
    }
}
