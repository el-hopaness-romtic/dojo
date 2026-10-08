package com.example.task;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.kafka.KafkaContainer;

class KafkaTestConfiguration {
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0");

    static synchronized void startIfNotRunning() {
        if (!KAFKA.isRunning()) {
            KAFKA.start();
        }
    }

    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("KAFKA_SERVERS", KAFKA::getBootstrapServers);
    }
}
