package com.example.task.controller;

import com.example.task.IntegrationTestBase;
import com.example.task.dto.TaskRequestDto;
import com.example.task.dto.TaskResponseDto;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TaskControllerTest extends IntegrationTestBase {
    HttpClient httpClient = HttpClient.newHttpClient();

    @Autowired
    KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @LocalServerPort
    int port;

    @Value("${tasks.topic}")
    String taskTopic;

    @Test
    @SneakyThrows
    void returnsTaskByEventIdAfterKafkaRegistration() {
        var request = new TaskRequestDto(UUID.randomUUID(), "controller integration task", 250L);
        kafkaTemplate.send(taskTopic, request.eventId(), request).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            var response = get(request.eventId().toString());

            assertEquals(200, response.statusCode());
            var task = objectMapper.readValue(response.body(), TaskResponseDto.class);
            assertEquals(request.eventId(), task.eventId());
            assertEquals(request.name(), task.name());
            assertEquals(request.duration(), task.duration());
            assertNotNull(task.createdAt());
        });
    }

    @Test
    @SneakyThrows
    void rejectsMalformedEventId() {
        var response = get("not-a-uuid");
        var error = objectMapper.readValue(response.body(), new TypeReference<Map<String, Object>>() {
        });

        assertEquals(400, response.statusCode());
        assertEquals("Task identifier has an invalid value", error.get("error"));
    }

    @Test
    @SneakyThrows
    void returnsNotFoundForUnknownEventId() {
        var response = get(UUID.randomUUID().toString());

        assertEquals(404, response.statusCode());
    }

    @SneakyThrows
    HttpResponse<String> get(String eventId) {
        var uri = UriComponentsBuilder.newInstance()
                .scheme("http")
                .host("127.0.0.1")
                .port(port)
                .pathSegment("api", "tasks", eventId)
                .build()
                .toUri();
        var request = HttpRequest.newBuilder(uri).GET().build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
