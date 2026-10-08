package com.example.task.service;

import com.example.task.IntegrationTestBase;
import com.example.task.dto.TaskRequestDto;
import com.example.task.model.TaskStatus;
import com.example.task.repository.TaskRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class TaskExecutionServiceTest extends IntegrationTestBase {

    @Autowired
    KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    TaskRepository taskRepository;

    @Autowired
    MeterRegistry metrics;

    @MockitoSpyBean
    TaskUpdateService updateService;

    @Value("${tasks.topic}")
    String taskTopic;

    @Value("${worker.progress-step}")
    Duration progressStep;

    @Test
    @SneakyThrows
    void completesTasksPublishedToKafka() {
        var requests = List.of(
                new TaskRequestDto(UUID.randomUUID(), "one-second task", 1_000L),
                new TaskRequestDto(UUID.randomUUID(), "two-second task", 2_000L),
                new TaskRequestDto(UUID.randomUUID(), "three-second task", 3_000L)
        );

        for (var request : requests) {
            kafkaTemplate.send(taskTopic, request.eventId(), request).get(10, TimeUnit.SECONDS);
        }

        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            for (var request : requests) {
                var task = taskRepository.findById(request.eventId()).orElse(null);
                assertNotNull(task, "Kafka message has not been registered yet: " + request.eventId());
                assertEquals(TaskStatus.COMPLETED, task.getStatus());
                assertEquals(100, task.getProgress());
            }
        });
    }

    @Test
    @SneakyThrows
    void persistsProgressBeforeCompletingTask() {
        var request = new TaskRequestDto(UUID.randomUUID(), "progress before completion", progressStep.toMillis() * 2);
        var beforeCompletion = new CountDownLatch(1);
        var resume = new CountDownLatch(1);

        doAnswer(invocation -> {
            beforeCompletion.countDown();
            awaitLatch(resume);
            return invocation.callRealMethod();
        }).when(updateService).complete(eq(request.eventId()), any(UUID.class));

        try {
            kafkaTemplate.send(taskTopic, request.eventId(), request).get(10, TimeUnit.SECONDS);
            awaitLatch(beforeCompletion);

            var task = taskRepository.findById(request.eventId()).orElseThrow();
            assertEquals(TaskStatus.IN_PROGRESS, task.getStatus());
            assertEquals(50, task.getProgress());
            verify(updateService).saveProgress(task.getEventId(), task.getExecutionId(), 50);
        } finally {
            resume.countDown();
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var task = taskRepository.findById(request.eventId()).orElseThrow();
            assertEquals(TaskStatus.COMPLETED, task.getStatus());
            assertEquals(100, task.getProgress());
        });
    }

    @Test
    @SneakyThrows
    void stopsWithoutCompletingWhenExecutionOwnershipChanges() {
        var request = new TaskRequestDto(UUID.randomUUID(), "execution ownership changes", progressStep.toMillis() * 2);
        var beforeProgressUpdate = new CountDownLatch(1);
        var resume = new CountDownLatch(1);
        var leaseLost = metrics.counter("tasks.outcomes", "status", "lease_lost");
        double initialLeaseLost = leaseLost.count();
        UUID newExecutionId = UUID.randomUUID();

        doAnswer(invocation -> {
            beforeProgressUpdate.countDown();
            awaitLatch(resume);
            return invocation.callRealMethod();
        }).when(updateService).saveProgress(eq(request.eventId()), any(UUID.class), eq(50));

        try {
            kafkaTemplate.send(taskTopic, request.eventId(), request).get(10, TimeUnit.SECONDS);
            awaitLatch(beforeProgressUpdate);
            var current = taskRepository.findById(request.eventId()).orElseThrow();
            current.setExecutionId(newExecutionId);
            taskRepository.save(current);
        } finally {
            resume.countDown();
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> leaseLost.count() > initialLeaseLost);

        var current = taskRepository.findById(request.eventId()).orElseThrow();
        assertEquals(newExecutionId, current.getExecutionId());
        assertEquals(TaskStatus.IN_PROGRESS, current.getStatus());
        assertEquals(0, current.getProgress());
        verify(updateService, never()).complete(eq(request.eventId()), any(UUID.class));
    }
}
