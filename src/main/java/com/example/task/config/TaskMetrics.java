package com.example.task.config;

import com.example.task.repository.TaskRepository;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;

import static com.example.task.model.TaskStatus.NEW;

@Component
@RequiredArgsConstructor
public class TaskMetrics {
    private final MeterRegistry registry;
    private final TaskRepository taskRepository;

    @PostConstruct
    void registerMetrics() {
        registry.gauge("tasks.ready", taskRepository, repository ->
                repository.countByStatusAndNextRetryAtLessThanEqual(NEW, Instant.now()));
    }
}
