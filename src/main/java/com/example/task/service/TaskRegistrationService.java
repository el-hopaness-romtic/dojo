package com.example.task.service;

import com.example.task.dto.TaskRequestDto;
import com.example.task.model.Task;
import com.example.task.model.TaskStatus;
import com.example.task.repository.TaskRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TaskRegistrationService {
    private final TaskRepository taskRepository;
    @Value("${worker.max-attempts}")
    private final int maxAttempts;

    @PostConstruct
    void validateMaxAttempts() {
        if (maxAttempts < 1) throw new IllegalArgumentException("worker.max-attempts must be positive");
    }

    @Transactional
    public void register(TaskRequestDto request) {
        Instant now = Instant.now();
        String normalizedName = request.name().strip();
        Task task = new Task();
        task.setEventId(request.eventId());
        task.setName(normalizedName);
        task.setDuration(request.duration());
        task.setStatus(TaskStatus.NEW);
        task.setProgress(0);
        task.setAttempts(0);
        task.setMaxAttempts(maxAttempts);
        task.setNextRetryAt(now);
        task.setCreatedAt(now);
        int inserted = taskRepository.insertIfAbsent(task);

        if (inserted == 0) {
            Task existing = taskRepository.findById(request.eventId())
                    .orElseThrow(() -> new IllegalStateException("Событие не найдено после конфликта регистрации"));

            if (!existing.getName().equals(normalizedName)
                || existing.getDuration() != request.duration()) {
                throw new ConflictingTaskException(request.eventId());
            }
        }
    }

    public static class ConflictingTaskException extends RuntimeException {
        public ConflictingTaskException(UUID eventId) {
            super("eventId already exists: " + eventId);
        }
    }
}
