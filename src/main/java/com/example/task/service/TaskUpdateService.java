package com.example.task.service;

import com.example.task.model.Task;
import com.example.task.model.TaskStatus;
import com.example.task.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TaskUpdateService {
    private final TaskRepository taskRepository;
    @Value("${worker.lease-duration}")
    private final Duration lease;

    @Transactional
    public boolean saveProgress(UUID eventId, UUID executionId, int progress) {
        if (progress < 0 || progress >= 100) {
            throw new IllegalArgumentException("Прогресс должен быть от 0 до 99");
        }

        var task = taskRepository.findAndLockByEventId(eventId);
        if (task.isEmpty()) throw new IllegalStateException("Задача не найдена: " + eventId);

        Task current = task.get();
        if (!executionId.equals(current.getExecutionId())) return false;
        if (current.getStatus() != TaskStatus.IN_PROGRESS) {
            throw new IllegalStateException("Задача не находится в статусе IN_PROGRESS: " + eventId);
        }

        Instant now = Instant.now();
        current.setProgress(progress);
        current.setLeaseUntil(now.plus(lease));
        taskRepository.save(current);
        return true;
    }

    @Transactional
    public boolean complete(UUID eventId, UUID executionId) {
        return finish(eventId, executionId, TaskStatus.COMPLETED, "Task completed");
    }

    @Transactional
    public boolean fail(UUID eventId, UUID executionId, String reason) {
        return finish(eventId, executionId, TaskStatus.FAILED, reason);
    }

    private boolean finish(UUID eventId, UUID executionId, TaskStatus status, String result) {
        var task = taskRepository.findAndLockByEventId(eventId);
        if (task.isEmpty()) throw new IllegalStateException("Задача не найдена: " + eventId);

        Task current = task.get();
        if (!executionId.equals(current.getExecutionId())) return false;
        if (current.getStatus() != TaskStatus.IN_PROGRESS) {
            throw new IllegalStateException("Задача не находится в статусе IN_PROGRESS: " + eventId);
        }

        Instant now = Instant.now();
        current.setStatus(status);
        if (status == TaskStatus.COMPLETED) current.setProgress(100);
        current.setExecutionId(null);
        current.setResult(result);
        current.setFinishedAt(now);
        current.setLeaseUntil(null);
        current.setNextRetryAt(null);
        taskRepository.save(current);
        return true;
    }
}
