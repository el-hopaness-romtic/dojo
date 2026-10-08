package com.example.task.service;

import com.example.task.model.TaskRun;
import com.example.task.model.TaskStatus;
import com.example.task.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TaskClaimService {
    private final TaskRepository taskRepository;
    @Value("${worker.lease-duration}")
    private final Duration lease;

    @Transactional
    public Optional<TaskRun> claimOne() {
        Instant now = Instant.now();
        return taskRepository.findNextReadyTask(now)
                .map(task -> {
                    UUID executionId = UUID.randomUUID();
                    task.setStatus(TaskStatus.IN_PROGRESS);
                    task.setExecutionId(executionId);
                    task.setAttempts(task.getAttempts() + 1);
                    task.setStartedAt(now);
                    task.setFinishedAt(null);
                    task.setProgress(0);
                    task.setResult(null);
                    task.setLeaseUntil(now.plus(lease));
                    task.setNextRetryAt(null);
                    task = taskRepository.save(task);
                    return new TaskRun(task.getEventId(), executionId, task.getDuration());
                });
    }
}
