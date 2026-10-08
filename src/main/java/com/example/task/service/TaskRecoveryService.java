package com.example.task.service;

import com.example.task.repository.TaskRepository;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Limit;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;

import static com.example.task.model.TaskStatus.FAILED;
import static com.example.task.model.TaskStatus.NEW;

@Service
@RequiredArgsConstructor
@Slf4j
public class TaskRecoveryService {
    private final TaskRepository taskRepository;
    @Value("${worker.retry-delay}")
    private final Duration retryDelay;
    @Value("${worker.recover-batch-size}")
    private final int recoverBatchSize;
    private final MeterRegistry metrics;

    @PostConstruct
    void validateRecoveryBatchSize() {
        if (recoverBatchSize < 1) {
            throw new IllegalArgumentException("worker.recover-batch-size must be positive");
        }
    }

    @Scheduled(fixedDelayString = "${worker.recovery-interval}")
    @Transactional
    public void recoverExpiredTasks() {
        Instant now = Instant.now();
        var expiredTasks = taskRepository.findExpiredTasks(now, Limit.of(recoverBatchSize));

        for (var task : expiredTasks) {
            boolean attemptsExhausted = task.getAttempts() >= task.getMaxAttempts();
            task.setStatus(attemptsExhausted ? FAILED : NEW);
            task.setResult(attemptsExhausted ? "Attempts exhausted after lease expiry" : null);
            if (!attemptsExhausted) {
                task.setProgress(0);
                task.setStartedAt(null);
                task.setFinishedAt(null);
                task.setNextRetryAt(now.plus(retryDelay));
            } else {
                task.setFinishedAt(now);
                task.setNextRetryAt(null);
            }
            task.setExecutionId(null);
            task.setLeaseUntil(null);
        }

        int recovered = expiredTasks.size();
        if (recovered > 0) {
            metrics.counter("tasks.recoveries").increment(recovered);
            log.warn("Recovered {} expired task leases", recovered);
        }
    }

    @Scheduled(fixedDelayString = "${worker.recovery-interval}")
    @Transactional
    public void retryFailedTasks() {
        Instant now = Instant.now();
        var failedTasks = taskRepository.findRetryableFailedTasks(Limit.of(recoverBatchSize));

        for (var task : failedTasks) {
            task.setStatus(NEW);
            task.setProgress(0);
            task.setResult(null);
            task.setExecutionId(null);
            task.setLeaseUntil(null);
            task.setStartedAt(null);
            task.setFinishedAt(null);
            task.setNextRetryAt(now.plus(retryDelay));
        }

        int retried = failedTasks.size();
        if (retried > 0) {
            metrics.counter("tasks.retries").increment(retried);
            log.info("Scheduled retry for {} failed tasks", retried);
        }
    }
}
