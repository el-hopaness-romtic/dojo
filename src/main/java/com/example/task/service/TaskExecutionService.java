package com.example.task.service;

import com.example.task.model.TaskRun;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
@Slf4j
public class TaskExecutionService {
    private final TaskClaimService claimService;
    private final TaskUpdateService updateService;
    private final MeterRegistry metrics;
    @Value("${worker.progress-step}")
    private final Duration progressStep;
    private final AtomicInteger active = new AtomicInteger();

    @PostConstruct
    void registerMetrics() {
        metrics.gauge("tasks.active", active);
    }

    public boolean runOne() {
        var task = claimService.claimOne();
        if (task.isEmpty()) return false;

        executeClaimed(task.get());
        return true;
    }

    private void executeClaimed(TaskRun task) {
        active.incrementAndGet();
        log.info("Task claimed eventId={} executionId={} attempt", task.eventId(), task.executionId());

        try {
            if (execute(task)) {
                recordOutcome("completed");
                log.info("Task execution finished eventId={} executionId={}", task.eventId(), task.executionId());
            } else {
                recordOutcome("lease_lost");
                log.info("Task execution lease no longer owned eventId={} executionId={}", task.eventId(), task.executionId());
            }
        } catch (Exception error) {
            log.error("Task execution failed eventId={} executionId={}", task.eventId(), task.executionId(), error);

            String reason = error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
            try {
                if (!updateService.fail(task.eventId(), task.executionId(), reason)) {
                    log.info("Task failure was not recorded because execution ownership changed eventId={} executionId={}",
                            task.eventId(), task.executionId());
                }
            } catch (Exception failureError) {
                log.warn("Failed to mark task failed and clear its lease eventId={} executionId={}",
                        task.eventId(), task.executionId(), failureError);
            }
            recordOutcome("failed");

            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
        } finally {
            active.decrementAndGet();
        }
    }

    private void recordOutcome(String status) {
        metrics.counter("tasks.outcomes", "status", status).increment();
    }

    public boolean execute(TaskRun task) throws InterruptedException {
        long durationMillis = task.duration();
        long elapsedMillis = 0;
        while (elapsedMillis < durationMillis) {
            long stepMillis = Math.clamp(progressStep.toMillis(), 1, durationMillis - elapsedMillis);
            Thread.sleep(stepMillis);
            elapsedMillis += stepMillis;
            if (elapsedMillis >= durationMillis) break;

            int progress = (int) (elapsedMillis * 100 / durationMillis);
            if (!updateService.saveProgress(task.eventId(), task.executionId(), progress)) return false;
        }
        return updateService.complete(task.eventId(), task.executionId());
    }
}
