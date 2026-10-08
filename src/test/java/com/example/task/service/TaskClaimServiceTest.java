package com.example.task.service;

import com.example.task.IntegrationTestBase;
import com.example.task.config.TaskServiceConfig;
import com.example.task.model.Task;
import com.example.task.model.TaskRun;
import com.example.task.model.TaskStatus;
import com.example.task.repository.TaskRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TaskClaimServiceTest extends IntegrationTestBase {

    @Autowired
    TaskRepository taskRepository;

    @Autowired
    TaskClaimService claims;

    @MockitoBean
    TaskServiceConfig taskServiceConfig;

    @Autowired
    EntityManager entityManager;

    TransactionTemplate txTemplate;

    @Autowired
    void setTransactionManager(PlatformTransactionManager transactionManager) {
        txTemplate = new TransactionTemplate(transactionManager);
    }

    @AfterEach
    void cleanUpTasks() {
        taskRepository.deleteAll();
    }

    @Test
    void claimsTaskWithEarliestNextRetryAtAndMarksItInProgress() {
        Task first = taskRepository.save(newReadyTask("first", Instant.now().minusSeconds(2)));
        Task second = taskRepository.save(newReadyTask("second", Instant.now().minusSeconds(1)));

        TaskRun claimed = claims.claimOne().orElseThrow();

        assertEquals(first.getEventId(), claimed.eventId());
        assertEquals(TaskStatus.IN_PROGRESS, taskRepository.findById(first.getEventId()).orElseThrow().getStatus());
        assertEquals(TaskStatus.NEW, taskRepository.findById(second.getEventId()).orElseThrow().getStatus());
    }

    @Test
    @SneakyThrows
    void claimSkipsLockedTask() {
        Task first = taskRepository.save(newReadyTask("first", Instant.now().minusSeconds(2)));
        Task second = taskRepository.save(newReadyTask("second", Instant.now().minusSeconds(1)));

        var locked = new CompletableFuture<Void>();
        var release = new CompletableFuture<Void>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var locking = executor.submit(() -> txTemplate.executeWithoutResult(status -> {
                Task lockedTask = taskRepository.findById(first.getEventId()).orElseThrow();
                entityManager.lock(lockedTask, LockModeType.PESSIMISTIC_WRITE);
                locked.complete(null);
                release.join();
            }));

            try {
                locked.get(10, TimeUnit.SECONDS);
                TaskRun claimed = executor.submit(claims::claimOne).get(10, TimeUnit.SECONDS).orElseThrow();
                assertEquals(second.getEventId(), claimed.eventId());
            } finally {
                release.complete(null);
            }
            locking.get(10, TimeUnit.SECONDS);
        }
    }

    Task newReadyTask(String name, Instant nextRetryAt) {
        Task task = new Task();
        task.setEventId(UUID.randomUUID());
        task.setName(name);
        task.setDuration(1);
        task.setStatus(TaskStatus.NEW);
        task.setProgress(0);
        task.setAttempts(0);
        task.setMaxAttempts(3);
        task.setNextRetryAt(nextRetryAt);
        task.setCreatedAt(Instant.now());
        return task;
    }

}
