package com.example.task.service;

import com.example.task.IntegrationTestBase;
import com.example.task.model.Task;
import com.example.task.model.TaskStatus;
import com.example.task.repository.TaskRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskUpdateServiceTest extends IntegrationTestBase {

    @Autowired
    TaskUpdateService updates;

    @Autowired
    TaskRepository taskRepository;

    @Test
    void progressUpdateRenewsLease() {
        UUID executionId = UUID.randomUUID();
        Instant originalLeaseUntil = Instant.now().plusSeconds(5);
        Task task = new Task();
        task.setEventId(UUID.randomUUID());
        task.setName("lease renewal");
        task.setDuration(1);
        task.setStatus(TaskStatus.IN_PROGRESS);
        task.setProgress(0);
        task.setAttempts(0);
        task.setMaxAttempts(3);
        task.setNextRetryAt(Instant.now());
        task.setCreatedAt(Instant.now());
        task.setExecutionId(executionId);
        task.setStartedAt(Instant.now());
        task.setLeaseUntil(originalLeaseUntil);
        task = taskRepository.save(task);

        assertTrue(updates.saveProgress(task.getEventId(), executionId, 25));

        Task updated = taskRepository.findById(task.getEventId()).orElseThrow();
        assertEquals(25, updated.getProgress());
        assertTrue(updated.getLeaseUntil().isAfter(originalLeaseUntil));
    }

    @Test
    void throwsWhenTaskDoesNotExist() {
        assertThrows(IllegalStateException.class,
                () -> updates.saveProgress(UUID.randomUUID(), UUID.randomUUID(), 25));
    }

    @Test
    void rejectsProgressOutsideInProgressRange() {
        assertThrows(IllegalArgumentException.class,
                () -> updates.saveProgress(UUID.randomUUID(), UUID.randomUUID(), -1));
        assertThrows(IllegalArgumentException.class,
                () -> updates.saveProgress(UUID.randomUUID(), UUID.randomUUID(), 100));
    }
}
