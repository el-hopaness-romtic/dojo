package com.example.task.repository;

import com.example.task.model.Task;
import com.example.task.model.TaskStatus;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.hibernate.Timeouts;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface TaskRepository extends JpaRepository<Task, UUID> {
    String SKIP_LOCKED = "" + Timeouts.SKIP_LOCKED_MILLI;

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<Task> findAndLockByEventId(UUID eventId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = SKIP_LOCKED))
    @Query("""
            SELECT task FROM Task task
            WHERE task.status = com.example.task.model.TaskStatus.IN_PROGRESS AND task.leaseUntil <= :now
            ORDER BY task.leaseUntil ASC
            """)
    List<Task> findExpiredTasks(@Param("now") Instant now, Limit limit);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = SKIP_LOCKED))
    @Query("""
            SELECT task FROM Task task
            WHERE task.status = com.example.task.model.TaskStatus.FAILED AND task.attempts < task.maxAttempts
            ORDER BY task.finishedAt ASC
            """)
    List<Task> findRetryableFailedTasks(Limit limit);

    long countByStatusAndNextRetryAtLessThanEqual(TaskStatus status, Instant now);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = SKIP_LOCKED))
    @Query("""
            SELECT task FROM Task task
            WHERE task.status = com.example.task.model.TaskStatus.NEW AND task.nextRetryAt <= :now
            ORDER BY task.nextRetryAt ASC
            LIMIT 1
            """)
    Optional<Task> findNextReadyTask(@Param("now") Instant now);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("""
            INSERT INTO Task (
                eventId,            name,                  duration,              status,             progress,
                attempts,           maxAttempts,           nextRetryAt,           createdAt
            )
            VALUES (
                :#{#task.eventId},  :#{#task.name},        :#{#task.duration},    :#{#task.status},   :#{#task.progress},
                :#{#task.attempts}, :#{#task.maxAttempts}, :#{#task.nextRetryAt}, :#{#task.createdAt}
            )
            ON CONFLICT (eventId) DO NOTHING
            """)
    int insertIfAbsent(@Param("task") Task task);
}
