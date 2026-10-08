package com.example.task.dto;

import com.example.task.model.TaskStatus;
import lombok.Builder;

import java.time.Instant;
import java.util.UUID;

@Builder
public record TaskResponseDto(
        UUID eventId,
        String name,
        long duration,
        TaskStatus status,
        int progress,
        String result,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt
) {
}
