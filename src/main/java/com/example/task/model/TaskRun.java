package com.example.task.model;

import java.util.UUID;

public record TaskRun(
        UUID eventId,
        UUID executionId,
        long duration
) {
}
