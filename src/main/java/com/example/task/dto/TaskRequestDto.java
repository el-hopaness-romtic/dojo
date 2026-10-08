package com.example.task.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.util.UUID;

public record TaskRequestDto(
        @NotNull(message = "Идентификатор события обязателен")
        UUID eventId,

        @NotBlank(message = "Название задачи обязательно")
        @Size(max = 255, message = "Название не должно превышать 255 символов")
        String name,

        @NotNull(message = "Длительность обязательна")
        @Positive(message = "Длительность должна быть положительной")
        Long duration
) {
}
