package com.example.task.service;

import com.example.task.dto.TaskResponseDto;
import com.example.task.model.Task;
import com.example.task.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TaskQueryService {
    private final TaskRepository taskRepository;

    public Optional<TaskResponseDto> findByEventId(UUID eventId) {
        return taskRepository.findById(eventId).map(this::toResponseDto);
    }

    private TaskResponseDto toResponseDto(Task task) {
        return TaskResponseDto.builder()
                .eventId(task.getEventId())
                .name(task.getName())
                .duration(task.getDuration())
                .status(task.getStatus())
                .progress(task.getProgress())
                .result(task.getResult())
                .createdAt(task.getCreatedAt())
                .startedAt(task.getStartedAt())
                .finishedAt(task.getFinishedAt())
                .build();
    }
}
