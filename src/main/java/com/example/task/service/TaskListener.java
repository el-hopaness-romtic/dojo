package com.example.task.service;

import com.example.task.dto.TaskRequestDto;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class TaskListener {
    private final Validator validator;
    private final TaskRegistrationService registration;

    @KafkaListener(topics = "${tasks.topic}")
    public void receive(ConsumerRecord<UUID, TaskRequestDto> consumerRecord) {
        TaskRequestDto request = consumerRecord.value();
        if (request == null) {
            throw new InvalidTaskMessageException("Тело сообщения не должно быть пустым");
        }

        UUID key = consumerRecord.key();
        if (key == null) {
            throw new InvalidTaskMessageException("Ключ не должен быть пустым");
        }

        Set<ConstraintViolation<TaskRequestDto>> violations = validator.validate(request);
        if (!violations.isEmpty()) {
            String detail = violations.stream().map(v -> v.getPropertyPath() + " " + v.getMessage()).sorted().toList().toString();
            throw new InvalidTaskMessageException("Недопустимая задача: " + detail);
        }
        if (!request.eventId().equals(key)) {
            throw new InvalidTaskMessageException("Ключ должен совпадать с идентификатором события");
        }

        registration.register(request);
        log.info("Task message registered eventId={} partition={} offset={}", request.eventId(), consumerRecord.partition(), consumerRecord.offset());
    }

    public static class InvalidTaskMessageException extends RuntimeException {
        public InvalidTaskMessageException(String message) {
            super(message);
        }
    }
}
