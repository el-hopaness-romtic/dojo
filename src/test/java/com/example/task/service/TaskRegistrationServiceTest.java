package com.example.task.service;

import com.example.task.IntegrationTestBase;
import com.example.task.dto.TaskRequestDto;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskRegistrationServiceTest extends IntegrationTestBase {

    @Autowired
    KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Autowired
    ConsumerFactory<Object, Object> consumerFactory;

    @Autowired
    JdbcTemplate jdbc;

    @Value("${tasks.topic}")
    String taskTopic;

    @Value("${tasks.dead-letter-topic}")
    String deadLetterTopic;

    @Test
    @SneakyThrows
    void kafkaTaskMessageIsRegisteredInPostgres() {
        UUID eventId = UUID.randomUUID();
        TaskRequestDto request = new TaskRequestDto(eventId, "integration test task", 250L);

        kafkaTemplate.send(taskTopic, eventId, request)
                .get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertEquals(1, jdbc.queryForObject(
                    "SELECT COUNT(*) FROM task WHERE event_id = ? AND name = ? AND duration = ?",
                    Integer.class, eventId, request.name(), request.duration()));
        });
    }

    @Test
    @SneakyThrows
    void postgresConnectionFailureInListenerIsRetriedUntilRegistrationSucceeds() {
        var dataSource = jdbc.getDataSource();
        assertNotNull(dataSource);

        UUID eventId = UUID.randomUUID();
        TaskRequestDto request = new TaskRequestDto(eventId, "retry after postgres disconnect", 250L);
        long deadLetterOffsetBefore = latestOffset(deadLetterTopic, 0);

        try (var lockConnection = dataSource.getConnection()) {
            lockConnection.setAutoCommit(false);
            try (var statement = lockConnection.createStatement()) {
                statement.execute("LOCK TABLE task IN ACCESS EXCLUSIVE MODE");
            }

            kafkaTemplate.send(taskTopic, 0, eventId, request).get(10, TimeUnit.SECONDS);

            int failedAttemptBackendPid = awaitBlockedTaskInsertBackendPid(null);
            assertEquals(Boolean.TRUE, jdbc.queryForObject(
                    "SELECT pg_terminate_backend(?)", Boolean.class, failedAttemptBackendPid));

            // The second backend proves the listener retried the same insert after PostgreSQL
            // terminated the connection used by its first attempt.
            int retryBackendPid = awaitBlockedTaskInsertBackendPid(failedAttemptBackendPid);
            assertNotEquals(retryBackendPid, failedAttemptBackendPid);

            lockConnection.rollback();
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM task WHERE event_id = ? AND name = ? AND duration = ?",
                Integer.class, eventId, request.name(), request.duration())));
        assertEquals(deadLetterOffsetBefore, latestOffset(deadLetterTopic, 0));
    }

    @Test
    @SneakyThrows
    void invalidJsonMessageIsSentToDeadLetterTopic() {
        UUID eventId = UUID.randomUUID();
        assertDeadLetterOffsetAdvanced(0, () -> {
            publishRawMessage(0, eventId, "{");
            return null;
        });
    }

    @Test
    @SneakyThrows
    void validJsonWithInvalidEventIdIsSentToDeadLetterTopic() {
        UUID eventId = UUID.randomUUID();
        String invalidJson = """
                {"eventId":"not-a-uuid","name":"invalid event id","duration":250}
                """;
        assertDeadLetterOffsetAdvanced(0, () -> {
            publishRawMessage(0, eventId, invalidJson);
            return null;
        });
    }

    @Test
    @SneakyThrows
    void messageWithMalformedKafkaKeyIsSentToDeadLetterTopic() {
        UUID eventId = UUID.randomUUID();
        String requestJson = "{\"eventId\":\"" + eventId + "\",\"name\":\"invalid key\",\"duration\":250}";
        assertDeadLetterOffsetAdvanced(0, () -> {
            var keyBytes = "not-a-uuid".getBytes(StandardCharsets.UTF_8);
            var valueBytes = requestJson.getBytes(StandardCharsets.UTF_8);
            publishRawBytes(0, keyBytes, valueBytes);
            return null;
        });
    }

    @Test
    @SneakyThrows
    void taskWithNegativeDurationIsSentToDeadLetterTopicWithValidationErrorHeader() {
        UUID eventId = UUID.randomUUID();
        TaskRequestDto request = new TaskRequestDto(eventId, "negative duration", -250L);

        long offset = assertDeadLetterOffsetAdvanced(0, () -> {
            kafkaTemplate.send(taskTopic, 0, eventId, request).get(10, TimeUnit.SECONDS);
            return null;
        });

        var deadLetterRecord = receiveDeadLetterRecord(0, offset);
        assertNotNull(deadLetterRecord);
        assertEquals(eventId, deadLetterRecord.key());
        assertEquals(request, deadLetterRecord.value());

        var exceptionHeader = deadLetterRecord.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        assertNotNull(exceptionHeader);
        String errorMessage = new String(exceptionHeader.value(), StandardCharsets.UTF_8);
        assertTrue(errorMessage.contains("duration"));
        assertTrue(errorMessage.contains("Недопустимая задача"));
        assertTrue(errorMessage.contains("положительной"));
    }

    @Test
    @SneakyThrows
    void messageWithNullKafkaKeyIsSentToDeadLetterTopic() {
        UUID eventId = UUID.randomUUID();
        TaskRequestDto request = new TaskRequestDto(eventId, "null key", 250L);

        long offset = assertDeadLetterOffsetAdvanced(0, () -> {
            kafkaTemplate.send(taskTopic, 0, null, request).get(10, TimeUnit.SECONDS);
            return null;
        });

        var deadLetterRecord = receiveDeadLetterRecord(0, offset);
        assertNotNull(deadLetterRecord);
        assertNull(deadLetterRecord.key());
        assertEquals(request, deadLetterRecord.value());
        var exceptionHeader = deadLetterRecord.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        assertNotNull(exceptionHeader);
        assertTrue(new String(exceptionHeader.value(), StandardCharsets.UTF_8).contains("Ключ не должен быть пустым"));
    }

    @Test
    @SneakyThrows
    void messageWithNullKafkaValueIsSentToDeadLetterTopic() {
        UUID eventId = UUID.randomUUID();

        long offset = assertDeadLetterOffsetAdvanced(0, () -> {
            kafkaTemplate.send(taskTopic, 0, eventId, null).get(10, TimeUnit.SECONDS);
            return null;
        });

        var deadLetterRecord = receiveDeadLetterRecord(0, offset);
        assertNotNull(deadLetterRecord);
        assertEquals(eventId, deadLetterRecord.key());
        assertNull(deadLetterRecord.value());
        var exceptionHeader = deadLetterRecord.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        assertNotNull(exceptionHeader);
        assertTrue(new String(exceptionHeader.value(), StandardCharsets.UTF_8).contains("Тело сообщения не должно быть пустым"));
    }

    @SneakyThrows
    long assertDeadLetterOffsetAdvanced(int partition, Callable<?> publish) {
        var deadLetterPartition = new TopicPartition(deadLetterTopic, partition);
        try (var admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            long firstNewDeadLetterOffset = admin.listOffsets(Map.of(deadLetterPartition, OffsetSpec.latest()))
                    .all().get(10, TimeUnit.SECONDS).get(deadLetterPartition).offset();

            publish.call();

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                long latestOffset = admin.listOffsets(Map.of(deadLetterPartition, OffsetSpec.latest()))
                        .all().get(10, TimeUnit.SECONDS).get(deadLetterPartition).offset();
                assertTrue(latestOffset > firstNewDeadLetterOffset);
            });
            return firstNewDeadLetterOffset;
        }
    }

    ConsumerRecord<Object, Object> receiveDeadLetterRecord(int partition, long offset) {
        kafkaTemplate.setConsumerFactory(consumerFactory);
        return kafkaTemplate.receive(deadLetterTopic, partition, offset, Duration.ofSeconds(10));
    }

    int awaitBlockedTaskInsertBackendPid(Integer excludedPid) {
        var backendPid = new java.util.concurrent.atomic.AtomicInteger();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var pids = jdbc.query("""
                    SELECT pid
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND pid <> pg_backend_pid()
                      AND (CAST(? AS integer) IS NULL OR pid <> CAST(? AS integer))
                      AND lower(query) LIKE 'insert into task%'
                      AND wait_event_type = 'Lock'
                    ORDER BY query_start
                    LIMIT 1
                    """, (resultSet, rowNum) -> resultSet.getInt(1), excludedPid, excludedPid);
            Integer pid = pids.stream().findFirst().orElse(null);
            assertNotNull(pid);
            backendPid.set(pid);
        });
        return backendPid.get();
    }

    @SneakyThrows
    long latestOffset(String topic, int partition) {
        var topicPartition = new TopicPartition(topic, partition);
        try (var admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            return admin.listOffsets(Map.of(topicPartition, OffsetSpec.latest()))
                    .all().get(10, TimeUnit.SECONDS).get(topicPartition).offset();
        }
    }

    @SneakyThrows
    void publishRawMessage(int partition, UUID key, String value) {
        byte[] keyBytes = null;
        if (key != null) {
            try (var serializer = new UUIDSerializer()) {
                keyBytes = serializer.serialize(taskTopic, key);
            }
        }
        var valueBytes = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
        publishRawBytes(partition, keyBytes, valueBytes);
    }

    @SneakyThrows
    void publishRawBytes(int partition, byte[] key, byte[] value) {
        var producerProperties = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (var producer = new KafkaProducer<byte[], byte[]>(producerProperties)) {
            producer.send(new ProducerRecord<>(taskTopic, partition, key, value)).get(10, TimeUnit.SECONDS);
        }
    }
}
