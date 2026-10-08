package com.example.task;

import lombok.SneakyThrows;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@ActiveProfiles("test")
@SpringBootTest(webEnvironment = RANDOM_PORT)
public abstract class IntegrationTestBase {

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            CompletableFuture.allOf(
                    CompletableFuture.runAsync(PostgresTestConfiguration::startIfNotRunning, executor),
                    CompletableFuture.runAsync(KafkaTestConfiguration::startIfNotRunning, executor)
            ).join();
        }

        PostgresTestConfiguration.registerProperties(registry);
        KafkaTestConfiguration.registerProperties(registry);
    }

    @SneakyThrows
    protected static void awaitLatch(CountDownLatch latch) {
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Timed out waiting for latch");
    }
}
