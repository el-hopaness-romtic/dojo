package com.example.task;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.postgresql.PostgreSQLContainer;

class PostgresTestConfiguration {
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.4");

    static synchronized void startIfNotRunning() {
        if (!POSTGRES.isRunning()) {
            POSTGRES.start();
        }
    }

    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("DB_URL", POSTGRES::getJdbcUrl);
        registry.add("DB_USER", POSTGRES::getUsername);
        registry.add("DB_PASSWORD", POSTGRES::getPassword);
    }
}
