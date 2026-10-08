package com.example.task.model;


import jakarta.persistence.Column;

import jakarta.persistence.Entity;

import jakarta.persistence.EnumType;

import jakarta.persistence.Enumerated;

import jakarta.persistence.Id;
import jakarta.persistence.Table;

import lombok.Getter;

import lombok.Setter;


import java.time.Instant;

import java.util.UUID;


@Entity
@Table(name = "task")
@Getter
@Setter
public class Task {
    @Id
    private UUID eventId;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private long duration;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private TaskStatus status;

    @Column(nullable = false)
    private int progress;

    @Column(columnDefinition = "text")
    private String result;

    private UUID executionId;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    private Instant startedAt;

    private Instant finishedAt;

    @Column(nullable = false)
    private int attempts;

    @Column(nullable = false)
    private int maxAttempts;

    private Instant leaseUntil;

    private Instant nextRetryAt;

}
