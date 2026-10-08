CREATE TABLE task
(
    event_id UUID PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    duration      BIGINT       NOT NULL CHECK (duration > 0),
    status        VARCHAR(20)  NOT NULL CHECK (status IN ('NEW', 'IN_PROGRESS', 'COMPLETED', 'FAILED')),
    progress      INTEGER      NOT NULL CHECK (progress BETWEEN 0 AND 100),
    result        TEXT,
    execution_id  UUID,
    created_at    TIMESTAMPTZ  NOT NULL,
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    attempts      INTEGER      NOT NULL CHECK (attempts >= 0),
    max_attempts  INTEGER      NOT NULL CHECK (max_attempts > 0),
    lease_until   TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    CONSTRAINT task_running_state CHECK (
        status <> 'IN_PROGRESS'
        OR (execution_id IS NOT NULL AND started_at IS NOT NULL)
    ),
    CONSTRAINT task_finished_state CHECK (
        status NOT IN ('COMPLETED', 'FAILED') OR finished_at IS NOT NULL
    )
);

-- для новых задач
CREATE INDEX idx_tasks_ready_next_retry_at
    ON task (next_retry_at)
    WHERE status = 'NEW';

-- для просроченных
CREATE INDEX idx_tasks_expired_lease_until
    ON task (lease_until)
    WHERE status = 'IN_PROGRESS';

-- для retryable
CREATE INDEX idx_tasks_retryable_failed_finished_at
    ON task (finished_at) WHERE status = 'FAILED' AND attempts < max_attempts;
