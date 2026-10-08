package com.example.task.config;

import com.example.task.service.TaskExecutionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.time.Duration;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class TaskServiceConfig {
    private final TaskExecutionService executionService;
    @Value("${worker.poll-delay}")
    private final Duration pollDelay;

    @Bean
    public SimpleAsyncTaskExecutor taskWorkerPool(@Value("${worker.max-concurrent}") int maxConcurrent,
                                                  @Value("${worker.shutdown-wait}") Duration shutdownWait) {
        var executor = new SimpleAsyncTaskExecutor("task-worker-");
        executor.setVirtualThreads(true);
        executor.setConcurrencyLimit(maxConcurrent);
        executor.setRejectTasksWhenLimitReached(true);
        executor.setTaskTerminationTimeout(shutdownWait.toMillis());
        executor.setCancelRemainingTasksOnClose(false);
        return executor;
    }

    @EventListener(ApplicationReadyEvent.class)
    void startWorkers(ApplicationReadyEvent event) {
        var taskWorkerPool = event.getApplicationContext().getBean("taskWorkerPool", SimpleAsyncTaskExecutor.class);
        int workerCount = taskWorkerPool.getConcurrencyLimit();
        for (int i = 0; i < workerCount; i++) taskWorkerPool.execute(() -> runWorker(taskWorkerPool));
        log.info("Started {} virtual task workers", workerCount);
    }

    private void runWorker(SimpleAsyncTaskExecutor taskWorkerPool) {
        while (taskWorkerPool.isActive() && !Thread.currentThread().isInterrupted()) {
            try {
                if (!executionService.runOne()) sleepBeforeRetry();
            } catch (Exception error) {
                log.error("Task worker failed; worker will retry", error);
                sleepBeforeRetry();
            }
        }
    }

    private void sleepBeforeRetry() {
        try {
            Thread.sleep(Math.max(1, pollDelay.toMillis()));
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }
}
