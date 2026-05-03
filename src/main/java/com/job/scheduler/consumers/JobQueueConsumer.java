package com.job.scheduler.consumers;

import com.job.scheduler.constants.Topics;
import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.service.WorkerService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class JobQueueConsumer {
    private final WorkerService workerService;

    @KafkaListener(id = "job-queue-worker", topics = Topics.TOPIC_JOB_QUEUE, groupId = "scheduler-group", concurrency = "12")
    public void consumeJobFromQueue(JobDispatchEvent jobDispatchEvent) {
        workerService.processJob(jobDispatchEvent);
    }

    @KafkaListener(id = "high-priority-job-queue-worker", topics = Topics.TOPIC_JOB_QUEUE_HIGH, groupId = "scheduler-group", concurrency = "12")
    public void consumeJobFromHighPriorityQueue(JobDispatchEvent jobDispatchEvent) {
        workerService.processJob(jobDispatchEvent);
    }
}
