package com.orchestrator.service;

import com.orchestrator.model.Job;
import com.orchestrator.model.JobRequest;
import com.orchestrator.model.JobResponse;
import com.orchestrator.repository.JobRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;


import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class JobService {

    private final KafkaTemplate<String, Map<String, String>> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;
    private final JobRepository jobRepository;
    private final MeterRegistry meterRegistry;

    public JobResponse submitJob(JobRequest request) {

        Job job = jobRepository.save(Job.builder()
                .status(Job.JobStatus.QUEUED)
                .inputText(request.getText())
                .taskType(request.getTaskType())
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build());

        String jobId = job.getId().toString();

        redisTemplate.opsForHash().put("job:" + jobId, "status", "QUEUED");

        Map<String, String> message = new HashMap<>();
        message.put("job_id", jobId);
        message.put("text", request.getText());
        message.put("task_type", request.getTaskType());

        log.info("Publishing job to Kafka topic ai-jobs: {}", message);

        kafkaTemplate.send("ai-jobs", message);

        

        meterRegistry.counter("jobs_submitted_total",
                "task_type", request.getTaskType()).increment();

        return new JobResponse(jobId, "QUEUED", null);
    }

    public JobResponse getJob(String jobId) {
        Map<Object, Object> state = redisTemplate.opsForHash()
                .entries("job:" + jobId);

        if (state.isEmpty()) {
            throw new RuntimeException("Job not found: " + jobId);
        }

        String status = (String) state.get("status");
        String result = (String) state.getOrDefault("result", null);

        return new JobResponse(jobId, status, result);
    }
}