package com.orchestrator.service;

import com.orchestrator.model.Job;
import com.orchestrator.model.JobRequest;
import com.orchestrator.model.JobResponse;
import com.orchestrator.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class JobService {

    private final KafkaTemplate<String, Map<String, String>> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;
    private final JobRepository jobRepository;

    public JobResponse submitJob(JobRequest request) {
        Job job = jobRepository.save(Job.builder()
                .status(Job.JobStatus.QUEUED)
                .inputText(request.getText())
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build());

        String jobId = job.getId().toString();

        redisTemplate.opsForHash().put("job:" + jobId, "status", "QUEUED");

        Map<String, String> message = new HashMap<>();
        message.put("job_id", jobId);
        message.put("text", request.getText());
        kafkaTemplate.send("ai-jobs", message);

        log.info("Submitted job {}", jobId);
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