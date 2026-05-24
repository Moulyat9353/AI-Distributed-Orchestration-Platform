package com.orchestrator.controller;

import com.orchestrator.model.JobRequest;
import com.orchestrator.model.JobResponse;
import com.orchestrator.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class JobController {

    private final JobService jobService;

    @PostMapping("/jobs")
    public ResponseEntity<JobResponse> submitJob(@RequestBody JobRequest request) {
        return ResponseEntity.accepted().body(jobService.submitJob(request));
    }

    @GetMapping("/jobs/{id}")
    public ResponseEntity<JobResponse> getJob(@PathVariable String id) {
        return ResponseEntity.ok(jobService.getJob(id));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "ok"));
    }
}