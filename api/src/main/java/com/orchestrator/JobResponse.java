package com.orchestrator.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class JobResponse {
    private String jobId;
    private String status;
    private String result;
}