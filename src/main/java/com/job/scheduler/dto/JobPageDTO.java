package com.job.scheduler.dto;

import java.util.List;

public record JobPageDTO(
        List<JobSummaryDTO> content,
        int page,
        int size,
        long totalElements,
        int totalPages,
        boolean first,
        boolean last
) {
}
