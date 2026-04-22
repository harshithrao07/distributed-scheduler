package com.job.scheduler.dto;

import java.util.List;

public record DlqPageDTO(
        List<DlqJobSummaryDTO> content,
        int page,
        int size,
        long totalElements,
        int totalPages,
        boolean first,
        boolean last
) {
}
