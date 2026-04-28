package com.job.scheduler.utility;

import java.util.UUID;

public class Utilities {
    private Utilities() {
    }

    public static String getLockKey(UUID jobId) {
        return "job-lock:" + jobId.toString();
    }

    public static String getDoneKey(String idempotencyKey) {
        return "job-done:" + idempotencyKey;
    }
}
