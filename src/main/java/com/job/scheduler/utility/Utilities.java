package com.job.scheduler.utility;

import com.job.scheduler.dto.JobDispatchEvent;

import java.util.UUID;

public class Utilities {
    public static String getLockKey(UUID jobId) {
        return "job-lock:" + jobId.toString();
    }

    public static String getDoneKey(UUID jobId) {
        return "job-done:" + jobId.toString();
    }
}
