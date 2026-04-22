package com.job.scheduler.handlers;

public interface JobHandler<T> {
    void handle(T payload);
}
