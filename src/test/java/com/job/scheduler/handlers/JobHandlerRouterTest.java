package com.job.scheduler.handlers;

import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.dto.payload.CleanupPayload;
import com.job.scheduler.dto.payload.SendEmailPayload;
import com.job.scheduler.dto.payload.WebhookPayload;
import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.service.JobService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobHandlerRouterTest {

    @Mock
    private JobService jobService;

    @Mock
    private SendEmailHandler sendEmailHandler;

    @Mock
    private WebhookHandler webhookHandler;

    @Mock
    private CleanupHandler cleanupHandler;

    private JobHandlerRouter jobHandlerRouter;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        jobHandlerRouter = new JobHandlerRouter(
                objectMapper,
                jobService,
                sendEmailHandler,
                webhookHandler,
                cleanupHandler
        );
    }

    @Test
    void routeDelegatesSendEmailPayloadToSendEmailHandler() {
        UUID jobId = UUID.randomUUID();
        Job job = new Job();
        job.setId(jobId);
        job.setJobType(JobType.SEND_EMAIL);

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("to", "user@example.com");
        payload.put("subject", "hello");
        payload.put("body", "world");
        job.setPayload(payload.toString());

        when(jobService.findById(jobId)).thenReturn(job);

        jobHandlerRouter.route(new JobDispatchEvent(jobId, JobType.SEND_EMAIL));

        ArgumentCaptor<SendEmailPayload> captor = ArgumentCaptor.forClass(SendEmailPayload.class);
        verify(sendEmailHandler).handle(captor.capture());
        assertThat(captor.getValue().to()).isEqualTo("user@example.com");
        assertThat(captor.getValue().subject()).isEqualTo("hello");
        assertThat(captor.getValue().body()).isEqualTo("world");
    }

    @Test
    void routeDelegatesWebhookPayloadToWebhookHandler() {
        UUID jobId = UUID.randomUUID();
        Job job = new Job();
        job.setId(jobId);
        job.setJobType(JobType.WEBHOOK);

        ObjectNode body = objectMapper.createObjectNode();
        body.put("message", "ping");
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("url", "https://example.com/hook");
        payload.set("body", body);
        job.setPayload(payload.toString());

        when(jobService.findById(jobId)).thenReturn(job);

        jobHandlerRouter.route(new JobDispatchEvent(jobId, JobType.WEBHOOK));

        ArgumentCaptor<WebhookPayload> captor = ArgumentCaptor.forClass(WebhookPayload.class);
        verify(webhookHandler).handle(captor.capture());
        assertThat(captor.getValue().url()).isEqualTo("https://example.com/hook");
        assertThat(captor.getValue().body().get("message").stringValue()).isEqualTo("ping");
    }

    @Test
    void routeDelegatesCleanupPayloadToCleanupHandler() {
        UUID jobId = UUID.randomUUID();
        Job job = new Job();
        job.setId(jobId);
        job.setJobType(JobType.CLEANUP);

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("olderThanDays", 30);
        job.setPayload(payload.toString());

        when(jobService.findById(jobId)).thenReturn(job);

        jobHandlerRouter.route(new JobDispatchEvent(jobId, JobType.CLEANUP));

        ArgumentCaptor<CleanupPayload> captor = ArgumentCaptor.forClass(CleanupPayload.class);
        verify(cleanupHandler).handle(captor.capture());
        assertThat(captor.getValue().olderThanDays()).isEqualTo(30);
    }

    @Test
    void routeThrowsWhenPayloadShapeIsInvalid() {
        UUID jobId = UUID.randomUUID();
        Job job = new Job();
        job.setId(jobId);
        job.setJobType(JobType.WEBHOOK);
        job.setPayload(objectMapper.createArrayNode().add("not-an-object").toString());

        when(jobService.findById(jobId)).thenReturn(job);

        JobDispatchEvent event = new JobDispatchEvent(jobId, JobType.WEBHOOK);

        assertThatThrownBy(() -> jobHandlerRouter.route(event))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Payload does not match expected shape");
    }
}
