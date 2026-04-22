package com.job.scheduler.handlers;

import com.job.scheduler.dto.payload.WebhookPayload;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

@Service
@RequiredArgsConstructor
public class WebhookHandler implements JobHandler<WebhookPayload> {
    private final RestClient restClient;

    @Override
    public void handle(WebhookPayload payload) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<String> response = restClient.post()
                .uri(payload.url())
                .headers(httpHeaders -> httpHeaders.addAll(headers))
                .body(payload.body() == null ? "{}" : payload.body().toString())
                .retrieve()
                .toEntity(String.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new IllegalStateException("Webhook failed with status: " + response.getStatusCode());
        }
    }
}
