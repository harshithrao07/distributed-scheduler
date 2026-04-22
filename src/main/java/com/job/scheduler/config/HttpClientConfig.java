package com.job.scheduler.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

@Configuration
public class HttpClientConfig {

    @Bean
    public RestClient restClient(
            @Value("${scheduler.webhook.connect-timeout-ms:5000}") long connectTimeoutMs,
            @Value("${scheduler.webhook.read-timeout-ms:10000}") long readTimeoutMs
    ) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofMillis(connectTimeoutMs));
        requestFactory.setReadTimeout(Duration.ofMillis(readTimeoutMs));

        return RestClient.builder()
                .requestFactory(requestFactory)
                .build();
    }
}
