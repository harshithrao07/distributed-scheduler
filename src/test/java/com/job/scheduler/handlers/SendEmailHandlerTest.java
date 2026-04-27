package com.job.scheduler.handlers;

import com.job.scheduler.dto.payload.SendEmailPayload;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SendEmailHandlerTest {

    @Mock
    private JavaMailSender mailSender;

    private SendEmailHandler sendEmailHandler;

    @BeforeEach
    void setUp() {
        sendEmailHandler = new SendEmailHandler(mailSender);
    }

    @Test
    void handleBuildsAndSendsSimpleMailMessage() {
        SendEmailPayload payload = new SendEmailPayload(
                "user@example.com",
                "hello",
                "world"
        );

        sendEmailHandler.handle(payload);

        ArgumentCaptor<SimpleMailMessage> captor = ArgumentCaptor.forClass(SimpleMailMessage.class);
        verify(mailSender).send(captor.capture());

        SimpleMailMessage message = captor.getValue();
        assertThat(message.getTo()).containsExactly("user@example.com");
        assertThat(message.getSubject()).isEqualTo("hello");
        assertThat(message.getText()).isEqualTo("world");
    }
}
