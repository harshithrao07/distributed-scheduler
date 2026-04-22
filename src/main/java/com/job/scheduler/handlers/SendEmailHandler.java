package com.job.scheduler.handlers;

import com.job.scheduler.dto.payload.SendEmailPayload;
import lombok.RequiredArgsConstructor;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SendEmailHandler implements JobHandler<SendEmailPayload> {
    private final JavaMailSender mailSender;

    @Override
    public void handle(SendEmailPayload payload) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo(payload.to());
        message.setSubject(payload.subject());
        message.setText(payload.body());

        mailSender.send(message);
    }
}
