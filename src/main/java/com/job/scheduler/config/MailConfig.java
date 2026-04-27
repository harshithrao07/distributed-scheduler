package com.job.scheduler.config;

import jakarta.mail.internet.MimeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

import java.io.InputStream;

@Configuration
public class MailConfig {

    @Bean
    @ConditionalOnMissingBean(JavaMailSender.class)
    public JavaMailSender loggingMailSender() {
        return new LoggingMailSender();
    }

    private static class LoggingMailSender implements JavaMailSender {
        private static final Logger log = LoggerFactory.getLogger(LoggingMailSender.class);

        @Override
        public MimeMessage createMimeMessage() {
            throw new UnsupportedOperationException("MIME messages are not supported by the logging mail sender");
        }

        @Override
        public MimeMessage createMimeMessage(InputStream contentStream) {
            throw new UnsupportedOperationException("MIME messages are not supported by the logging mail sender");
        }

        @Override
        public void send(MimeMessage mimeMessage) throws MailException {
            log.info("Skipping MIME email send because no SMTP mail sender is configured");
        }

        @Override
        public void send(MimeMessage... mimeMessages) throws MailException {
            log.info("Skipping {} MIME email(s) because no SMTP mail sender is configured", mimeMessages.length);
        }

        @Override
        public void send(SimpleMailMessage simpleMessage) throws MailException {
            log.info(
                    "Skipping email send because no SMTP mail sender is configured. to={}, subject={}",
                    simpleMessage.getTo(),
                    simpleMessage.getSubject()
            );
        }

        @Override
        public void send(SimpleMailMessage... simpleMessages) throws MailException {
            log.info("Skipping {} email(s) because no SMTP mail sender is configured", simpleMessages.length);
        }
    }
}
