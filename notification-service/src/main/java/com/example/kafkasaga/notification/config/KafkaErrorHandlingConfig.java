package com.example.kafkasaga.notification.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * 컨슈머 처리 실패 시 지수 백오프로 재시도(1s -> 2s -> 4s, 최대 3회)하고,
 * 그래도 실패하면 원본 토픽명 + "-dlt" 토픽으로 (DeadLetterPublishingRecoverer 기본 명명 규칙) 메시지를 전송한다(Dead Letter Queue).
 */
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaOperations<Object, Object> kafkaOperations) {
        var recoverer = new DeadLetterPublishingRecoverer(kafkaOperations);

        var backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxElapsedTime(7_000L);

        return new DefaultErrorHandler(recoverer, backOff);
    }
}
