package com.example.task.config;

import com.example.task.dto.TaskRequestDto;
import com.example.task.service.TaskListener;
import com.example.task.service.TaskRegistrationService;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;
import java.util.UUID;

@Configuration
public class KafkaConfig {
    @Bean
    @SuppressWarnings({"unchecked", "resource"})
    DefaultKafkaProducerFactoryCustomizer kafkaProducerFactoryCustomizer() {
        return factory -> {
            var typedFactory = (DefaultKafkaProducerFactory<Object, Object>) factory;

            typedFactory.setKeySerializer(new DelegatingByTypeSerializer(Map.of(
                    UUID.class, new UUIDSerializer(),
                    byte[].class, new ByteArraySerializer()
            )));

            typedFactory.setValueSerializer(new DelegatingByTypeSerializer(Map.of(
                    TaskRequestDto.class, new JacksonJsonSerializer<TaskRequestDto>().noTypeInfo(),
                    byte[].class, new ByteArraySerializer()
            )));
        };
    }

    @Bean
    CommonErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> template,
                                         @Value("${tasks.dead-letter-topic}") String deadLetterTopic) {
        var recoverer = new DeadLetterPublishingRecoverer(
                template,
                (consumerRecord, exception) -> new TopicPartition(deadLetterTopic, consumerRecord.partition())
        );
        recoverer.setFailIfSendResultIsError(true);

        var errorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(1_000, FixedBackOff.UNLIMITED_ATTEMPTS)
        );
        errorHandler.setClassifications(Map.of(
                TaskListener.InvalidTaskMessageException.class, false,
                TaskRegistrationService.ConflictingTaskException.class, false,
                DeserializationException.class, false,
                MessageConversionException.class, false,
                ConversionException.class, false
        ), true);
        return errorHandler;
    }
}
