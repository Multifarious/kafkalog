package io.ifar.kafkalog.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;

public class LogMessageFactory {
    private final String topic;

    public LogMessageFactory(String topic) {
        this.topic = topic;
    }

    public ProducerRecord<String, String> create(String line) {
        return new ProducerRecord<>(topic, line);
    }
}
