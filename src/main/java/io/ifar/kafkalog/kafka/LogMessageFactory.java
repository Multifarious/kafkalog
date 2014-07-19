package io.ifar.kafkalog.kafka;

import kafka.producer.KeyedMessage;

public class LogMessageFactory {
    private final String topic;

    public LogMessageFactory(String topic) {
        this.topic = topic;
    }

    public KeyedMessage<String, String> create(String line) {
        return new KeyedMessage<String, String>(topic, line);
    }
}
