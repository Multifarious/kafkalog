package io.ifar.kafkalog;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.ifar.kafkalog.kafka.KafkaProducerConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class KafkalogConfiguration extends Configuration {
    @JsonProperty
    private int port = 514;

    @JsonProperty
    private int maxLineLength = 8192;

    /**
     * No point in using unbounded queues anywhere; better to block to give some back
     * pressure (block & wait) than to run out of memory.
     */
    private int maxQueueLength = 50000;

    @JsonProperty
    @NotNull
    private String topic;

    @JsonProperty
    @NotNull
    @Valid
    private KafkaProducerConfiguration kafkaProducer;

    public int getPort() {
        return port;
    }

    public int getMaxLineLength() {
        return maxLineLength;
    }

    public int getMaxQueueLength() {
        return maxQueueLength;
    }

    public String getTopic() {
        return topic;
    }

    public KafkaProducerConfiguration getKafkaProducerConfiguration() {
        return kafkaProducer;
    }
}
