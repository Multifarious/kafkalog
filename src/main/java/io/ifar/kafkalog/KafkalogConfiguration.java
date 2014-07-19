package io.ifar.kafkalog;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

import javax.validation.constraints.NotNull;

public class KafkalogConfiguration extends Configuration {
    @JsonProperty
    private int port;

    @JsonProperty
    private int maxLineLength;

    @JsonProperty
    @NotNull
    private String topic;

    @JsonProperty
    @NotNull
    private String brokers;

    public int getPort() {
        return port;
    }

    public int getMaxLineLength() {
        return maxLineLength;
    }

    public String getTopic() {
        return topic;
    }

    public String getBrokers() {
        return brokers;
    }
}
