package io.ifar.kafkalog;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

import javax.validation.constraints.NotNull;

public class KafkalogConfiguration extends Configuration {
    @JsonProperty
    private int port = 514;

    @JsonProperty
    private int maxLineLength = 8192;

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
