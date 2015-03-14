package io.ifar.kafkalog.kafka;
import com.amazonaws.util.EC2MetadataUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import sun.net.www.protocol.http.AuthenticationHeader;

import javax.validation.constraints.NotNull;
import java.io.Serializable;

/**
 * See <a href="http://kafka.apache.org/documentation.html#producerconfigs">Kafka producer configuration</a> docs.
 */
public class KafkaProducerConfiguration implements Serializable {


    @JsonProperty
    private Optional<Long> bufferMemory = Optional.absent();

    @JsonProperty
    private Optional<Integer> retries = Optional.absent();

    @JsonProperty
    @NotNull
    private String brokers;


    @JsonProperty
    private KafkaRequiredAcks requestRequiredAcks = KafkaRequiredAcks.one; // leader has written it.

    @JsonProperty
    private KafkaCompressionCodec compressionCodec = KafkaCompressionCodec.none;

    @JsonProperty
    private Optional<Integer> batchSize = Optional.absent();

    @JsonProperty
    private Optional<String> clientId = Optional.absent();

    @JsonProperty
    private Optional<Long> lingerMillis = Optional.absent();

    @JsonProperty
    private Optional<Integer> maxRequestSize = Optional.absent();

    @JsonProperty
    private Optional<Integer> receiveBufferBytes = Optional.absent();

    @JsonProperty
    private Optional<Integer> sendBufferBytes = Optional.absent();

    @JsonProperty
    private Optional<Integer> timeoutMillis = Optional.absent();

    @JsonProperty
    private Optional<Boolean> blockOnBufferFull = Optional.absent();

    @JsonProperty
    private Optional<Long> retryBackoffMillis = Optional.absent();

    @JsonProperty
    private Optional<Long> reconnectBackoffMillis = Optional.absent();

    @JsonProperty
    private Optional<Long> metadataFetchTimeoutMillis = Optional.absent();

    @JsonProperty
    private Optional<Long> metadataMaxAgeMillis = Optional.absent();

    @JsonProperty
    private boolean registerKafkaMetricsWithDropwizardMetrics = true;

    public Optional<Long> getBufferMemory() {
        return bufferMemory;
    }

    public Optional<Integer> getRetries() {
        return retries;
    }

    public Optional<Integer> getBatchSize() {
        return batchSize;
    }

    public Optional<Long> getLingerMillis() {
        return lingerMillis;
    }

    public Optional<Integer> getMaxRequestSize() {
        return maxRequestSize;
    }

    public Optional<Integer> getReceiveBufferBytes() {
        return receiveBufferBytes;
    }

    public Optional<Integer> getSendBufferBytes() {
        return sendBufferBytes;
    }

    public Optional<Integer> getTimeoutMillis() {
        return timeoutMillis;
    }

    public Optional<Boolean> getBlockOnBufferFull() {
        return blockOnBufferFull;
    }

    public Optional<Long> getRetryBackoffMillis() {
        return retryBackoffMillis;
    }

    public Optional<Long> getReconnectBackoffMillis() {
        return reconnectBackoffMillis;
    }

    public Optional<Long> getMetadataFetchTimeoutMillis() {
        return metadataFetchTimeoutMillis;
    }

    public Optional<Long> getMetadataMaxAgeMillis() {
        return metadataMaxAgeMillis;
    }

    public Optional<String> getClientId() {
        if (!clientId.isPresent()) {
            clientId = Optional.fromNullable(EC2MetadataUtils.getInstanceId());
        }
        return clientId;
    }

    public boolean getRegisterKafkaMetricsWithDropwizardMetrics() {
        return registerKafkaMetricsWithDropwizardMetrics;
    }

    public enum KafkaCompressionCodec { none, gzip, snappy }

    public enum KafkaRequiredAcks {

        zero("0"), one("1"), all("all");

        private String value;

        KafkaRequiredAcks(String value) {
            this.value = value;
        }

        public String configurationValue() {
            return value;
        }
    }

    public String getBrokers() {
        return brokers;
    }

    public KafkaRequiredAcks getRequestRequiredAcks() {
        return requestRequiredAcks;
    }

    public KafkaCompressionCodec getCompressionCodec() {
        return compressionCodec;
    }

    public void setBrokers(String brokers) {
        this.brokers = brokers;
    }
}

