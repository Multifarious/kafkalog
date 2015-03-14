package io.ifar.kafkalog.kafka;


import com.codahale.metrics.health.HealthCheck;
import com.google.common.collect.ImmutableList;
import io.dropwizard.lifecycle.Managed;
import org.apache.kafka.clients.producer.*;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class ManagedKafkaProducer implements Managed, Producer<String,String> {
    static final Logger LOG = LoggerFactory.getLogger(ManagedKafkaProducer.class);
    private boolean running;
    private KafkaProducer<String,String> producer;
    private final HashMap<String, Object> configMap;

    public ManagedKafkaProducer(KafkaProducerConfiguration config) {

        this.configMap = new HashMap<>();
        configMap.put(BOOTSTRAP_SERVERS_CONFIG, ImmutableList.copyOf(config.getBrokers().split("[, ]+")));
        configMap.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(COMPRESSION_TYPE_CONFIG, config.getCompressionCodec().name());
        configMap.put(ACKS_CONFIG, config.getRequestRequiredAcks().configurationValue());

        if (config.getBufferMemory().isPresent()) {
            configMap.put(BUFFER_MEMORY_CONFIG, config.getBufferMemory().get());
        }
        if (config.getRetries().isPresent()) {
            configMap.put(RETRIES_CONFIG, config.getRetries().get());
        }
        if (config.getBatchSize().isPresent()) {
            configMap.put(BATCH_SIZE_CONFIG, config.getBatchSize().get());
        }
        if (config.getClientId().isPresent()) {
            configMap.put(CLIENT_ID_CONFIG, config.getClientId().get());
        }
        if (config.getLingerMillis().isPresent()) {
            configMap.put(LINGER_MS_CONFIG, config.getLingerMillis().get());
        }
        if (config.getMaxRequestSize().isPresent()) {
            configMap.put(MAX_REQUEST_SIZE_CONFIG, config.getMaxRequestSize().get());
        }
        if (config.getBlockOnBufferFull().isPresent()) {
            configMap.put(BLOCK_ON_BUFFER_FULL_CONFIG, config.getBlockOnBufferFull().get());
        }
        if (config.getReceiveBufferBytes().isPresent()) {
            configMap.put(RECEIVE_BUFFER_CONFIG, config.getReceiveBufferBytes().get());
        }
        if (config.getSendBufferBytes().isPresent()) {
            configMap.put(SEND_BUFFER_CONFIG, config.getSendBufferBytes().get());
        }
        if (config.getTimeoutMillis().isPresent()) {
            configMap.put(TIMEOUT_CONFIG, config.getTimeoutMillis().get());
        }
        if (config.getRetryBackoffMillis().isPresent()) {
            configMap.put(RETRY_BACKOFF_MS_CONFIG, config.getRetryBackoffMillis().get());
        }
        if (config.getReconnectBackoffMillis().isPresent()) {
            configMap.put(RECONNECT_BACKOFF_MS_CONFIG, config.getReconnectBackoffMillis().get());
        }
        if (config.getMetadataFetchTimeoutMillis().isPresent()) {
            configMap.put(METADATA_FETCH_TIMEOUT_CONFIG, config.getMetadataFetchTimeoutMillis().get());
        }
        if (config.getMetadataMaxAgeMillis().isPresent()) {
            configMap.put(METADATA_MAX_AGE_CONFIG, config.getMetadataMaxAgeMillis().get());
        }
        if (config.getRegisterKafkaMetricsWithDropwizardMetrics()) {
            configMap.put(METRIC_REPORTER_CLASSES_CONFIG, ImmutableList.of(MetricsToMetricsAdapter.class.getName()));
        }
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String,String> record) {
        return producer.send(record);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String,String> record, Callback callback) {
        return producer.send(record, callback);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return producer.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return producer.metrics();
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting Kafka producer.");
        this.running = true;
        this.producer = new KafkaProducer<>(configMap);
        LOG.info("Kafka producer started.");
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Stopping Kafka producer...");
        if(producer != null) {
            // blocks until flushed.
            producer.close();
        }
        producer = null;
        this.running = false;
        LOG.info("Kafka producer stopped.");
    }

    private HealthCheck healthCheck = new HealthCheck() {
        @Override
        protected Result check() throws Exception {
            return running
                    ? HealthCheck.Result.healthy()
                    : HealthCheck.Result.unhealthy("Not running.");
        }
    };

    public HealthCheck healthCheck() {
        return healthCheck;
    }

}

