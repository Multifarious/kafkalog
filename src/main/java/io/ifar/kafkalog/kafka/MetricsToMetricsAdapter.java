package io.ifar.kafkalog.kafka;


import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public class MetricsToMetricsAdapter implements MetricsReporter {

    public static final String METRIC_PREFIX = "metric.prefix";
    public static MetricRegistry registry = new MetricRegistry();

    private ConcurrentMap<MetricName, Double> values;
    private String prefix;
    private Lock registryLock = new ReentrantLock(true);

    @Override
    public void init(List<KafkaMetric> metrics) {
        Preconditions.checkNotNull(registry, "Expected a MetricsRegistry to be supplied prior to initialization.");

        values = new ConcurrentHashMap<>();

        for (final KafkaMetric metric : metrics) {
            registry.register(name(metric.metricName()), new Gauge<Double>() {
                @Override
                public Double getValue() {
                    return metric.value();
                }
            });
        }
    }

    @Override
    public void metricChange(final KafkaMetric metric) {
        if (values.put(metric.metricName(), metric.value()) == null) {
            registryLock.lock();
            try {
                if(!registry.getGauges().containsKey(name(metric.metricName()))) {
                    registry.register(name(metric.metricName()), new Gauge<Double>() {
                        @Override
                        public Double getValue() {
                            return values.get(metric.metricName());
                        }
                    });
                }
            } finally {
                registryLock.unlock();
            }
        }
    }

    private String name(MetricName metricName) {
        return MetricRegistry.name("kafka", prefix, metricName.group(), metricName.name());
    }

    @Override
    public void close() {
        for (MetricName name : values.keySet()) {
            registry.remove(name(name));
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.prefix = Optional.fromNullable(configs.get(METRIC_PREFIX)).or(
                "producer"
        ).toString();
    }
}
