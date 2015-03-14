package io.ifar.kafkalog;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.ifar.kafkalog.kafka.MetricsToMetricsAdapter;
import io.ifar.kafkalog.kafka.LogMessageFactory;
import io.ifar.kafkalog.kafka.ManagedKafkaProducer;
import io.ifar.kafkalog.syslog.IngestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkalogApplication extends Application<KafkalogConfiguration> {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) throws Exception {
        new KafkalogApplication().run(args);
    }

    @Override
    public void initialize(Bootstrap<KafkalogConfiguration> kafkalogConfigurationBootstrap) { }

    @Override
    public void run(KafkalogConfiguration configuration, Environment environment) throws Exception {
        LogMessageFactory messageFactory = new LogMessageFactory(configuration.getTopic());

        ManagedKafkaProducer producer = new ManagedKafkaProducer(configuration.getKafkaProducerConfiguration());
        if (configuration.getKafkaProducerConfiguration().getRegisterKafkaMetricsWithDropwizardMetrics()) {
            MetricsToMetricsAdapter.registry = environment.metrics();
        }
        environment.healthChecks().register("kafkaProducer", producer.healthCheck());
        environment.lifecycle().manage(producer);

        IngestService ingestService =
                new IngestService(configuration.getPort(), configuration.getMaxLineLength(), producer, messageFactory,
                        environment.metrics());
        environment.lifecycle().manage(ingestService);

        environment.jersey().disable();
    }
}
