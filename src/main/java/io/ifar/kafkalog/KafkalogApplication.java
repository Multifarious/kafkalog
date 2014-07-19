package io.ifar.kafkalog;

import com.google.common.collect.Queues;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.ifar.kafkalog.kafka.LogMessageFactory;
import io.ifar.kafkalog.kafka.LogProducer;
import io.ifar.kafkalog.syslog.IngestService;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class KafkalogApplication extends Application<KafkalogConfiguration> {
    static final Logger LOG = LoggerFactory.getLogger(KafkalogApplication.class);

    public static void main(String[] args) throws Exception {
        new KafkalogApplication().run(args);
    }

    @Override
    public void initialize(Bootstrap<KafkalogConfiguration> kafkalogConfigurationBootstrap) {

    }

    private Producer createProducer(KafkalogConfiguration configuration) {
        Properties props = new Properties();
        props.put("metadata.broker.list", configuration.getBrokers());
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        props.put("request.required.acks", "-1");

        LOG.debug("Creating producer with configuration: " + props);
        return new Producer(new ProducerConfig(props));
    }

    @Override
    public void run(KafkalogConfiguration configuration, Environment environment) throws Exception {
        BlockingQueue<String> lineBuffer = Queues.newLinkedBlockingDeque();
        LogMessageFactory messageFactory = new LogMessageFactory(configuration.getTopic());
        Producer producer = createProducer(configuration);
        LogProducer logProducer = new LogProducer(lineBuffer, messageFactory, producer);
        environment.lifecycle().manage(logProducer);

        IngestService ingestService =
                new IngestService(configuration.getPort(), configuration.getMaxLineLength(), lineBuffer);
        environment.lifecycle().manage(ingestService);

        environment.jersey().disable();
    }
}
