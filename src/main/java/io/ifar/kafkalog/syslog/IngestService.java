package io.ifar.kafkalog.syslog;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.dropwizard.lifecycle.Managed;
import io.ifar.kafkalog.kafka.LogMessageFactory;
import io.ifar.kafkalog.kafka.ManagedKafkaProducer;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.LinkOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IngestService implements Managed {
    static final Logger LOG = LoggerFactory.getLogger(IngestService.class);

    private final int port;
    private final int maxLineLength;

    final ExecutorService bossThreadPool = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                    .setNameFormat("kafkalog-boss-%d")
                    .build());

    final ExecutorService workerThreadPool = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                    .setNameFormat("kafkalog-worker-%d")
                    .build());
    private final MetricRegistry metricRegistry;
    private final LogMessageFactory messageFactory;
    private final ManagedKafkaProducer producer;

    private Channel channel;

    public IngestService(int port, int maxLineLength, ManagedKafkaProducer producer, LogMessageFactory messageFactory,
                         MetricRegistry metricRegistry)
    {
        this.port = port;
        this.maxLineLength = maxLineLength;
        this.producer = producer;
        this.messageFactory = messageFactory;
        this.metricRegistry = metricRegistry;
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting syslog listener ({}).", IngestService.class.getSimpleName());
        ServerBootstrap bootstrap =
                new ServerBootstrap(new NioServerSocketChannelFactory(bossThreadPool, workerThreadPool));
        bootstrap.setPipelineFactory(new IngestPipelineFactory(maxLineLength, producer, messageFactory,
                metricRegistry));

        SocketAddress sockAddr = new InetSocketAddress(port);
        channel = bootstrap.bind(sockAddr);
        LOG.info("Started syslog listener ({}) on {}", IngestService.class.getSimpleName(), sockAddr);
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Stopping syslog listener ({}).", IngestService.class.getSimpleName());
                channel.close();
        bossThreadPool.shutdown();
        workerThreadPool.shutdown();
        LOG.info("Stopped syslog listener ({}).", IngestService.class.getSimpleName());
    }
}
