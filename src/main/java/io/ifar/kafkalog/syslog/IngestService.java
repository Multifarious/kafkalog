package io.ifar.kafkalog.syslog;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.dropwizard.lifecycle.Managed;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IngestService implements Managed {
    static final Logger LOG = LoggerFactory.getLogger(IngestService.class);

    private final int port;
    private final int maxLineLength;
    private final BlockingQueue<String> lineBuffer;

    final ExecutorService bossThreadPool = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                    .setNameFormat("kafkalog-boss-%d")
                    .build());

    final ExecutorService workerThreadPool = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                    .setNameFormat("kafkalog-worker-%d")
                    .build());

    private Channel channel;

    public IngestService(int port, int maxLineLength, BlockingQueue<String> lineBuffer) {
        this.port = port;
        this.maxLineLength = maxLineLength;
        this.lineBuffer = lineBuffer;
    }

    @Override
    public void start() throws Exception {
        ServerBootstrap bootstrap =
                new ServerBootstrap(new NioServerSocketChannelFactory(bossThreadPool, workerThreadPool));
        bootstrap.setPipelineFactory(new IngestPipelineFactory(maxLineLength, lineBuffer));

        SocketAddress sockAddr = new InetSocketAddress(port);
        channel = bootstrap.bind(sockAddr);
        LOG.info("Started syslog listener on {}", sockAddr);
    }

    @Override
    public void stop() throws Exception {
        channel.close();
        bossThreadPool.shutdown();
        workerThreadPool.shutdown();
    }
}
