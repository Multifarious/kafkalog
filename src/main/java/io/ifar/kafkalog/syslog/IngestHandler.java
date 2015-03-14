package io.ifar.kafkalog.syslog;

import com.codahale.metrics.MetricRegistry;
import io.ifar.kafkalog.kafka.LogMessageFactory;
import io.ifar.kafkalog.kafka.ManagedKafkaProducer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IngestHandler extends SimpleChannelHandler {
    static final Logger LOG = LoggerFactory.getLogger(IngestHandler.class);
    private final ManagedKafkaProducer producer;
    private final LogMessageFactory messageFactory;

    public IngestHandler(ManagedKafkaProducer producer, LogMessageFactory messageFactory, MetricRegistry metricRegistry) {
        this.producer = producer;
        this.messageFactory = messageFactory;

    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (!(msg instanceof String)) { // sanity check
            LOG.warn("Received message of unrecognized type: "+((msg == null) ? "[null]" : msg.getClass().getName())+" received");
            return;
        }
        String line = (String)e.getMessage();

        // The Kafka producer can/should provide backpressure here.
        producer.send(messageFactory.create(line));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        LOG.warn("Caught exception during channel read", e);

        if (ctx.getChannel() != null && !(ctx.getChannel() instanceof DatagramChannel)) {
            LOG.warn("Closing channel of IngestHandler");
            ctx.getChannel().close();
        }
    }
}
