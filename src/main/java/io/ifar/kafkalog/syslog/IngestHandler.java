package io.ifar.kafkalog.syslog;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.ifar.kafkalog.KafkalogApplication;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class IngestHandler extends SimpleChannelHandler {
    static final Logger LOG = LoggerFactory.getLogger(IngestHandler.class);

    private final Meter receivedMessagesMeter;
    private final Meter queueFullMeter;

    private final BlockingQueue<String> lineBuffer;

    public IngestHandler(BlockingQueue<String> lineBuffer,
                         MetricRegistry metrics) {
        this.lineBuffer = lineBuffer;
        this.receivedMessagesMeter = metrics.meter(this.getClass().getSimpleName() + "-" + "receivedMessages");
        this.queueFullMeter = metrics.meter(this.getClass().getSimpleName() + "-" + "queueFull");
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (!(msg instanceof String)) { // sanity check
            LOG.warn("Received message of unrecognized type: "+((msg == null) ? "[null]" : msg.getClass().getName())+" received");
            return;
        }
        String message = (String)e.getMessage();
        LOG.debug("Received message: {}", message);
        while (!lineBuffer.offer(message)) {
            queueFullMeter.mark();
            LOG.warn("BlockingQueue full, can not queue message yet; need to wait a bit to let it drain...");
            Thread.sleep(500L);
        }
        receivedMessagesMeter.mark();
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
