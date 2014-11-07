package io.ifar.kafkalog.syslog;

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

    private final BlockingQueue<String> lineBuffer;

    public IngestHandler(BlockingQueue<String> lineBuffer) {
        this.lineBuffer = lineBuffer;
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
            // Ideally we'd have metrics to get JMX (etc) stats to see how often we get this
            LOG.warn("BlockingQueue full, can not queue message yet; need to wait a bit to let it drain...");
            Thread.sleep(500L);
        }
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
