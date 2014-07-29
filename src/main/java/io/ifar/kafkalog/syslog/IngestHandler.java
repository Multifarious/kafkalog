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
        if (e.getMessage() instanceof String) {
            String message = (String)e.getMessage();
            LOG.debug("Received message: {}", message);
            lineBuffer.put(message);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        LOG.debug("Caught exception", e);

        if (ctx.getChannel() != null && !(ctx.getChannel() instanceof DatagramChannel)) {
            ctx.getChannel().close();
        }
    }
}
