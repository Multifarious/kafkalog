package io.ifar.kafkalog.syslog;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.util.CharsetUtil;

import java.util.concurrent.BlockingQueue;

public class IngestPipelineFactory implements ChannelPipelineFactory {
    private final int maxLineLength;
    private final BlockingQueue<String> lineBuffer;

    public IngestPipelineFactory(int maxLineLength, BlockingQueue<String> lineBuffer) {
        this.maxLineLength = maxLineLength;
        this.lineBuffer = lineBuffer;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();

        pipeline.addLast("frameDecoder", new DelimiterBasedFrameDecoder(maxLineLength, Delimiters.lineDelimiter()));
        pipeline.addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8));
        pipeline.addLast("handler", new IngestHandler(lineBuffer));

        return pipeline;
    }
}
