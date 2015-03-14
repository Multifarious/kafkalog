package io.ifar.kafkalog.syslog;

import com.codahale.metrics.MetricRegistry;
import io.ifar.kafkalog.kafka.LogMessageFactory;
import io.ifar.kafkalog.kafka.ManagedKafkaProducer;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.util.CharsetUtil;

public class IngestPipelineFactory implements ChannelPipelineFactory {
    private final int maxLineLength;
    private final ManagedKafkaProducer producer;
    private final LogMessageFactory messageFactory;
    private final MetricRegistry metricRegistry;

    public IngestPipelineFactory(int maxLineLength, ManagedKafkaProducer producer, LogMessageFactory messageFactory,
                                 MetricRegistry metricRegistry)
    {
        this.producer = producer;
        this.messageFactory = messageFactory;
        this.maxLineLength = maxLineLength;
        this.metricRegistry = metricRegistry;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();

        pipeline.addLast("frameDecoder", new DelimiterBasedFrameDecoder(maxLineLength, Delimiters.lineDelimiter()));
        pipeline.addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8));
        pipeline.addLast("handler", new IngestHandler(producer, messageFactory, metricRegistry));

        return pipeline;
    }
}
