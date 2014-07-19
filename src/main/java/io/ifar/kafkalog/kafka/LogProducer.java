package io.ifar.kafkalog.kafka;

import io.dropwizard.lifecycle.Managed;
import kafka.javaapi.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class LogProducer implements Managed {
    static final Logger LOG = LoggerFactory.getLogger(LogProducer.class);

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final BlockingQueue<String> lineBuffer;
    private final LogMessageFactory messageFactory;
    private final Producer producer;

    private Future runnable;

    public LogProducer(BlockingQueue<String> lineBuffer, LogMessageFactory messageFactory, Producer producer) {
        this.lineBuffer = lineBuffer;
        this.messageFactory = messageFactory;
        this.producer = producer;
    }

    @Override
    public void start() throws Exception {
        runnable = executor.submit(new LogProducerRunnable());
    }

    @Override
    public void stop() throws Exception {
        runnable.cancel(true);
    }

    private final class LogProducerRunnable implements Runnable {
        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            while (true) {
                String line = lineBuffer.poll();
                producer.send(messageFactory.create(line));

                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
            }
        }
    }
}
