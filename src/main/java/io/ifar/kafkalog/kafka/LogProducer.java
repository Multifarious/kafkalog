package io.ifar.kafkalog.kafka;

import com.codahale.metrics.health.HealthCheck.Result;
import io.dropwizard.lifecycle.Managed;
import kafka.javaapi.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class LogProducer implements Managed {
    static final Logger LOG = LoggerFactory.getLogger(LogProducer.class);
    static final long BUFFER_POLL_TIMEOUT = 1000L;

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

    public Result healthCheck() {
        if (runnable == null) {
            return Result.unhealthy("Producer task not yet started");
        }
        if (runnable.isCancelled()) {
            return Result.unhealthy("Producer task cancelled");
        }
        if (runnable.isDone()) {
            return Result.unhealthy("Producer task not running any more");
        }

        return Result.healthy();
    }

    @Override
    public void start() throws Exception {
        LOG.info("Submitting producer task to executor");
        runnable = executor.submit(new LogProducerRunnable());
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Cancelling producer task");
        runnable.cancel(true);
    }

    private final class LogProducerRunnable implements Runnable {
        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            while (true) {
                boolean interrupted = false;

                try {
                    String line = lineBuffer.poll(BUFFER_POLL_TIMEOUT, TimeUnit.MILLISECONDS);
                    if (line != null) {
                        LOG.debug("Sending buffered data: {}", line);
                        producer.send(messageFactory.create(line));
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    LOG.warn("Uncaught problem in LogProducerRunnable: "+e.getMessage(), e);
                }

                if (interrupted || Thread.currentThread().isInterrupted()) {
                    LOG.info("LogProducer task interrupted. Exiting.");
                    return;
                }
            }
        }
    }
}
