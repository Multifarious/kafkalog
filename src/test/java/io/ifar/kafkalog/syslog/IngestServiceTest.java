package io.ifar.kafkalog.syslog;

import com.google.common.collect.Queues;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;

public class IngestServiceTest {
    static final int PORT = 5140;
    static final String DUMB_TEST_MESSAGE = "TEST MESSAGE FROM OUTER SPACE";

    private void sendMessage(String message) throws IOException {
        Socket clientSocket = new Socket("localhost", PORT);
        DataOutputStream output = new DataOutputStream(clientSocket.getOutputStream());
        output.writeBytes(message + '\n');
        clientSocket.close();
    }

    @Test
    public void singleMessage() throws Exception {
        BlockingQueue<String> lineBuffer = Queues.newLinkedBlockingDeque();
        IngestService ingestService = new IngestService(PORT, 8192, lineBuffer);
        ingestService.start();
        sendMessage(DUMB_TEST_MESSAGE);
        assertEquals(DUMB_TEST_MESSAGE, lineBuffer.take());
        ingestService.stop();
    }
}
