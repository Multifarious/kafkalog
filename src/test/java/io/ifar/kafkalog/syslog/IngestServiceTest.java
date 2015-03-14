package io.ifar.kafkalog.syslog;

import com.google.common.io.Files;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

public class IngestServiceTest {
    static final String DUMB_TEST_MESSAGE = "TEST MESSAGE FROM OUTER SPACE";
    private static TestingServer zk;
    private static KafkaServerStartable kafkaServer;

    private static final Logger LOG = LoggerFactory.getLogger(IngestServiceTest.class);
    private static int syslogPort;

    @BeforeClass
    public static void setup() throws Exception {
        File temp = Files.createTempDir();

        zk = new TestingServer(choosePort());
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("zookeeper.connect", zk.getConnectString());
        kafkaProps.setProperty("broker.id", "0");
        kafkaProps.setProperty("port", String.format("%d", choosePort()));
        kafkaProps.setProperty("log.dirs", temp.getPath());
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProps);

        kafkaServer = new KafkaServerStartable(kafkaConfig);
        kafkaServer.startup();

        syslogPort = choosePort();
    }

    @AfterClass
    public static void teardown() {
        try {
            kafkaServer.shutdown();
        } catch (Exception e) {

        }
        try {
            zk.close();
        } catch (Exception e) {

        }
    }

    private void sendMessage(String message) throws IOException {
        Socket clientSocket = new Socket("localhost", syslogPort);
        DataOutputStream output = new DataOutputStream(clientSocket.getOutputStream());
        output.writeBytes(message + '\n');
        clientSocket.close();
    }

    @Test
    public void singleMessage() throws Exception {
        //IngestService ingestService = new IngestService(PORT, 8192, lineBuffer);
        //ingestService.start();
        //sendMessage(DUMB_TEST_MESSAGE);
        //assertEquals(DUMB_TEST_MESSAGE, lineBuffer.take());
        //ingestService.stop();
    }

    private static int choosePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to allocate a port.", e);
        }
    }
}
