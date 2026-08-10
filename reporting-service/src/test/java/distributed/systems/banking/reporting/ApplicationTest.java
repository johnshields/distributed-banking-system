package distributed.systems.banking.reporting;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import distributed.systems.banking.common.KafkaTopics;
import distributed.systems.banking.common.Transaction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ApplicationTest {

    private ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    void setup() {
        Logger logger = (Logger) LoggerFactory.getLogger(Application.class);
        logAppender = new ListAppender<>();
        logAppender.start();
        logger.addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        Logger logger = (Logger) LoggerFactory.getLogger(Application.class);
        logger.detachAppender(logAppender);
    }

    private String lastLogMessage() {
        return logAppender.list.get(logAppender.list.size() - 1).getFormattedMessage();
    }

    @Test
    void recordTransactionForReportingLogsValidTransaction() {
        Transaction transaction = new Transaction("joe1680", 128.63, "Ireland");

        Application.recordTransactionForReporting(KafkaTopics.VALID_TRANSACTIONS, transaction);

        assertTrue(lastLogMessage().startsWith("valid transaction from"));
    }

    @Test
    void recordTransactionForReportingLogsSuspiciousTransaction() {
        Transaction transaction = new Transaction("dkelly9283", 1653.32, "China");

        Application.recordTransactionForReporting(KafkaTopics.SUSPICIOUS_TRANSACTIONS, transaction);

        assertTrue(lastLogMessage().startsWith("suspicious transaction from"));
    }

    @Test
    void recordTransactionForReportingLogsHighValueTransaction() {
        Transaction transaction = new Transaction("dkelly9283", 1653.32, "China");

        Application.recordTransactionForReporting(KafkaTopics.HIGH_VALUE_TRANSACTIONS, transaction);

        assertTrue(lastLogMessage().startsWith("high value transaction from"));
    }

    @Test
    void recordTransactionForReportingIgnoresUnknownTopic() {
        Transaction transaction = new Transaction("joe1680", 128.63, "Ireland");

        Application.recordTransactionForReporting("unrelated-topic", transaction);

        assertTrue(logAppender.list.isEmpty());
    }

    @Test
    void buildConsumerPropertiesSetsExpectedConfig() {
        Properties properties = Application.buildConsumerProperties("localhost:9092", "transactions-group");

        assertEquals("localhost:9092", properties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("transactions-group", properties.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(StringDeserializer.class.getName(), properties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(Transaction.TransactionDeserializer.class.getName(), properties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertEquals(false, properties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }
}
