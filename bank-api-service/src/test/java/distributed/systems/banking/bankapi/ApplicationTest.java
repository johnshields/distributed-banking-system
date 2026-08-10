package distributed.systems.banking.bankapi;

import distributed.systems.banking.common.KafkaTopics;
import distributed.systems.banking.common.Transaction;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class ApplicationTest {

    private IncomingTransactionsReader transactionsReader;
    private CustomerAddressDatabase userDb;
    private MockProducer<String, Transaction> mockProducer;

    @BeforeEach
    private void setup() {
        transactionsReader = new IncomingTransactionsReader("test-transactions.txt");
        userDb = new CustomerAddressDatabase("test-user-residence.txt");
        mockProducer = new MockProducer<>(true, null, new StringSerializer(), new Transaction.TransactionSerializer());
    }


    @Test
    void testProducesMessages() throws ExecutionException, InterruptedException {
        Application testApp = new Application();
        testApp.processTransactions(transactionsReader, userDb, mockProducer);

        // 5 input transactions, plus 1 extra send for dkelly9283 which is both high-value and suspicious
        assertEquals(6, mockProducer.history().size());
    }


    @Test
    public void testValidTransactionsTopic() throws ExecutionException, InterruptedException {
        Application testApp = new Application();
        testApp.processTransactions(transactionsReader, userDb, mockProducer);

        ProducerRecord<String, Transaction> record =
                (ProducerRecord<String, Transaction>) mockProducer.history().get(0);

        assertEquals(KafkaTopics.VALID_TRANSACTIONS, record.topic());
    }


    @Test
    public void testHighValueTransactionsTopic() throws ExecutionException, InterruptedException {
        Application testApp = new Application();
        testApp.processTransactions(transactionsReader, userDb, mockProducer);

        ProducerRecord<String, Transaction> record =
                (ProducerRecord<String, Transaction>) mockProducer.history().get(1);

        assertEquals(KafkaTopics.HIGH_VALUE_TRANSACTIONS, record.topic());
    }


    @Test
    public void testSuspiciousTransactionsTopic() throws ExecutionException, InterruptedException {
        Application testApp = new Application();
        testApp.processTransactions(transactionsReader, userDb, mockProducer);

        ProducerRecord<String, Transaction> record = (ProducerRecord<String, Transaction>) mockProducer.history().get(2);

        assertEquals(KafkaTopics.SUSPICIOUS_TRANSACTIONS, record.topic());
    }

    @Test
    public void testMessageContents() throws ExecutionException, InterruptedException {
        Application testApp = new Application();
        testApp.processTransactions(transactionsReader, userDb, mockProducer);
        String testUser = "joe1680";
        String testLocation = "Ireland";
        double testAmount = 128.63;

        Transaction expectedTransaction = new Transaction(testUser, testAmount, testLocation);

        ProducerRecord<String, Transaction> record =
                (ProducerRecord<String, Transaction>) mockProducer.history().get(0);

        assertEquals(expectedTransaction.getUser(), record.key());
        assertEquals(expectedTransaction, record.value());
    }

    @Test
    void buildProducerPropertiesSetsExpectedConfig() {
        Properties properties = Application.buildProducerProperties("localhost:9092");

        assertEquals("localhost:9092", properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("transaction-producer", properties.get(ProducerConfig.CLIENT_ID_CONFIG));
        assertEquals(StringSerializer.class.getName(), properties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(Transaction.TransactionSerializer.class.getName(), properties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }
}