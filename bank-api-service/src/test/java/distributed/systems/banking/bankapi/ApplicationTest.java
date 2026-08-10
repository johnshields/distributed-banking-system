package distributed.systems.banking.bankapi;

import distributed.systems.banking.common.Transaction;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class ApplicationTest {

    private static final String SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions";
    private static final String VALID_TRANSACTIONS_TOPIC = "valid-transactions";
    private static final String HIGH_VALUE_TRANSACTIONS_TOPIC = "high-value-transactions";
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

        assertEquals(VALID_TRANSACTIONS_TOPIC, record.topic());
    }


    @Test
    public void testHighValueTransactionsTopic() throws ExecutionException, InterruptedException {
        Application testApp = new Application();
        testApp.processTransactions(transactionsReader, userDb, mockProducer);

        ProducerRecord<String, Transaction> record =
                (ProducerRecord<String, Transaction>) mockProducer.history().get(1);

        assertEquals(HIGH_VALUE_TRANSACTIONS_TOPIC, record.topic());
    }


    @Test
    public void testSuspiciousTransactionsTopic() throws ExecutionException, InterruptedException {
        Application testApp = new Application();
        testApp.processTransactions(transactionsReader, userDb, mockProducer);

        ProducerRecord<String, Transaction> record = (ProducerRecord<String, Transaction>) mockProducer.history().get(2);

        assertEquals(SUSPICIOUS_TRANSACTIONS_TOPIC, record.topic());
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
}