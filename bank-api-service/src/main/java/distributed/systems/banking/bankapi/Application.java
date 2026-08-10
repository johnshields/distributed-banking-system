package distributed.systems.banking.bankapi;

import distributed.systems.banking.common.Transaction;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Banking API Service
 * needs to produce messages to the topics
 */
public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    // suspicious-transactions with 2 partitions + replication factor of 3
    private static final String ST_TOPIC = "suspicious-transactions";
    // valid-transactions with 3 partitions + replication factor of 3
    private static final String VT_TOPIC = "valid-transactions";
    private static final String HVT_TOPIC = "high-value-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Producer<String, Transaction> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);

        IncomingTransactionsReader user = new IncomingTransactionsReader();
        CustomerAddressDatabase transactionLocation = new CustomerAddressDatabase();

        try {
            processTransactions(user, transactionLocation, kafkaProducer);
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Failed to process transactions", e);
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
        logger.info("process transactions");
    }

    public static void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                           CustomerAddressDatabase customerAddressDatabase,
                                           Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException {
        while (incomingTransactionsReader.hasNext()) {
            Transaction transaction = incomingTransactionsReader.next();
            String userResidence = customerAddressDatabase.getUserResidence(transaction.getUser());
            String id = transaction.getUser();

            // high value transactions
            if (transaction.getAmount() > 1000) {
                ProducerRecord<String, Transaction> transactionRecord =
                        new ProducerRecord<>(HVT_TOPIC, id, transaction);

                RecordMetadata highvTranMetadata = kafkaProducer.send(transactionRecord).get();
                logger.info("Record from (key: {}, value: {}) was sent to (partition: {}, offset: {})",
                        transactionRecord.key(), transactionRecord.value(), highvTranMetadata.partition(), highvTranMetadata.offset());
            }
            // valid transactions
            if (userResidence.equalsIgnoreCase(transaction.getTransactionLocation())) {
                ProducerRecord<String, Transaction> transactionRecord =
                        new ProducerRecord<>(VT_TOPIC, id, transaction);

                RecordMetadata validTranMetadata = kafkaProducer.send(transactionRecord).get();
                logger.info("Record from (key: {}, value: {}) was sent to (partition: {}, offset: {})",
                        transactionRecord.key(), transactionRecord.value(), validTranMetadata.partition(), validTranMetadata.offset());
            }
            // suspicious transactions
            else {
                ProducerRecord<String, Transaction> transactionRecord =
                        new ProducerRecord<>(ST_TOPIC, id, transaction);

                RecordMetadata suspiciousTranMetadata = kafkaProducer.send(transactionRecord).get();
                logger.info("Record from (key: {}, value: {}) was sent to (partition: {}, offset: {})",
                        transactionRecord.key(), transactionRecord.value(), suspiciousTranMetadata.partition(), suspiciousTranMetadata.offset());
            }
            logger.info("Record of transaction");
        }
    }

    public static Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "transaction-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName());

        logger.info("Transaction producer");
        return new KafkaProducer<>(properties);
    }

}
