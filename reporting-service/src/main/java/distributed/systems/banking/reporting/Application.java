package distributed.systems.banking.reporting;

import distributed.systems.banking.common.Transaction;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * Reporting Service
 * should be getting all transactions
 */
public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    // using an array list so the size of the array cannot be modified
    public static final List<String> TOPICS = new ArrayList<>(Arrays.asList("high-value-transactions", "valid-transactions", "suspicious-transactions"));
    private static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";

    public static void main(String[] args) {
        String consumerGroup = "transactions-group";
        if (args.length == 1) {
            consumerGroup = args[0];
        }

        logger.info("Consumer is part of consumer group {}", consumerGroup);
        Consumer<String, Transaction> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);
        consumeMessages(TOPICS, kafkaConsumer);
    }

    public static void consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {
        kafkaConsumer.subscribe(topics);
        logger.info("Record of all transactions");

        while (true) {
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, Transaction> transactionRecord : consumerRecords) {
                logger.info("Received record(key: {}, value: {}, partition: {}, offset: {})",
                        transactionRecord.key(), transactionRecord.value(), transactionRecord.partition(), transactionRecord.offset());

                recordTransactionForReporting(transactionRecord.topic(), transactionRecord.value());
            }
            kafkaConsumer.commitAsync();
        }
    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        logger.info("Transaction consumer");
        return new KafkaConsumer<>(properties);
    }

    private static void recordTransactionForReporting(String topic, Transaction transaction) {
        // Print a different message depending on whether transaction is suspicious or valid

        // valid transactions
        if (topic.equalsIgnoreCase("valid-transactions")) {
            logger.info("valid transaction from {}", transaction);
        }
        // suspicious transactions
        else if (topic.equalsIgnoreCase("suspicious-transactions")) {
            logger.info("suspicious transaction from {}", transaction);
        }
        // high value transactions
        else if (topic.equalsIgnoreCase("high-value-transactions")) {
            logger.info("high value transaction from {}", transaction);
        }
    }

}
