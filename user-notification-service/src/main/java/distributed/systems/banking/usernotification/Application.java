package distributed.systems.banking.usernotification;

import distributed.systems.banking.common.Transaction;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * User Notification Service
 * should only get suspicious transactions
 */
public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private static final String ST_TOPIC = "suspicious-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";

    public static void main(String[] args) {
        // Kafka consumer group IDs cannot contain spaces
        String consumerGroup = "suspicious-transactions-group";
        if (args.length == 1) {
            consumerGroup = args[0];
        }

        logger.info("Consumer is part of consumer group {}", consumerGroup);
        Consumer<String, Transaction> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);
        consumeMessages(ST_TOPIC, kafkaConsumer);
    }

    public static void consumeMessages(String topic, Consumer<String, Transaction> kafkaConsumer) {
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        logger.info("Record of suspicious transactions");

        while (true) {
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, Transaction> transactionRecord : consumerRecords) {
                logger.info("Received record(key: {}, value: {}, partition: {}, offset: {})",
                        transactionRecord.key(), transactionRecord.value(), transactionRecord.partition(), transactionRecord.offset());

                sendUserNotification(transactionRecord.value());
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

    private static void sendUserNotification(Transaction transaction) {
        logger.info(transaction.toString());
    }

}
