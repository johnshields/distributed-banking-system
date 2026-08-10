package distributed.systems.banking.usernotification;

import distributed.systems.banking.common.Transaction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ApplicationTest {

    @Test
    void buildConsumerPropertiesSetsExpectedConfig() {
        Properties properties = Application.buildConsumerProperties("localhost:9092", "suspicious-transactions-group");

        assertEquals("localhost:9092", properties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("suspicious-transactions-group", properties.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(StringDeserializer.class.getName(), properties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(Transaction.TransactionDeserializer.class.getName(), properties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertEquals(false, properties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }
}
