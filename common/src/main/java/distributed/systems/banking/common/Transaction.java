package distributed.systems.banking.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class Transaction {
    private String user;
    private double amount;
    private String transactionLocation;

    public Transaction() {
    }

    public Transaction(String user, double amount, String transactionLocation) {
        this.user = user;
        this.amount = amount;
        this.transactionLocation = transactionLocation;
    }

    public String getUser() {
        return user;
    }

    public double getAmount() {
        return amount;
    }

    public String getTransactionLocation() {
        return transactionLocation;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public void setTransactionLocation(String transactionLocation) {
        this.transactionLocation = transactionLocation;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "user='" + user + '\'' +
                ", amount=" + amount +
                ", transactionLocation='" + transactionLocation + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return Double.compare(that.amount, amount) == 0 &&
                Objects.equals(user, that.user) &&
                Objects.equals(transactionLocation, that.transactionLocation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, amount, transactionLocation);
    }

    /**
     * Kafka Serializer implementation.
     * Serializes a Transaction to JSON so it can be sent to a Kafka Topic
     */
    public static class TransactionSerializer implements Serializer<Transaction> {
        private static final Logger logger = LoggerFactory.getLogger(TransactionSerializer.class);
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public byte[] serialize(String topic, Transaction data) {
            try {
                return MAPPER.writeValueAsString(data).getBytes(StandardCharsets.UTF_8);
            } catch (Exception e) {
                logger.error("Failed to serialize transaction for topic {}", topic, e);
                return null;
            }
        }
    }

    /**
     * Kafka Deserializer implementation.
     * Deserializes a Transaction from JSON to a {@link Transaction} object
     */
    public static class TransactionDeserializer implements Deserializer<Transaction> {
        private static final Logger logger = LoggerFactory.getLogger(TransactionDeserializer.class);
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public Transaction deserialize(String topic, byte[] data) {
            try {
                return MAPPER.readValue(data, Transaction.class);
            } catch (Exception e) {
                logger.error("Failed to deserialize transaction for topic {}", topic, e);
                return null;
            }
        }
    }
}
