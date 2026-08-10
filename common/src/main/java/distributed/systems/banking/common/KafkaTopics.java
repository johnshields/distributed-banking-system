package distributed.systems.banking.common;

public final class KafkaTopics {

    public static final String VALID_TRANSACTIONS = "valid-transactions";
    public static final String SUSPICIOUS_TRANSACTIONS = "suspicious-transactions";
    public static final String HIGH_VALUE_TRANSACTIONS = "high-value-transactions";

    private KafkaTopics() {
    }
}
