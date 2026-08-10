package distributed.systems.banking.common;

public final class KafkaConfig {

    public static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094");

    private KafkaConfig() {
    }
}
