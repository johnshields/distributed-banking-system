package distributed.systems.banking.common;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class TransactionTest {

    @Test
    void equalsReturnsTrueForSameFieldValues() {
        Transaction a = new Transaction("joe1680", 128.63, "Ireland");
        Transaction b = new Transaction("joe1680", 128.63, "Ireland");

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void equalsReturnsFalseWhenAmountDiffers() {
        Transaction a = new Transaction("joe1680", 128.63, "Ireland");
        Transaction b = new Transaction("joe1680", 999.99, "Ireland");

        assertNotEquals(a, b);
    }

    @Test
    void serializeThenDeserializeRoundTripsToAnEqualTransaction() {
        Transaction original = new Transaction("joe1680", 128.63, "Ireland");
        Transaction.TransactionSerializer serializer = new Transaction.TransactionSerializer();
        Transaction.TransactionDeserializer deserializer = new Transaction.TransactionDeserializer();

        byte[] serialized = serializer.serialize("valid-transactions", original);
        Transaction result = deserializer.deserialize("valid-transactions", serialized);

        assertEquals(original, result);
    }

    @Test
    void deserializeReturnsNullForMalformedData() {
        Transaction.TransactionDeserializer deserializer = new Transaction.TransactionDeserializer();

        Transaction result = deserializer.deserialize("valid-transactions", "not valid json".getBytes(StandardCharsets.UTF_8));

        assertNull(result);
    }
}
