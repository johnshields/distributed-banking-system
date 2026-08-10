package distributed.systems.banking.bankapi;

import distributed.systems.banking.common.Transaction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IncomingTransactionsReaderTest {

    private final IncomingTransactionsReader reader = new IncomingTransactionsReader("test-transactions.txt");

    @Test
    void hasNextIsTrueBeforeExhaustingTransactions() {
        assertTrue(reader.hasNext());
    }

    @Test
    void nextParsesFieldsInOrder() {
        Transaction first = reader.next();

        assertEquals("joe1680", first.getUser());
        assertEquals("Ireland", first.getTransactionLocation());
        assertEquals(128.63, first.getAmount());
    }

    @Test
    void hasNextIsFalseAfterAllTransactionsConsumed() {
        while (reader.hasNext()) {
            reader.next();
        }

        assertFalse(reader.hasNext());
    }
}
