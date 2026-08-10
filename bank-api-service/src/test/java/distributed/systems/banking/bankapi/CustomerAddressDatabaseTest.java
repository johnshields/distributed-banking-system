package distributed.systems.banking.bankapi;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CustomerAddressDatabaseTest {

    private final CustomerAddressDatabase db = new CustomerAddressDatabase("test-user-residence.txt");

    @Test
    void getUserResidenceReturnsResidenceForKnownUser() {
        assertEquals("Dubai", db.getUserResidence("dkelly9283"));
    }

    @Test
    void getUserResidenceThrowsForUnknownUser() {
        assertThrows(RuntimeException.class, () -> db.getUserResidence("nonexistent-user"));
    }
}
