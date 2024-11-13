package de.thriemer.spatial.evaluation;

import de.thriemer.spatial.AerospikeDatabase;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeDatabaseTest extends AbstractDatabaseTest<AerospikeDatabase> {
    @Override
    AerospikeDatabase instantiateDatabase() {
        return new AerospikeDatabase();
    }
}
