package de.thriemer.spatial.evaluation;

import de.thriemer.spatial.OpenSearchDatabase;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OpenSearchDatabaseTest extends AbstractDatabaseTest<OpenSearchDatabase> {
    @Override
    OpenSearchDatabase instantiateDatabase() {
        return new OpenSearchDatabase();
    }
}
