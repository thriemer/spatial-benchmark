package de.thriemer.spatial.evaluation;

import de.thriemer.spatial.ADXDatabase;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AdxDatabaseTest extends AbstractDatabaseTest<ADXDatabase> {


    @Override
    ADXDatabase instantiateDatabase() {
        return new ADXDatabase();
    }
}
