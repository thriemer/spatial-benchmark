package de.thriemer.spatial.evaluation;

import de.thriemer.spatial.ADXDatabase;
import de.thriemer.spatial.ADXVanillaDatabase;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AdxVanillaDatabaseTest extends AbstractDatabaseTest<ADXVanillaDatabase> {


    @Override
    ADXVanillaDatabase instantiateDatabase() {
        return new ADXVanillaDatabase();
    }
}
