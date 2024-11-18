package de.thriemer.spatial.benchmark.scenarios;

import de.thriemer.spatial.benchmark.DataGenerator;
import de.thriemer.spatial.benchmark.DataPoint;
import de.thriemer.spatial.benchmark.Page;
import de.thriemer.spatial.framework.Blackhole;
import de.thriemer.spatial.framework.DatabaseAbstraction;
import de.thriemer.spatial.framework.Scenario;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class PaginationScenarioFirstRun extends Scenario<Integer> {
    private final String tableName = Parameters.OSM_DATA_TABLE;

    @Autowired
    Blackhole blackhole;

    public PaginationScenarioFirstRun() {
        super("Pagination Scenario First Run");
    }

    public List<Integer> getParams() {
        return PaginationScenario.PAGE_SIZES;
    }

    DataGenerator generator = new DataGenerator(42);
    Envelope boundingBox;

    @Override
    public void prepare(DatabaseAbstraction db) {
        timer.resetAll();
        Parameters.createOSMDataTable(db, tableName);
        boundingBox = db.getDataBoundingBox(tableName, "pos");
    }

    @Override
    public void iterate(DatabaseAbstraction database, Integer pageSize) {
        Page<DataPoint> page;

        double halfWidth = boundingBox.getWidth() / 2d;
        double halfHeight = boundingBox.getHeight() / 2d;
        double cx = boundingBox.centre().getX() + generator.generateInRange(-halfWidth, halfWidth);
        double cy = boundingBox.centre().getY() + generator.generateInRange(-halfHeight, halfHeight);
        var queryShape = new GeometryFactory().toGeometry(
                new Envelope(new Envelope(cx - 0.1, cx + 0.1, cy - 0.1, cy + 0.1)));

        timer.start();
        page = database.fetchArea(tableName, queryShape, 0, pageSize);
        timer.end();

        database.deleteCursors();
        blackhole.consumeFull(page);
    }


    @Override
    public void cleanup(DatabaseAbstraction db) {
        generator = new DataGenerator(42);
        timer.resetAll();
    }


}