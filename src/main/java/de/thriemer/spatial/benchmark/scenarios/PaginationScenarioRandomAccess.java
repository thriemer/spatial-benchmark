package de.thriemer.spatial.benchmark.scenarios;

import de.thriemer.spatial.benchmark.DataGenerator;
import de.thriemer.spatial.framework.Blackhole;
import de.thriemer.spatial.framework.DatabaseAbstraction;
import de.thriemer.spatial.framework.Scenario;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.IntStream;

@Component
@Slf4j
public class PaginationScenarioRandomAccess extends Scenario {
    private final String tableName = Parameters.OSM_DATA_TABLE;

    @Autowired
    Blackhole blackhole;

    public PaginationScenarioRandomAccess() {
        super("Pagination Scenario Random Access");
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
    public void iterate(DatabaseAbstraction database, Object o) {
        int pageSize = 1000;

        double halfWidth = boundingBox.getWidth() / 2d;
        double halfHeight = boundingBox.getHeight() / 2d;
        double cx = boundingBox.centre().getX() + generator.generateInRange(-halfWidth, halfWidth);
        double cy = boundingBox.centre().getY() + generator.generateInRange(-halfHeight, halfHeight);
        var queryShape = new GeometryFactory().toGeometry(
                new Envelope(cx - 0.1, cx + 0.1, cy - 0.1, cy + 0.1));

        var page = database.fetchArea(tableName, queryShape, 0, pageSize);

        var pageList = new ArrayList<>(IntStream.range(1, page.maxPageNumber + 1).boxed().toList());
        Collections.shuffle(pageList);

        for (int i = 0; i < Math.min(pageList.size(), 10); i++) {
            int pageNumber = pageList.get(i);
            timer.start();
            page = database.fetchArea(tableName, queryShape, pageNumber, pageSize);
            timer.end();
        }

        blackhole.consumeFull(page);
        database.deleteCursors();
    }


    @Override
    public void cleanup(DatabaseAbstraction db) {
        generator = new DataGenerator(42);
        timer.resetAll();
    }


}