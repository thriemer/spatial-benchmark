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

import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class PaginationScenario extends Scenario<Integer> {
    private final String tableName = Parameters.OSM_DATA_TABLE;

    @Autowired
    Blackhole blackhole;

    public static final List<Integer> PAGE_SIZES = List.of(1000, 2500, 5000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000);

    public PaginationScenario() {
        super("Pagination Scenario Fetch Pages");
    }

    public List<Integer> getParams() {
        return PAGE_SIZES;
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
        double dataArea = boundingBox.getArea();
        double queryArea = 2 * dataArea *  pageSize / (double) Parameters.getNumberOfPoints();
        double halfSideLength = Math.sqrt(queryArea) / 2d;

        double halfWidth = boundingBox.getWidth() / 2d;
        double halfHeight = boundingBox.getHeight() / 2d;
        double cx = boundingBox.centre().getX() + generator.generateInRange(-halfWidth, halfWidth);
        double cy = boundingBox.centre().getY() + generator.generateInRange(-halfHeight, halfHeight);
        var queryShape = new GeometryFactory().toGeometry(
                new Envelope(new Envelope(cx - halfSideLength, cx + halfSideLength, cy - halfSideLength, cy + halfSideLength)));

        // retrieve the first page without timing it
        int pageNumber = 0;
        page = database.fetchArea(tableName, queryShape, pageNumber,  pageSize);
        blackhole.consumeFull(page);
        pageNumber++;
        List<DataPoint> queryResult = new ArrayList<>();
        while (pageNumber <= page.maxPageNumber) {
            timer.start();
            page = database.fetchArea(tableName, queryShape, pageNumber,  pageSize);
            timer.end();
            queryResult.addAll(page.data);
            pageNumber++;
        }
        blackhole.consumeFull(queryResult);
        database.deleteCursors();
    }


    @Override
    public void cleanup(DatabaseAbstraction db) {
        generator = new DataGenerator(42);
        timer.resetAll();
    }


}
