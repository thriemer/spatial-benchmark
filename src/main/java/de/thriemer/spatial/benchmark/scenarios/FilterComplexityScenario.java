package de.thriemer.spatial.benchmark.scenarios;

import de.thriemer.spatial.benchmark.DataGenerator;
import de.thriemer.spatial.benchmark.DataPoint;
import de.thriemer.spatial.framework.Blackhole;
import de.thriemer.spatial.framework.DatabaseAbstraction;
import de.thriemer.spatial.framework.Scenario;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

import static de.thriemer.spatial.framework.Helper.generateCircle;

@Component
@Slf4j
public class FilterComplexityScenario extends Scenario<Integer> {
    private DataGenerator generator;
    private final String tableName = Parameters.OSM_DATA_TABLE;
    private double minLon, maxLon, minLat, maxLat;

    @Autowired
    Blackhole blackhole;

    public FilterComplexityScenario() {
        super("Polygon filter complexity");
    }

    @Override
    public List<Integer> getParams() {
        return List.of(100, 250, 500, 1000, 2500, 5_000, 10_000, 25_000, 50_000, 100_000);
    }

    @Override
    public void prepare(DatabaseAbstraction db) {
        timer.resetAll();
        Parameters.createOSMDataTable(db, tableName);
        generator = new DataGenerator(42);

        var bb = db.getDataBoundingBox(tableName, "pos");
        minLat = bb.getMinY();
        maxLat = bb.getMaxY();
        minLon = bb.getMinX();
        maxLon = bb.getMaxX();
        System.out.println();
    }

    @Override
    public void iterate(DatabaseAbstraction database, Integer vertexCount) {
        double radius = 0.2;

        var p = new Coordinate(generator.generateInRange(minLon + radius, maxLon - radius), generator.generateInRange(minLat + radius, maxLat - radius));

        Geometry queryShape = generateCircle(p.getX(), p.getY(), radius, vertexCount);
        timer.start();
        List<DataPoint> points = database.fetchArea(tableName, queryShape);
        timer.end();
        blackhole.consumeFull(points);
    }

    @Override
    public void cleanup(DatabaseAbstraction db) {
        timer.resetAll();
    }


}
