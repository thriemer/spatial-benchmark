package de.thriemer.spatial.benchmark.scenarios;

import de.thriemer.spatial.benchmark.DataGenerator;
import de.thriemer.spatial.benchmark.DataPoint;
import de.thriemer.spatial.framework.Blackhole;
import de.thriemer.spatial.framework.DatabaseAbstraction;
import de.thriemer.spatial.framework.Scenario;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;

@Component
@Slf4j
public class SpatialNonSpatialFilterCombination extends Scenario<String> {

    private final Blackhole blackhole;
    private static final long seed = 42;
    final String tableName = Parameters.OSM_DATA_TABLE;
    private DataGenerator generator;
    private double minLon, maxLon, minLat, maxLat;


    public SpatialNonSpatialFilterCombination(Blackhole blackhole) {
        super("Combination of spatial with non spatial filter");
        this.blackhole = blackhole;
    }

    Random rand;

    @Override
    public void prepare(DatabaseAbstraction db) {

        Parameters.createOSMDataTable(db, tableName);

        var bb = db.getDataBoundingBox(tableName, "pos");
        minLat = bb.getMinY();
        maxLat = bb.getMaxY();
        minLon = bb.getMinX();
        maxLon = bb.getMaxX();

        log.info("Testing combination of spatial and non spatial query parameters");
        generator = new DataGenerator(seed);
        rand = new Random(seed);
    }

    @Override
    public List<String> getParams() {
        return List.of("spatial only", "additional filter");
    }

    GeometryFactory factory = new GeometryFactory();

    @Override
    public void iterate(DatabaseAbstraction database, String p) {
        double maxRange = 1.5;
        double rangeX = generator.generateInRange(0.5, maxRange);
        double startX = generator.generateInRange(minLon, maxLon - maxRange);
        double rangeY = generator.generateInRange(0.5, maxRange);
        double startY = generator.generateInRange(minLat, maxLat - maxRange);
        Geometry queryShape = factory.toGeometry(new Envelope(startX, startX + rangeX, startY, startY + rangeY));

        if (p.equals("spatial only")) {
            timer.start();
            List<DataPoint> spatialOnlyPoints = database.fetchArea(tableName, queryShape);
            timer.end();
            blackhole.consumeFull(spatialOnlyPoints);
        } else {
            timer.start();
            List<DataPoint> combinedPoints = database.fetchArea(tableName, queryShape, "some_float", rand.nextFloat());
            timer.end();
            blackhole.consumeFull(combinedPoints);
        }
    }

    @Override
    public void cleanup(DatabaseAbstraction db) {
        generator = new DataGenerator(seed);
        rand = new Random(seed);
        timer.resetAll();
    }
}
