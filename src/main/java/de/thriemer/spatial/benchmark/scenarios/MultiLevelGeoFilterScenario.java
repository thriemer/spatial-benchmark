package de.thriemer.spatial.benchmark.scenarios;

import de.thriemer.spatial.benchmark.DataGenerator;
import de.thriemer.spatial.benchmark.DataPoint;
import de.thriemer.spatial.framework.Blackhole;
import de.thriemer.spatial.framework.DatabaseAbstraction;
import de.thriemer.spatial.framework.Scenario;
import org.locationtech.jts.geom.Geometry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

import static de.thriemer.spatial.framework.Helper.generateCircle;

@Component
public class MultiLevelGeoFilterScenario extends Scenario {

    private DataGenerator generator;
    private final String tableName = Parameters.OSM_DATA_TABLE;
    private double minLon, maxLon, minLat, maxLat;

    @Autowired
    Blackhole blackhole;

    public MultiLevelGeoFilterScenario() {
        super("Multiple geolocation filters");
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
    public void iterate(DatabaseAbstraction database, Object o) {
        double radius = 0.1;
        double innerRadius = 0.01;

        double lon = generator.generateInRange(minLon + radius, maxLon - radius);
        double lat = generator.generateInRange(minLat + radius, maxLat - radius);
        Geometry queryShape = generateCircle(lon, lat, radius, 500);

        double innerLon = generator.generateInRange(lon - radius + innerRadius, lon + radius - innerRadius);
        double innerLat = generator.generateInRange(lat - radius + innerRadius, lat + radius - innerRadius);
        Geometry innerShape = generateCircle(innerLon, innerLat, innerRadius, 500);

        timer.start();
        List<DataPoint> points = database.fetchArea(tableName, queryShape, innerShape);
        timer.end();

        blackhole.consumeFull(points);
    }

    @Override
    public void cleanup(DatabaseAbstraction db) {
        timer.resetAll();
    }

}
