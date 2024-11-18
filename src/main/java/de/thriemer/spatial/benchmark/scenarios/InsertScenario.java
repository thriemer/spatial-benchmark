package de.thriemer.spatial.benchmark.scenarios;

import de.thriemer.spatial.benchmark.DataGenerator;
import de.thriemer.spatial.benchmark.DataPoint;
import de.thriemer.spatial.framework.DataType;
import de.thriemer.spatial.framework.DatabaseAbstraction;
import de.thriemer.spatial.framework.Scenario;
import io.vavr.Tuple2;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.IntStream;

@Component
public class InsertScenario extends Scenario<Integer> {
    final String tableName = "point_data_insert";

    private DataGenerator generator;
    // GER bounding box
    private final double minLon = 5;
    private final double maxLon = 15;
    private final double minLat = 45;
    private final double maxLat = 55;

    public InsertScenario() {
        super("Batch Insert Point");
    }

    @Override
    public List<Integer> getParams() {
        return List.of(1, 10, 100, 500, 1000, 5_000, 10_000, 25_000, 50_000, 75_000, 100_000);
    }

    @Override
    public void prepare(DatabaseAbstraction db) {
        timer.resetAll();
        if (db.tableExists(tableName)) {
            db.dropTables(tableName);
        }
        db.createTable(tableName, new Tuple2<>("pos", DataType.GEO_POINT), new Tuple2<>("point_id", DataType.INT), new Tuple2<>("some_float", DataType.DOUBLE), new Tuple2<>("tags", DataType.STRING));
        generator = new DataGenerator();
    }


    @Override
    public void iterate(DatabaseAbstraction database, Integer p) {
        List<DataPoint> points = IntStream.range(0, p).mapToObj(i -> generator.generateDataPoint(minLon, maxLon, minLat, maxLat)).toList();
        timer.start();
        database.persistMultiplePoints(tableName, points);
        timer.end();
    }

    @Override
    public void cleanup(DatabaseAbstraction db) {
        timer.resetAll();
        generator = new DataGenerator();
        db.dropTables(tableName);
        db.createTable(tableName, new Tuple2<>("pos", DataType.GEO_POINT), new Tuple2<>("point_id", DataType.INT), new Tuple2<>("some_float", DataType.DOUBLE), new Tuple2<>("tags", DataType.STRING));
    }

}
