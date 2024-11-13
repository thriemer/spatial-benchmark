package de.thriemer.spatial.benchmark.scenarios;

import com.jakewharton.fliptables.FlipTable;
import de.thriemer.spatial.benchmark.DataPoint;
import de.thriemer.spatial.framework.DataType;
import de.thriemer.spatial.framework.DatabaseAbstraction;
import de.thriemer.spatial.framework.Scenario;
import io.vavr.Tuple2;
import lombok.SneakyThrows;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.geojson.GeoJsonReader;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

//@Component
public class LocationFilterScenario extends Scenario {

    private final String tableName = "location_filter_support";
    private final Geometry contractPolygon;

    @SneakyThrows
    public LocationFilterScenario() {
        super("Location Filter Support");
        contractPolygon = new GeoJsonReader().read("""
                {"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[0.5,9.5],[9.5,9.5],[9.5,0.5],[0.5,0.5],[0.5,9.5]],[[3.5,6.5],[6.5,6.5],[6.5,3.5],[3.5,3.5],[3.5,6.5]]]}}
                """);
    }

    @Override
    public void prepare(DatabaseAbstraction database) {
        int id = 0;
        if (!database.tableExists(tableName)) {
            database.createTable(tableName, new Tuple2<>("pos", DataType.GEO_POINT), new Tuple2<>("point_id", DataType.INT), new Tuple2<>("some_float", DataType.DOUBLE), new Tuple2<>("tags", DataType.STRING));
            List<DataPoint> points = new ArrayList<>();
            for (int longitude = -10; longitude < 10; longitude++) {
                for (int latitude = -10; latitude < 10; latitude++) {
                    points.add(new DataPoint(longitude, latitude, id++, 0, ""));
                }
            }
            database.persistMultiplePoints(tableName, points);
            database.createIndex(tableName, "pos");
        }
    }

    @Override
    public void iterate(DatabaseAbstraction database, Object param) {
        String[] header = new String[]{"Area", "Supported"};
        String[][] content = getArguments()
                .map(argument -> {
                    String supported = "yes";
                    if (database.fetchArea(tableName, argument.contract, argument.filter).size() != argument.expectedPoints) {
                        supported = "no";
                    }
                    return new String[]{argument.name(), supported};
                }).toArray(String[][]::new);
        String result = FlipTable.of(header, content);
    }


    record Argument(String name, Geometry contract, Geometry filter, int expectedPoints) {
    }

    Stream<Argument> getArguments() {
        return Stream.of(
                new Argument("Fully Inside", contractPolygon, rect(0.5, 3.5), 9),
                new Argument("Partially Outside", contractPolygon, rect(-5, 3.5), 9),
                new Argument("Fully Outside", contractPolygon, rect(-5, 0.5), 0),
                new Argument("Covering Hole", contractPolygon, rect(3.5, 6.5), 0)
        );
    }

    GeometryFactory factory = new GeometryFactory();

    private Geometry rect(double min, double max) {
        return factory.toGeometry(new Envelope(min, max, min, max));
    }

    @Override
    public void cleanup(DatabaseAbstraction database) {
        database.dropTables(tableName);
    }

}
