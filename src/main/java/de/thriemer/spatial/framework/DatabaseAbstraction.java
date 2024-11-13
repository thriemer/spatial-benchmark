package de.thriemer.spatial.framework;

import de.thriemer.spatial.benchmark.DataPoint;
import de.thriemer.spatial.benchmark.Page;
import io.vavr.Tuple2;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.testcontainers.containers.GenericContainer;

import java.util.ArrayList;
import java.util.List;

public abstract class DatabaseAbstraction {

    protected List<String> createdTables = new ArrayList<>();
    protected GenericContainer<?> container;

    public abstract String getName();

    public abstract String getProcess();

    public abstract void setup();

    public abstract boolean tableExists(String tableName);

    public abstract void createTable(String tableName, Tuple2<String, DataType>... columns);

    public abstract Envelope getDataBoundingBox(String table, String column);

    public abstract void persistMultiplePoints(String tableName, List<DataPoint> dataPoints);

    public abstract void createIndex(String tableName, String... params);

    public abstract Page<DataPoint> fetchArea(String tableName, Geometry shape, int page, int pageSize);

    public abstract List<DataPoint> fetchArea(String tableName, Geometry shape);

    public abstract List<DataPoint> fetchArea(String tableName, Geometry contractPolygon, Geometry filter);

    public abstract List<DataPoint> fetchArea(String tableName, Geometry shape, String value, float greaterThan);

    public abstract void deleteCursors();

    public abstract void dropTables(String... tables);

    public abstract int count(String tableName);

    public void cleanUp() {
        createdTables.forEach(this::dropTables);
        container.stop();
    }

    public abstract String getVolume();
}
