package de.thriemer.spatial;

import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.*;
import com.github.dockerjava.api.model.Bind;
import de.thriemer.spatial.benchmark.DataPoint;
import de.thriemer.spatial.benchmark.Page;
import de.thriemer.spatial.framework.DataType;
import de.thriemer.spatial.framework.DatabaseAbstraction;
import de.thriemer.spatial.framework.DatabaseStatisticCollector;
import io.vavr.Tuple2;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import org.locationtech.jts.io.geojson.GeoJsonWriter;
import org.springframework.stereotype.Service;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class AerospikeDatabase extends DatabaseAbstraction {

    //    Namespace → Relational Database
    //    Primary Index → Primary Index
    //    Set → Table
    //    Record → Database Row
    //    Bin → Field

    GeoJsonReader geoJsonReader = new GeoJsonReader();
    GeoJsonWriter geoJsonWriter = new GeoJsonWriter();

    IAerospikeClient client;
    ClientPolicy clientPolicy;
    String namespace = "default";

    @Override
    public String getName() {
        return "Aerospike";
    }

    @Override
    public String getProcess() {
        return "asd";
    }

    @Override
    public void setup() {

        container = new GenericContainer<>(DockerImageName.parse("aerospike:ce-7.0.0.4"));
        container.withEnv("NAMESPACE", "default")
                .withEnv("STORAGE_GB", "128")
                .withEnv("MEM_GB", "128")
                .addExposedPort(3000);
        if (DatabaseStatisticCollector.useVolume) {
            container.setBinds(List.of(Bind.parse("aerospike-volume:/opt/aerospike/data")));
        }
        container.waitingFor(Wait.forLogMessage(".*heartbeat-received.*", 1));
        container.start();

        client = new AerospikeClient("127.0.0.1", container.getFirstMappedPort());
        clientPolicy = new ClientPolicy();
        clientPolicy.writePolicyDefault.expiration = -1;
        clientPolicy.writePolicyDefault.commitLevel = CommitLevel.COMMIT_ALL;
    }

    @Override
    public boolean tableExists(String tableName) {
        String answer = Info.request(clientPolicy.infoPolicyDefault, client.getNodes()[0], "sets");

        for (String tableInfo : answer.split(";")) {
            if (tableInfo.contains(tableName)) {
                return !tableInfo.contains("objects=0");
            }
        }
        return false;
    }

    @Override
    public void createTable(String tableName, Tuple2<String, DataType>... columns) {
        this.createdTables.add(tableName);
        // i guess there are no tables?
    }


    @Override
    public Envelope getDataBoundingBox(String table, String column) {
        // Aerospike has no GIS capabilities so the accumulation has to be written in java
        log.warn("Aerospike has no aggregate function so to get the bounding box a table scan is necessary");
        log.warn("Therefore hardcoded values are used. This will only work for OSM turkey");

        return DatabaseStatisticCollector.fastLane ? new Envelope(9.3977818, 9.6803433, 46.8232622, 47.5258072) : new Envelope(18.3577395, 45.1481355, 31.21, 46.345246800000005);

        /*
        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(table);
        stmt.setMaxRecords(1000);
        PartitionFilter pFilter = PartitionFilter.all();
        QueryPolicy queryPolicy = new QueryPolicy(clientPolicy.queryPolicyDefault);
        AtomicReference<Double> minX = new AtomicReference<>(Double.MAX_VALUE);
        AtomicReference<Double> maxX = new AtomicReference<>(-Double.MAX_VALUE);
        AtomicReference<Double> minY = new AtomicReference<>(Double.MAX_VALUE);
        AtomicReference<Double> maxY = new AtomicReference<>(-Double.MAX_VALUE);
        while (!pFilter.isDone()) {
            RecordSet rs = client.queryPartitions(queryPolicy, stmt, pFilter);
            rs.forEach(r -> {
                try {
                    Point point = (Point) reader.read(new StringReader(r.record.getGeoJSONString("pos")));
                    minX.set(Math.min(minX.get(), point.getX()));
                    maxX.set(Math.max(maxX.get(), point.getX()));
                    minY.set(Math.min(minY.get(), point.getY()));
                    maxY.set(Math.max(maxY.get(), point.getY()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return new RectangleImpl(minX.get(), maxX.get(), minY.get(), maxY.get(), SpatialContext.GEO);
         */
    }

    @Override
    public void persistMultiplePoints(String tableName, List<DataPoint> dataPoints) {
        List<BatchRecord> records = dataPoints.stream().map(dp -> convertToBatchWrite(tableName, dp)).toList();
        client.operate(clientPolicy.batchPolicyDefault, records);
    }

    BatchRecord convertToBatchWrite(String tableName, DataPoint dataPoint) {
        Key k = new Key(namespace, tableName, dataPoint.id());
        var ops = Arrays.stream(mapToBins(dataPoint)).map(Operation::put).toArray(Operation[]::new);
        return new BatchWrite(k, ops);
    }

    @Override
    public void createIndex(String tableName, String... params) {
        String indexName = tableName + "_" + Arrays.stream(params).reduce("", (a, b) -> a + b) + "_idx";
        log.info("creating index: {}", indexName);
        Policy p = new Policy();
        p.setTimeout(0);
        var task = client.createIndex(p, namespace, tableName, indexName, params[0], IndexType.GEO2DSPHERE);
        task.waitTillComplete();
    }

    record Cursor(Statement stmt, PartitionFilter pf, int maxElements) {
    }

    Map<String, Cursor> cursors = new HashMap<>();
    Map<Integer, Page<DataPoint>> pageCache = new HashMap<>();

    @Override
    public Page<DataPoint> fetchArea(String tableName, Geometry shape, int page, int pageSize) {
        String cursorName = tableName + pageSize;
        if (pageCache.containsKey(page)) {
            return pageCache.get(page);
        }
        Cursor cursor;
        if (!cursors.containsKey(cursorName)) {
            Filter f = Filter.geoWithinRegion("pos",geoJsonWriter.write(shape));
            Statement countStatement = new Statement();
            countStatement.setNamespace(namespace);
            countStatement.setSetName(tableName);
            countStatement.setFilter(f);
            var aggregateResult = client.query(clientPolicy.queryPolicyDefault, countStatement);
            AtomicInteger elementCount = new AtomicInteger();
            aggregateResult.forEach(o -> elementCount.getAndIncrement());

            Statement stmt = new Statement();
            stmt.setNamespace(namespace);
            stmt.setSetName(tableName);
            stmt.setMaxRecords(pageSize);
            stmt.setFilter(f);
            PartitionFilter pFilter = PartitionFilter.all();

            cursor = new Cursor(stmt, pFilter, elementCount.get());
            cursors.put(cursorName, cursor);
        } else {
            cursor = cursors.get(cursorName);
        }

        int maxCachedPage = pageCache.keySet().stream().mapToInt(Integer::intValue).max().orElse(-1);
        int maxPage = cursor.maxElements() / pageSize;
        if (page > maxPage) {
            return new Page<>(page, maxPage, new ArrayList<>());
        }
        while (maxCachedPage <= page) {
            List<DataPoint> results = new ArrayList<>(pageSize);
            if (!cursor.pf().isDone()) {   // until no more results to process
                QueryPolicy queryPolicy = new QueryPolicy(clientPolicy.queryPolicyDefault);
                RecordSet rs = client.queryPartitions(queryPolicy, cursor.stmt(), cursor.pf());
                rs.forEach(r -> results.add(mapToPoint(r.record)));
            } else {
                cursors.remove(cursorName);
            }
            pageCache.put(++maxCachedPage, new Page<>(page, cursor.maxElements() / pageSize, results));
        }
        return pageCache.get(page);
    }

    @SneakyThrows
    DataPoint mapToPoint(Record r) {
        Point point = (Point) geoJsonReader.read(r.getGeoJSONString("pos"));
        return new DataPoint(point.getX(), point.getY(), r.getInt("id"), r.getFloat("some_float"), r.getString("tags"));
    }

    @Override
    public List<DataPoint> fetchArea(String tableName, Geometry shape) {
        return fetch(tableName, shape, Exp.val(true));
    }

    @Override
    public List<DataPoint> fetchArea(String tableName, Geometry contractPolygon, Geometry filter) {
        return fetch(tableName, contractPolygon, Exp.geoCompare(Exp.geoBin("pos"), Exp.geo(geoJsonWriter.write(filter))));
    }

    @Override
    public List<DataPoint> fetchArea(String tableName, Geometry shape, String parameter, float greaterThan) {
        var exp = Exp.gt(Exp.floatBin(parameter), Exp.val(greaterThan));
        return fetch(tableName, shape, exp);
    }

    @Override
    public void deleteCursors() {
        cursors.clear();
        pageCache.clear();
    }

    List<DataPoint> fetch(String tableName, Geometry shape, Exp exp) {
        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(tableName);
        stmt.setFilter(Filter.geoWithinRegion("pos", geoJsonWriter.write(shape)));
        QueryPolicy queryPolicy = new QueryPolicy(clientPolicy.queryPolicyDefault);
        queryPolicy.filterExp = Exp.build(exp);
        RecordSet rs = client.query(queryPolicy, stmt);
        List<DataPoint> results = new ArrayList<>();
        rs.forEach(r -> results.add(mapToPoint(r.record)));
        return results;
    }

    @Override
    public void dropTables(String... tables) {
        for (String set : tables) {
            client.truncate(null, namespace, set, null);
        }
    }

    @Override
    public int count(String tableName) {
        AtomicInteger count = new AtomicInteger(0);
        client.scanAll(clientPolicy.scanPolicyDefault, namespace, tableName, (k, r) -> count.incrementAndGet());
        return count.get();
    }

    @Override
    public String getVolume() {
        return "aerospike-volume";
    }

    GeometryFactory factory = new GeometryFactory();

    private Bin[] mapToBins(DataPoint dataPoint) {
        Geometry point = factory.createPoint(new Coordinate(dataPoint.longitude(), dataPoint.latitude()));
        return new Bin[]{new Bin("pos", Value.getAsGeoJSON(geoJsonWriter.write(point))), new Bin("id", dataPoint.id()), new Bin("some_float", dataPoint.someFloat()), new Bin("tags", dataPoint.tags())};
    }

}
