package de.thriemer.spatial;

import com.github.dockerjava.api.model.Bind;
import de.thriemer.spatial.benchmark.DataPoint;
import de.thriemer.spatial.benchmark.Page;
import de.thriemer.spatial.framework.DataType;
import de.thriemer.spatial.framework.DatabaseAbstraction;
import de.thriemer.spatial.framework.DatabaseStatisticCollector;
import io.vavr.Tuple2;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.postgis.jdbc.PGgeometry;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;
import org.postgresql.util.PGobject;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.stereotype.Service;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.sql.*;
import java.util.*;

@Service
@Slf4j
@RegisterReflectionForBinding(classes = {net.postgis.jdbc.DriverWrapper.class})
public class PostGisDatabase extends DatabaseAbstraction {

    WKTWriter wktWriter = new WKTWriter();

    Connection connection;
    Statement statement;

    @Override
    public String getName() {
        return "PostGIS";
    }

    @Override
    public String getProcess() {
        return "postgres";
    }

    @SneakyThrows
    @Override
    public void setup() {
        container = new GenericContainer<>(DockerImageName.parse("postgis/postgis:16-3.4"));
        String username = "test_user";
        String password = "p@ssword";
        container.withEnv(Map.of(
                        "POSTGRES_PASSWORD", password,
                        "POSTGRES_USER", username,
                        "POSTGRES_DB", "db"
                ))
                .addExposedPort(5432);
        if (DatabaseStatisticCollector.useVolume) {
            container.setBinds(List.of(Bind.parse("postgis-volume:/var/lib/postgresql/data")));
        }
        container.withLogConsumer(new Slf4jLogConsumer(log));
        container.waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 1));
        container.start();
        // this is a hack because if postgis isn't initialized the DB restarts and the log message appears twice
        // if it is initialized it appears only once. I could do something fancy but waiting also works
        Thread.sleep(5_000);
        try {
            Properties props = new Properties();
            props.setProperty("user", username);
            props.setProperty("password", password);
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:" + container.getFirstMappedPort() + "/db", props);
            statement = connection.createStatement();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        log.info("Connection established");
    }

    @Override
    @SneakyThrows
    public boolean tableExists(String tableName) {
        String query = """
                        SELECT EXISTS (
                        SELECT * FROM information_schema.tables
                        WHERE table_name = '{{table_name}}'
                );""".replace("{{table_name}}", tableName);
        var result = statement.executeQuery(query);
        result.next();
        return result.getBoolean(1);
    }

    @SneakyThrows
    @Override
    public void createTable(String tableName, Tuple2<String, DataType>... columns) {
        this.createdTables.add(tableName);
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("CREATE TABLE ").append(tableName).append(" ( ");
        for (int i = 0; i < columns.length; i++) {
            queryBuilder.append(columns[i]._1).append(" ").append(mapDataType(columns[i]._2));
            if (i != columns.length - 1) {
                queryBuilder.append(",");
            }
        }

        queryBuilder.append(" )");
        log.info("Create table. Query= {}", queryBuilder);
        statement.execute(queryBuilder.toString());
    }

    private String mapDataType(DataType dataType) {
        return switch (dataType) {
            case DOUBLE -> "float8";
            case INT -> "int";
            case GEO_POINT -> "geometry(POINT)";
            case GEOMETRY -> "geometry";
            case STRING -> "varchar";
        };
    }

    @Override
    @SneakyThrows
    public Envelope getDataBoundingBox(String table, String column) {
        String query = "SELECT min(ST_Ymin({{columns}}::geometry)) as minY,min(ST_Xmin({{columns}}::geometry)) as minX, max(ST_Ymax({{columns}}::geometry)) as maxY,max(ST_Xmax({{columns}}::geometry)) as maxX FROM {{table}}"
                .replace("{{columns}}", column)
                .replace("{{table}}", table);

        var resultSet = statement.executeQuery(query);
        resultSet.next();
        return new Envelope(resultSet.getDouble("minX"), resultSet.getDouble("maxX"),
                resultSet.getDouble("minY"), resultSet.getDouble("maxY"));
    }

    @SneakyThrows
    @Override
    public void persistMultiplePoints(String tableName, List<DataPoint> dataPoints) {
        String query = "INSERT INTO {{tableName}} VALUES(ST_MakePoint(?,?),?,?,?);".replace("{{tableName}}", tableName);
        PreparedStatement preparedStatement = connection.prepareStatement(query);

        for (var dp : dataPoints) {
            insertIntoStatement(preparedStatement, dp);
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.close();
    }

    private void insertIntoStatement(PreparedStatement preparedStatement, DataPoint dp) throws SQLException {
        preparedStatement.setDouble(1, dp.longitude());
        preparedStatement.setDouble(2, dp.latitude());
        preparedStatement.setInt(3, dp.id());
        preparedStatement.setFloat(4, dp.someFloat());
        preparedStatement.setString(5, dp.tags());
    }

    @SneakyThrows
    @Override
    public void createIndex(String tableName, String... params) {

        String indexName = tableName + "_" + Arrays.stream(params).reduce("", (a, b) -> a + b) + "_idx";
        if (!statement.executeQuery("SELECT indexname FROM pg_indexes WHERE tablename='{{tableName}}' AND indexname='{{indexName}}'"
                .replace("{{tableName}}", tableName)
                .replace("{{indexName}}", indexName)
        ).next()) {

            StringBuilder query = new StringBuilder().append("CREATE INDEX ").append(indexName).append(" ON ").append(tableName).append(" USING GIST(");

            for (int i = 0; i < params.length; i++) {
                query.append(params[i]);
                if (i != params.length - 1) {
                    query.append(",");
                }
            }
            query.append(");");
            statement.execute(query.toString());
            log.info("Created index: {} ", indexName);
            log.info("Clustering on index. This will take a long time");
            statement.execute("CLUSTER " + tableName + " using " + indexName);
            log.info("Clustering finished. Vacuum analyze");
            statement.execute("VACUUM ANALYZE " + tableName);
            log.info("Vacuum analyze Done");
        }
    }

    HashMap<String, Integer> cursorNames = new HashMap<>();

    @Override
    @SneakyThrows
    public Page<DataPoint> fetchArea(String tableName, Geometry shape, int page, int pageSize) {
        String cursorName = "cs" + pageSize;
        if (!cursorNames.containsKey(cursorName)) {
            String wkt = convertToWKT(shape);
            String query = "SELECT * FROM {{tableName}} WHERE ST_INTERSECTS(ST_GeomFromText('{{wkt}}'), pos);".replace("{{tableName}}", tableName).replace("{{wkt}}", wkt);
            String cursorStatement = "DECLARE " + cursorName + " SCROLL CURSOR WITH HOLD FOR " + query;
            log.debug("Creating cursor with: {}", cursorStatement);
            statement.execute(cursorStatement);

            var resultSet = statement.executeQuery("SELECT Count(*) FROM {{tableName}} WHERE ST_INTERSECTS(ST_GeomFromText('{{wkt}}'), pos);".replace("{{tableName}}", tableName).replace("{{wkt}}", wkt));
            resultSet.next();
            int elementCount = resultSet.getInt(1);
            int pages = elementCount / pageSize;
            cursorNames.put(cursorName, pages);
        }
        statement.execute("MOVE ABSOLUTE " + page * pageSize + " IN " + cursorName);
        var set = statement.executeQuery("FETCH " + pageSize + " FROM " + cursorName);
        var points = convert(set);
        int maxPageCount = cursorNames.get(cursorName);
        cursorNames.put(cursorName, page);
        log.debug("Result sie: {}", points.size());
        return new Page<>(page, maxPageCount, points);
    }

    @SneakyThrows
    @Override
    public List<DataPoint> fetchArea(String tableName, Geometry shape) {
        String wkt = convertToWKT(shape);
        String query = "SELECT * FROM {{tableName}} WHERE ST_INTERSECTS(ST_GeomFromText('{{wkt}}'), pos);".replace("{{tableName}}", tableName).replace("{{wkt}}", wkt);
        var set = statement.executeQuery(query);
        return convert(set);
    }

    @SneakyThrows
    @Override
    public List<DataPoint> fetchArea(String tableName, Geometry contractPolygon, Geometry filter) {
        String wkt1 = convertToWKT(contractPolygon);
        String wkt2 = convertToWKT(filter);
        String query = "SELECT * FROM {{tableName}} WHERE ST_INTERSECTS(ST_Intersection( ST_GeomFromText('{{wkt1}}'),ST_GeomFromText('{{wkt2}}')), pos);"
                .replace("{{tableName}}", tableName)
                .replace("{{wkt1}}", wkt1)
                .replace("{{wkt2}}", wkt2);
        var set = statement.executeQuery(query);
        return convert(set);
    }

    @SneakyThrows
    @Override
    public List<DataPoint> fetchArea(String tableName, Geometry shape, String parameter, float greaterThan) {
        String wkt = convertToWKT(shape);
        String query = "SELECT * FROM {{tableName}} WHERE ST_INTERSECTS(ST_GeomFromText('{{wkt}}'), pos) AND {{parameter}}>{{gt}};"
                .replace("{{tableName}}", tableName)
                .replace("{{wkt}}", wkt)
                .replace("{{parameter}}", parameter)
                .replace("{{gt}}", greaterThan + "");
        var set = statement.executeQuery(query);
        return convert(set);
    }

    @Override
    @SneakyThrows
    public void deleteCursors() {
        for (String cursorName : cursorNames.keySet()) {
            statement.execute("CLOSE " + cursorName);
        }
        cursorNames.clear();
    }

    @SneakyThrows
    private List<DataPoint> convert(ResultSet set) {
        List<DataPoint> results = new ArrayList<>();
        while (set.next()) {
            var obj = set.getObject("pos", PGobject.class);
            PGgeometry geom = new PGgeometry(obj.getValue());
            var point = geom.getGeometry().getFirstPoint();
            results.add(new DataPoint(
                    point.getX(),
                    point.getY(),
                    set.getInt("point_id"),
                    set.getFloat("some_float"),
                    set.getString("tags")
            ));
        }
        return results;
    }

    private String convertToWKT(Geometry shape) {
        return wktWriter.write(shape);
    }

    @SneakyThrows
    @Override
    public void dropTables(String... tables) {
        for (String t : tables) {
            statement.execute("DROP TABLE IF EXISTS " + t);
        }
    }

    @SneakyThrows
    @Override
    public int count(String tableName) {
        var result = statement.executeQuery("SELECT count(*) from " + tableName);
        result.next();
        return result.getInt(1);
    }

    @Override
    public String getVolume() {
        return "postgis-volume";
    }
}
