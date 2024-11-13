package de.thriemer.spatial;

import com.github.dockerjava.api.model.Bind;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.auth.endpoints.WellKnownKustoEndpointsData;
import de.thriemer.spatial.benchmark.DataPoint;
import de.thriemer.spatial.benchmark.Page;
import de.thriemer.spatial.framework.DataType;
import de.thriemer.spatial.framework.DatabaseAbstraction;
import de.thriemer.spatial.framework.DatabaseStatisticCollector;
import io.vavr.Tuple2;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.geojson.GeoJsonWriter;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.stereotype.Service;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Service
@Slf4j
@RegisterReflectionForBinding(classes = {WellKnownKustoEndpointsData.class, WellKnownKustoEndpointsData.AllowedEndpoints.class})
public class ADXDatabase extends DatabaseAbstraction {

    private Client adxClient;

    GeoJsonWriter geoJsonWriter = new GeoJsonWriter();

    @Override
    public String getName() {
        return "Azure Data Explorer";
    }

    @Override
    public String getProcess() {
        return "Kusto.Personal";
    }


    @Override
    @SneakyThrows
    public void setup() {
        container = new GenericContainer<>(DockerImageName.parse("mcr.microsoft.com/azuredataexplorer/kustainer-linux:latest"));
        container.withEnv("ACCEPT_EULA", "Y")
                .addExposedPort(8080);
        if (DatabaseStatisticCollector.useVolume) {
            container.setBinds(List.of(Bind.parse("adx-volume:/kusto/tmp")));
        }
        container.waitingFor(Wait.forLogMessage(".*Kusto.Personal start-up time.*", 1));
        container.start();

        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithUserPrompt("http://localhost:" + container.getFirstMappedPort());
        adxClient = ClientFactory.createClient(csb);
        adxClient.executeMgmt(".show database");
    }

    @Override
    @SneakyThrows
    public boolean tableExists(String tableName) {
        var result = adxClient.executeMgmt(".show tables (" + tableName + ")").getPrimaryResults();
        return result.count() != 0;
    }

    @SneakyThrows
    @Override
    public void createTable(String tableName, Tuple2<String, DataType>... columns) {
        this.createdTables.add(tableName);
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append(".create table ").append(tableName).append(" ( ");
        for (int i = 0; i < columns.length; i++) {
            var type = columns[i]._2;
            if (type == DataType.GEO_POINT) {
                queryBuilder.append("longitude:real,");
                queryBuilder.append("latitude:real,");
                queryBuilder.append("s2level:string");
            } else {
                queryBuilder.append(columns[i]._1).append(":").append(mapDataType(type));
            }
            if (i != columns.length - 1) {
                queryBuilder.append(",");
            }
        }

        queryBuilder.append(" )");
        log.info("Create table. Query= {}", queryBuilder);
        adxClient.executeMgmt(queryBuilder.toString());
    }

    private String mapDataType(DataType dataType) {
        return switch (dataType) {
            case DOUBLE -> "real";
            case INT -> "int";
            case GEO_POINT -> "NOT_SUPPORTED_AND_NEEDS_WORK_AROUND";
            case GEOMETRY -> "dynamic";
            case STRING -> "string";
        };
    }

    @SneakyThrows
    @Override
    public void persistMultiplePoints(String tableName, List<DataPoint> dataPoints) {
        StringBuilder builder = new StringBuilder();
        builder.append(".ingest inline into table ").append(tableName).append(" <|\n");
        for (DataPoint dataPoint : dataPoints) {
            appendDataPoint(builder, dataPoint);
            builder.append("\n");
        }
        log.debug("Ingesting with command= {}", builder);
        adxClient.execute(builder.toString());
    }

    private void appendDataPoint(StringBuilder builder, DataPoint dataPoint) {
        String s2CellId = S2CellId.fromLatLng(S2LatLng.fromDegrees(dataPoint.latitude(), dataPoint.longitude())).parent(11).toToken();
        builder.append(dataPoint.longitude()).append(",").append(dataPoint.latitude()).append(",")
                .append(s2CellId).append(",").append(dataPoint.id()).append(",").append(dataPoint.someFloat()).append(",").append(dataPoint.tags());
    }

    @Override
    @SneakyThrows
    public Envelope getDataBoundingBox(String table, String column) {
        String query = """
                {{tableName}}
                | summarize minX=min(longitude), maxX=max(longitude), minY=min(latitude), maxY=max(latitude);
                """.replace("{{tableName}}", table);
        var queryResult = adxClient.executeQuery(query);
        var result = queryResult.getPrimaryResults();
        result.next();
        return new Envelope(result.getDouble("minX"), result.getDouble("maxX"),
                result.getDouble("minY"), result.getDouble("maxY"));
    }

    @Override
    public void createIndex(String tableName, String... params) {

    }

    HashMap<String, Integer> sqrNames = new HashMap<>();

    @SneakyThrows
    @Override
    public Page<DataPoint> fetchArea(String tableName, Geometry shape, int page, int pageSize) {

        String sqrName = "sqr_" + pageSize;
        if (!sqrNames.containsKey(sqrName)) {
            String geoJson = geoJsonWriter.write(shape);
            String query =
                    """
                            .set-or-replace stored_query_result {{sqrName}} <|
                            let coveringS2Cells = datatable(poly:dynamic)[
                                dynamic({{geojson}})
                            ];
                            coveringS2Cells
                            | extend s2level = geo_polygon_to_s2cells(poly)
                            | mv-expand s2level to typeof(string)
                            | extend smaller_poly = geo_intersection_2polygons(geo_s2cell_to_polygon(s2level), poly)
                            | join kind=inner {{tableName}} on s2level
                            | where geo_point_in_polygon(longitude,latitude, smaller_poly)
                            | project-away s2level, poly, smaller_poly
                            | serialize Num=row_number()
                            """
                            .replace("{{sqrName}}", sqrName)
                            .replace("{{tableName}}", tableName)
                            .replace("{{geojson}}", geoJson);
            sqrNames.put(sqrName, 0);
            log.debug("Creating stored query result with name: {}", sqrName);
            adxClient.executeMgmt(query);
        }

        String query = """
                let in_area = stored_query_result("{{tableName}}");
                in_area | count | as data_count;
                in_area | where Num between ({{range}}) | project-away Num | as data
                """.replace("{{tableName}}", sqrName).replace("{{range}}", page * pageSize + ".." + ((page + 1) * pageSize - 1));

        var result = adxClient.executeQuery(query);
        var mainResult = result.getResultTables().stream().filter(t -> t.getTableName().equals("data")).findFirst().get();
        int count = result.getResultTables().stream().filter(t -> t.getTableName().equals("data_count") && t.next()).mapToInt(t -> t.getInt("Count")).findFirst().getAsInt();

        List<DataPoint> resultList = mapQueryResult(mainResult);

        int maxPageNumber = count / pageSize;
        return new Page<>(page, maxPageNumber, resultList);
    }

    @SneakyThrows
    @Override
    public List<DataPoint> fetchArea(String tableName, Geometry shape) {
        String geoJson = geoJsonWriter.write(shape);
        String query = """
                set notruncation;
                let coveringS2Cells = datatable(poly:dynamic)[
                    dynamic({{geojson}})
                ];
                coveringS2Cells
                | extend s2level = geo_polygon_to_s2cells(poly)
                | mv-expand s2level to typeof(string)
                | extend smaller_poly = geo_intersection_2polygons(geo_s2cell_to_polygon(s2level), poly)
                | join kind=inner {{tableName}} on s2level
                | where geo_point_in_polygon(longitude,latitude, smaller_poly)
                | project-away s2level, poly, smaller_poly
                """.replace("{{tableName}}", tableName).replace("{{geojson}}", geoJson);
        log.debug("Executing Query: {}", query);
        var queryResult = adxClient.executeQuery(query);
        var result = queryResult.getPrimaryResults();
        return mapQueryResult(result);
    }

    @Override
    @SneakyThrows
    public List<DataPoint> fetchArea(String tableName, Geometry contractPolygon, Geometry filter) {
        String geoJson1 = geoJsonWriter.write(contractPolygon);
        String geoJson2 = geoJsonWriter.write(filter);
        String query = """
                set notruncation;
                let coveringS2Cells = (print poly=geo_intersection_2polygons(dynamic({{geojson1}}), dynamic({{geojson2}})));
                coveringS2Cells
                | extend s2level = geo_polygon_to_s2cells(poly)
                | mv-expand s2level to typeof(string)
                | extend smaller_poly = geo_intersection_2polygons(geo_s2cell_to_polygon(s2level), poly)
                | join kind=inner {{tableName}} on s2level
                | where geo_point_in_polygon(longitude,latitude, smaller_poly)
                | project-away s2level, poly, smaller_poly
                """.replace("{{tableName}}", tableName).replace("{{geojson1}}", geoJson1).replace("{{geojson2}}", geoJson2);
        log.debug("Executing Query: {}", query);
        var queryResult = adxClient.executeQuery(query);
        var result = queryResult.getPrimaryResults();
        return mapQueryResult(result);
    }


    private List<DataPoint> mapQueryResult(KustoResultSetTable mainResult) {
        List<DataPoint> resultList = new ArrayList<>(mainResult.count());
        while (mainResult.next()) {
            resultList.add(new DataPoint(mainResult.getDouble("longitude"),
                    mainResult.getDouble("latitude"),
                    mainResult.getInt("point_id"),
                    mainResult.getFloat("some_float"),
                    mainResult.getString("tags")
            ));
        }
        return resultList;
    }


    @SneakyThrows
    @Override
    public List<DataPoint> fetchArea(String tableName, Geometry shape, String parameter, float greaterThan) {
        String geoJson = geoJsonWriter.write(shape);
        String query = """
                set notruncation;
                let coveringS2Cells = datatable(poly:dynamic)[
                    dynamic({{geojson}})
                ];
                coveringS2Cells
                | extend s2level = geo_polygon_to_s2cells(poly)
                | mv-expand s2level to typeof(string)
                | extend smaller_poly = geo_intersection_2polygons(geo_s2cell_to_polygon(s2level), poly)
                | join kind=inner {{tableName}} on s2level
                | where {{parameter}} > {{greaterThan}}
                | where geo_point_in_polygon(longitude,latitude, smaller_poly)
                | project-away s2level, poly, smaller_poly
                """.replace("{{tableName}}", tableName)
                .replace("{{geojson}}", geoJson)
                .replace("{{parameter}}", parameter)
                .replace("{{greaterThan}}", "" + greaterThan);

        var queryResult = adxClient.executeQuery(query);
        return mapQueryResult(queryResult.getPrimaryResults());
    }

    @Override
    @SneakyThrows
    public void deleteCursors() {
        for (String name : sqrNames.keySet()) {
            adxClient.executeMgmt(".drop stored_query_result " + name);
        }
        sqrNames.clear();
    }


    @SneakyThrows
    @Override
    public void dropTables(String... tables) {
        for (String table : tables) {
            adxClient.executeMgmt(".drop table %s ifexists".formatted(table));
        }
    }

    @SneakyThrows
    @Override
    public int count(String tableName) {
        var queryResult = adxClient.executeQuery(tableName + "|summarize Count = count()");
        var results = queryResult.getPrimaryResults();
        results.next();
        return (int) results.getLong("Count");
    }

    @Override
    public String getVolume() {
        return "adx-volume";
    }
}
