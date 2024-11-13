package de.thriemer.spatial;

import com.github.dockerjava.api.model.Bind;
import de.thriemer.spatial.benchmark.DataPoint;
import de.thriemer.spatial.benchmark.Page;
import de.thriemer.spatial.framework.DataType;
import de.thriemer.spatial.framework.DatabaseAbstraction;
import de.thriemer.spatial.framework.DatabaseStatisticCollector;
import io.vavr.Tuple2;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.GeoLocation;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.mapping.*;
import org.opensearch.client.opensearch._types.query_dsl.GeoPolygonPoints;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.RangeQuery;
import org.opensearch.client.opensearch.core.*;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexResponse;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.opensearch.testcontainers.OpensearchContainer;
import org.springframework.stereotype.Service;
import org.testcontainers.utility.DockerImageName;

import java.util.*;

@Service
@Slf4j
public class OpenSearchDatabase extends DatabaseAbstraction {

    //    RDBMS => Databases => Tables => Columns/Rows
    //    Opensearch => Clusters => Indices => Shards => Documents with key-weight pairs

    OpenSearchClient osc;

    @Override
    public String getName() {
        return "OpenSearch";
    }

    @Override
    public String getProcess() {
        return "java";
    }

    @SneakyThrows
    @Override
    public void setup() {
        container = new OpensearchContainer<>(DockerImageName.parse("opensearchproject/opensearch:2.17.0"));
        if (DatabaseStatisticCollector.useVolume) {
            container.setBinds(List.of(Bind.parse("opensearch-volume:/usr/share/opensearch/data")));
        }
        container.start();

        final HttpHost host = new HttpHost("http", "localhost", container.getFirstMappedPort());
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(host), new UsernamePasswordCredentials("admin", "admin".toCharArray()));

        final ApacheHttpClient5TransportBuilder builder = ApacheHttpClient5TransportBuilder.builder(host);
        builder.setHttpClientConfigCallback(b -> b.setDefaultCredentialsProvider(credentialsProvider));

        osc = new OpenSearchClient(builder.build());
    }

    @SneakyThrows
    @Override
    public boolean tableExists(String tableName) {
        var response = osc.indices().exists(new ExistsRequest.Builder().index(tableName).build());
        return response.value();
    }

    @SneakyThrows
    @Override
    public void createTable(String tableName, Tuple2<String, DataType>... columns) {

        var types = new TypeMapping.Builder();
        for (var t : columns) {
            switch (t._2) {
                case DOUBLE -> types.properties(t._1, new Property(DoubleNumberProperty.of(b -> b)));
                case INT -> types.properties(t._1, new Property(IntegerNumberProperty.of(b -> b)));
                case GEO_POINT -> types.properties(t._1, new Property(GeoPointProperty.of(b -> b)));
                case STRING -> types.properties(t._1, new Property(TextProperty.of(b -> b)));
                case GEOMETRY -> types.properties(t._1, new Property(GeoShapeProperty.of(b -> b)));
            }
        }

        CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder().index(tableName).mappings(types.build()).build();
        osc.indices().create(createIndexRequest);
    }

    @Override
    @SneakyThrows
    public Envelope getDataBoundingBox(String table, String column) {
        var result = osc.search(s -> s.index(table).aggregations(column, a -> a.geoBounds(b -> b.field(column))), Object.class);
        var bb = result.aggregations().get(column).geoBounds().bounds().tlbr();
        //x = longtitude

        return new Envelope(bb.topLeft().latlon().lon(), bb.bottomRight().latlon().lon(),
                bb.topLeft().latlon().lat(), bb.bottomRight().latlon().lat());
    }


    @Override
    @SneakyThrows
    public void persistMultiplePoints(String tableName, List<DataPoint> dataPoints) {
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (var point : dataPoints) {
            var p = OpenSearchDataPoint.from(point);
            br.operations(op -> op.index(idx -> idx.index(tableName).id(p.getPoint_id() + "").document(p)));
        }

        BulkResponse result = osc.bulk(br.build());

        if (result.errors()) {
            log.error("Bulk had errors");
            for (BulkResponseItem item : result.items()) {
                if (item.error() != null) {
                    log.error(item.error().reason());
                }
            }
        }
        osc.indices().refresh(r -> r.index(tableName));
    }

    @Override
    public void createIndex(String tableName, String... params) {
        // There are no indexes?
    }

    record Cursor(int maxPages, int maxElements, Map<Integer, Integer> pages) {
        Optional<Integer> getMaxPage() {
            return pages.keySet().stream().max(Comparator.comparingInt(a -> a));
        }
    }

    Map<Integer, Cursor> pages = new HashMap<>();


    @Override
    @SneakyThrows
    public Page<DataPoint> fetchArea(String tableName, Geometry shape, int page, int pageSize) {
        GeoPolygonPoints poly = convertToGeoPolygonPoints(shape);

        int key = Objects.hash(tableName, shape, pageSize);
        if (!pages.containsKey(key)) {

            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(tableName)
                    .sort(SortOptions.of(s -> s.field(f -> f.field("point_id"))))
                    .query(new Query.Builder().geoPolygon(p -> p.field("pos").polygon(poly)).build())
                    .aggregations("count", a -> a.valueCount(c -> c.field("point_id")))
                    .build();

            var result = osc.search(searchRequest, OpenSearchDataPoint.class);
            int elements = (int) result.aggregations().get("count").valueCount().value();
            pages.put(key, new Cursor(elements / pageSize, elements, new HashMap<>()));
        }
        var cursor = pages.get(key);

        if (cursor.maxElements == 0) {
            return new Page<>(page, cursor.maxPages(), new ArrayList<>());
        }

        var maxPage = cursor.getMaxPage();

        if (maxPage.isEmpty()) {
            List<DataPoint> points = fetchComplete(tableName, poly, pageSize, 0, true);
            cursor.pages.put(0, points.getLast().id());
            if (page == 0) {
                return new Page<>(page, cursor.maxPages(), points);
            }
        }
        // scroll to page
        for (int i = cursor.getMaxPage().get() + 1; i < page; i++) {
            int searchAfter = cursor.pages.get(i - 1);
            List<DataPoint> points = fetchComplete(tableName, poly, pageSize, searchAfter, false);
            cursor.pages.put(i, points.getLast().id());
        }

        int searchAfter = cursor.pages.get(page - 1);
        List<DataPoint> points = fetchComplete(tableName, poly, pageSize, searchAfter, false);
        if (!points.isEmpty()) {
            cursor.pages.put(page, points.getLast().id());
        }

        return new Page<>(page, cursor.maxPages(), points);
    }

    @SneakyThrows
    private List<DataPoint> fetchComplete(String tableName, GeoPolygonPoints poly, int size, int searchAfter, boolean firstPage) {
        List<DataPoint> results = new ArrayList<>();
        while (size > 0) {

            var builder = new SearchRequest.Builder()
                    .index(tableName)
                    .size(Math.min(size, 10_000))
                    .sort(SortOptions.of(s -> s.field(f -> f.field("point_id"))))
                    .query(new Query.Builder().geoPolygon(p -> p.field("pos").polygon(poly)).build());

            if (!firstPage) {
                builder = builder.searchAfter("" + searchAfter);
            }
            var result = osc.search(builder.build(), OpenSearchDataPoint.class);
            result.hits().hits().stream().map(h -> h.source().toDataPoint()).forEach(results::add);
            if (result.hits().hits().isEmpty()) {
                break;
            }
            size -= result.hits().hits().size();
            searchAfter = results.getLast().id();
            firstPage = false;
        }

        return results;
    }

    @Override
    @SneakyThrows
    public List<DataPoint> fetchArea(String tableName, Geometry shape) {
        GeoPolygonPoints poly = convertToGeoPolygonPoints(shape);
        var query = new Query.Builder().geoPolygon(p -> p.field("pos").polygon(poly)).build();
        return scrollFetch(tableName, query);
    }

    private GeoPolygonPoints convertToGeoPolygonPoints(Geometry shape) {
        return new GeoPolygonPoints.Builder().points(
                        Arrays.stream(shape.getCoordinates())
                                .map(c -> new GeoLocation.Builder()
                                        .latlon(l -> l.lat(c.getY()).lon(c.getX()))
                                        .build()
                                )
                                .toList()
                )
                .build();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class OpenSearchDataPoint {
        private static final WKTReader reader = new WKTReader();
        String pos;
        int point_id;
        float some_float;
        String tags;

        public static OpenSearchDataPoint from(DataPoint d) {
            return new OpenSearchDataPoint("POINT(" + d.longitude() + " " + d.latitude() + ")", d.id(), d.someFloat(), d.tags());
        }

        @SneakyThrows
        public DataPoint toDataPoint() {
            Geometry g = reader.read(this.pos);
            return new DataPoint(g.getCoordinate().getX(), g.getCoordinate().getY(), this.point_id, this.some_float, this.tags);
        }

    }

    @Override
    @SneakyThrows
    public List<DataPoint> fetchArea(String tableName, Geometry contractPolygon, Geometry filter) {

        GeoPolygonPoints poly = convertToGeoPolygonPoints(contractPolygon);
        GeoPolygonPoints poly1 = convertToGeoPolygonPoints(filter);

        var query = new Query.Builder().bool(b -> b.must(
                new Query.Builder().geoPolygon(p -> p.field("pos").polygon(poly)).build(),
                new Query.Builder().geoPolygon(p -> p.field("pos").polygon(poly1)).build()
        )).build();

        return scrollFetch(tableName, query);
    }

    @Override
    @SneakyThrows
    public List<DataPoint> fetchArea(String tableName, Geometry shape, String fieldName, float greaterThan) {
        GeoPolygonPoints poly = convertToGeoPolygonPoints(shape);

        Query gtQuery = new RangeQuery.Builder().field(fieldName).gt(JsonData.of(greaterThan)).build().toQuery();
        Query polyQuery = new Query.Builder().geoPolygon(gp -> gp.field("pos").polygon(poly)).build();
        Query query = new Query.Builder().bool(b -> b.must(gtQuery, polyQuery)).build();
        return scrollFetch(tableName, query);
    }

    @SneakyThrows
    private List<DataPoint> scrollFetch(String tableName, Query query) {
        List<DataPoint> dataPoints = new ArrayList<>();

        var searchBuilder = new SearchRequest.Builder()
                .index(tableName)
                .query(query)
                .size(10_000)
                .scroll(t -> t.time("1m"));
        var queryResult = osc.search(searchBuilder.build(), OpenSearchDataPoint.class);
        queryResult.hits().hits().stream().map(h -> h.source().toDataPoint()).forEach(dataPoints::add);
        String scrollId = queryResult.scrollId();

        var hits = queryResult.hits().hits();

        while (hits != null && !hits.isEmpty()) {
            var result = osc.scroll(new ScrollRequest.Builder().scrollId(scrollId).scroll(t -> t.time("1m")).build(), OpenSearchDataPoint.class);
            scrollId = result.scrollId();
            hits = result.hits().hits();
            result.hits().hits().stream().map(h -> h.source().toDataPoint()).forEach(dataPoints::add);
        }

        if (scrollId != null) {
            osc.clearScroll(new ClearScrollRequest.Builder().scrollId(scrollId).build());
        }

        return dataPoints;
    }

    @Override
    @SneakyThrows
    public void deleteCursors() {
        pages.clear();
    }

    @Override
    @SneakyThrows
    public void dropTables(String... tables) {
        for (String t : tables) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest.Builder().index(t).build();
            DeleteIndexResponse deleteIndexResponse = osc.indices().delete(deleteIndexRequest);
        }
    }

    @SneakyThrows
    @Override
    public int count(String tableName) {
        return (int) osc.count(new CountRequest.Builder().index(tableName).build()).count();
    }

    @Override
    public String getVolume() {
        return "opensearch-volume";
    }
}
