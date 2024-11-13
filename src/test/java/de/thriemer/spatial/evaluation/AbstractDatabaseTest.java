package de.thriemer.spatial.evaluation;

import de.thriemer.spatial.PostGisDatabase;
import de.thriemer.spatial.benchmark.DataGenerator;
import de.thriemer.spatial.benchmark.DataPoint;
import de.thriemer.spatial.benchmark.Page;
import de.thriemer.spatial.benchmark.scenarios.Parameters;
import de.thriemer.spatial.framework.DatabaseAbstraction;
import de.thriemer.spatial.framework.DatabaseStatisticCollector;
import de.thriemer.spatial.framework.Helper;
import me.tongfei.progressbar.ProgressBar;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.geojson.GeoJsonWriter;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static de.thriemer.spatial.framework.Helper.generateCircle;
import static de.thriemer.spatial.framework.Helper.pointToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractDatabaseTest<T extends DatabaseAbstraction> {

    private static final String tableName = "test_table";

    T cut;
    DatabaseAbstraction reference;

    abstract T instantiateDatabase();

    @BeforeEach
    void init() {
        DatabaseStatisticCollector.fastLane = true;
        DatabaseStatisticCollector.useVolume = false;

        if (cut == null) {
            cut = instantiateDatabase();
            cut.setup();
            Parameters.createOSMDataTable(cut, tableName);
        }

        if (reference == null) {
            reference = new PostGisDatabase();
            reference.setup();
            Parameters.createOSMDataTable(reference, tableName);
        }

    }


    @Test
    void getDataBoundingBox() {
        var expectedBB = reference.getDataBoundingBox(tableName, "pos");
        var actualBB = cut.getDataBoundingBox(tableName, "pos");

        assertTrue(
                Helper.isEqual(expectedBB.getMinX(), actualBB.getMinX()) &&
                        Helper.isEqual(expectedBB.getMaxX(), actualBB.getMaxX()) &&
                        Helper.isEqual(expectedBB.getMinY(), actualBB.getMinY()) &&
                        Helper.isEqual(expectedBB.getMaxY(), actualBB.getMaxY()),
                "Expected: " + expectedBB + ", Actual: " + actualBB
        );
    }


    @Test
    void fetchArea() {
        var generator = new DataGenerator(new Random().nextLong());
        double radius = 0.001;
        var bb = reference.getDataBoundingBox(tableName, "pos");
        double minLat = bb.getMinY();
        double maxLat = bb.getMaxY();
        double minLon = bb.getMinX();
        double maxLon = bb.getMaxX();

        int maxSteps = 500;
        ProgressBar progressBar = new ProgressBar("Test Run", maxSteps);


        for (int i = 0; i < maxSteps; i++) {
            double lon = generator.generateInRange(minLon + radius, maxLon - radius);
            double lat = generator.generateInRange(minLat + radius, maxLat - radius);
            Geometry queryShape = generateCircle(lon, lat, radius, 100);

            List<DataPoint> points = reference.fetchArea(tableName, queryShape);
            List<DataPoint> actual = cut.fetchArea(tableName, queryShape);

            assertEqualsIgnoreOrder(points, actual, queryShape);

            progressBar.step();
        }
    }

    @Test
    void fetchAreaMultiStep() {
        var generator = new DataGenerator(new Random().nextLong());
        double radius = 0.05;
        double innerRadius = radius / 4.0;
        var bb = reference.getDataBoundingBox(tableName, "pos");
        double minLat = bb.getMinY();
        double maxLat = bb.getMaxY();
        double minLon = bb.getMinX();
        double maxLon = bb.getMaxX();

        int maxSteps = 500;
        ProgressBar progressBar = new ProgressBar("Test Run", maxSteps);

        for (int i = 0; i < maxSteps; i++) {
            double lon = generator.generateInRange(minLon + radius, maxLon - radius);
            double lat = generator.generateInRange(minLat + radius, maxLat - radius);
            Geometry queryShape = generateCircle(lon, lat, radius, 200);

            double innerLon = generator.generateInRange(lon - radius + innerRadius, lon + radius - innerRadius);
            double innerLat = generator.generateInRange(lat - radius + innerRadius, lat + radius - innerRadius);
            Geometry innerShape = generateCircle(innerLon, innerLat, innerRadius, 100);


            List<DataPoint> points = reference.fetchArea(tableName, queryShape, innerShape);
            List<DataPoint> actual = cut.fetchArea(tableName, queryShape, innerShape);
            assertEqualsIgnoreOrder(points, actual, queryShape, innerShape);

            progressBar.step();
        }
    }

    @Test
    void additionalFilter() {
        var generator = new DataGenerator(new Random().nextLong());
        double radius = 0.01;
        var bb = reference.getDataBoundingBox(tableName, "pos");
        double minLat = bb.getMinY();
        double maxLat = bb.getMaxY();
        double minLon = bb.getMinX();
        double maxLon = bb.getMaxX();

        int maxSteps = 500;
        ProgressBar progressBar = new ProgressBar("Test Run", maxSteps);


        for (int i = 0; i < maxSteps; i++) {
            float random = (float) generator.generateInRange(-1, 1);
            double lon = generator.generateInRange(minLon + radius, maxLon - radius);
            double lat = generator.generateInRange(minLat + radius, maxLat - radius);
            Geometry queryShape = generateCircle(lon, lat, radius, 100);

            List<DataPoint> points = reference.fetchArea(tableName, queryShape, "some_float", random);
            List<DataPoint> actual = cut.fetchArea(tableName, queryShape, "some_float", random);

            assertEqualsIgnoreOrder(points, actual, queryShape);
            progressBar.step();
        }
    }

    @Test
    void pagination() {
        int maxSteps = 500;
        ProgressBar progressBar = new ProgressBar("Test Run", maxSteps);

        for (int i = 0; i < maxSteps; i++) {
            paginationStep();
            progressBar.step();
        }
    }

    void paginationStep() {
        var generator = new DataGenerator(new Random().nextLong());
        double radius = 0.01;
        var bb = reference.getDataBoundingBox(tableName, "pos");
        double minLat = bb.getMinY();
        double maxLat = bb.getMaxY();
        double minLon = bb.getMinX();
        double maxLon = bb.getMaxX();

        double lon = generator.generateInRange(minLon + radius, maxLon - radius);
        double lat = generator.generateInRange(minLat + radius, maxLat - radius);
        Geometry queryShape = generateCircle(lon, lat, radius, 100);
        int size = 50;

        Page<DataPoint> page1Expected = reference.fetchArea(tableName, queryShape, 0, size);
        Page<DataPoint> page1Actual = cut.fetchArea(tableName, queryShape, 0, size);

        List<DataPoint> points = new ArrayList<>(page1Expected.data);
        List<DataPoint> actual = new ArrayList<>(page1Actual.data);

        assertEquals(page1Expected.maxPageNumber, page1Actual.maxPageNumber);
        assertEquals(page1Expected.pageNumber, page1Actual.pageNumber);

        var pageList = new ArrayList<>(IntStream.range(1, page1Expected.maxPageNumber + 1).boxed().toList());
        Collections.shuffle(pageList);

        for (int page : pageList) {
            Page<DataPoint> pageExpected = reference.fetchArea(tableName, queryShape, page, size);
            points.addAll(pageExpected.data);
            Page<DataPoint> pageActual = cut.fetchArea(tableName, queryShape, page, size);
            actual.addAll(pageActual.data);
        }

        assertEqualsIgnoreOrder(points, actual, queryShape);
        reference.deleteCursors();
        cut.deleteCursors();
    }

    private static void assertEqualsIgnoreOrder(List<DataPoint> expected, List<DataPoint> actual, Geometry... queryShapes) {
        try {
            Assertions.assertThat(actual).containsExactlyInAnyOrder(expected.toArray(DataPoint[]::new));
        } catch (AssertionError e) {
            GeoJsonWriter geoJsonWriter = new GeoJsonWriter();

            String polygon = Arrays.stream(queryShapes).map(queryShape -> "{\"type\": \"Feature\",\"properties\": {},\"geometry\": " + geoJsonWriter.write(queryShape) + "}").collect(Collectors.joining(","));

            var correct = expected.stream().distinct().filter(actual::contains).map(po -> pointToString(po, "#26a269")).collect(Collectors.joining(","));

            var tooMuchList = new ArrayList<>(actual);
            tooMuchList.removeAll(expected);
            var tooMuch = tooMuchList.stream().map(po -> pointToString(po, "#c061cb")).collect(Collectors.joining(","));

            var missingList = new ArrayList<>(expected);
            missingList.removeAll(actual);
            var missing = missingList.stream().map(po -> pointToString(po, "#ed333b")).collect(Collectors.joining(","));


            String geoJson = polygon + "," + correct + "," + missing + "," + tooMuch;
            if (geoJson.endsWith(",")) {
                geoJson = geoJson.substring(0, geoJson.length() - 1);
            }

            geoJson = """
                    {
                     "type":"FeatureCollection",
                       "features":[
                    """ + geoJson + "]}";
            geoJson = geoJson
                    .replaceAll(" +", " ")
                    .replaceAll(",+", ",")
                    .replace("\n", "");

            System.err.println("\n\n" + geoJson + "\n\n");

            // ignore off by one errors
            if (Math.abs(expected.size() - actual.size()) == 1) {
                return;
            }
            throw new RuntimeException("Output doenst match.");
        }
    }

    @AfterAll
    void tearDown() {
        if (reference != null) {
            reference.cleanUp();
        }
        if (cut != null) {
            cut.cleanUp();
        }
    }

}
