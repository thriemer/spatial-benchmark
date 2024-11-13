package de.thriemer.spatial.framework;

import de.thriemer.spatial.benchmark.DataPoint;
import org.knowm.xchart.VectorGraphicsEncoder;
import org.knowm.xchart.internal.chartpart.Chart;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
public class Helper {

    private static boolean standAlone = true;

    public static String sensiblePrint(long ms) {
        return prettyPrint(Duration.ofMillis(ms));
    }

    private static String prettyPrint(Duration duration) {
        return duration.toString().substring(2).replaceAll("(\\d[HMS])(?!$)", "$1 ").toLowerCase();
    }

    public static void savePdf(Chart<?, ?> chart, String name) {
        String path = standAlone?"generated/pics/":"../arbeit/pics/";
        new File(path).mkdirs();

        try {
            VectorGraphicsEncoder.saveVectorGraphic(chart, path + name.replace(" ", "_") + ".pdf", VectorGraphicsEncoder.VectorGraphicsFormat.PDF);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void saveFile(String content, String name, String... additionalFolders){
        String subFolder = String.join("/", additionalFolders);
        String prefix = (standAlone?"generated/tex/":"../arbeit/tex/")+subFolder;
        new File(prefix).mkdirs();
        Path path = Path.of(prefix + name.replace(" ", "_") + ".tex");
        try {
            Files.writeString(path,content);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final double EPSILON = 1E-5;

    public static boolean isEqual(double a, double b) {
        return a == b || Math.abs(a - b) < EPSILON;
    }

    // lon = x,
    // lat = y

    private static GeometryFactory factory = new GeometryFactory();

    public static Geometry generateCircle(double centerLon, double centerLat, double radius, int vertices) {
        double dPhi = 2d * Math.PI / vertices;

        List<Coordinate> coords = IntStream.range(1, vertices).mapToObj(i -> {
            double phi = dPhi * i;
            double dLong = Math.cos(phi) * radius;
            double dLat = Math.sin(phi) * radius / 2d; // divide by to because latitude only has half of the mapping space
            return new Coordinate(centerLon + dLong, centerLat + dLat);
        }).collect(Collectors.toList());

        Coordinate startAndEnd = new Coordinate(centerLon + radius, centerLat);
        coords.addFirst(startAndEnd);
        coords.addLast(startAndEnd);

        return factory.createPolygon(coords.toArray(Coordinate[]::new));
    }

    public static String pointToString(DataPoint point, String color) {
        return """
                    {
                       "type":"Feature",
                       "properties":{
                          "marker-color":"%s",
                          "tags": "%s",
                          "some_float":"%f"
                       },
                       "geometry":{
                          "coordinates":[
                             %f,
                             %f
                          ],
                          "type":"Point"
                       }
                    }
                """.formatted(color, point.tags(), point.someFloat(), point.longitude(), point.latitude());
    }

}
