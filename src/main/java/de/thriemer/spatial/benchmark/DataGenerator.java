package de.thriemer.spatial.benchmark;

import java.util.Random;

public class DataGenerator {

    private final Random r;

    public DataGenerator() {
        r = new Random();
    }

    public DataGenerator(long seed) {
        r = new Random(seed);
    }

    public double generateInRange(double min, double max) {
        return r.nextDouble() * (max - min) + min;
    }

    private int id = 0;

    public DataPoint generateDataPoint(double lonMin, double lonMax, double latMin, double latMax) {
        return new DataPoint(generateInRange(lonMin, lonMax), generateInRange(latMin, latMax), id++, r.nextFloat(), "generated");
    }


}
