package de.thriemer.spatial.framework;

import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

public class QueryTimer {

    private final List<Double> recordedMeasurements = new ArrayList<>();
    private long start;


    public void start() {
        start = System.nanoTime();
    }

    public void end() {
        double diff = (System.nanoTime() - start) / 1000_000d;
        recordedMeasurements.add(diff);
    }

}
