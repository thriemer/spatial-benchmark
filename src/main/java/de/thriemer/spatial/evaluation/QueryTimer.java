package de.thriemer.spatial.evaluation;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@NoArgsConstructor
public class QueryTimer {

    @Getter
    private final List<Double> recordedResults = new ArrayList<>();
    private long start;

    public void start() {
        start = System.nanoTime();
    }

    public void end() {
        double diff = (System.nanoTime() - start) / 1000_000d;
        recordedResults.add(diff);
    }

    public void resetAll() {
        recordedResults.clear();
    }

    public String getUnit() {
        return "ms";
    }

}
