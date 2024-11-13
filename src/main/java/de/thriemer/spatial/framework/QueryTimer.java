package de.thriemer.spatial.framework;

import com.jakewharton.fliptables.FlipTable;
import lombok.NoArgsConstructor;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

@NoArgsConstructor
public class QueryTimer {

    private static final DecimalFormat df = new DecimalFormat("0.00");
    private final List<Double> recordedResults = new ArrayList<>();
    private final List<SummaryStatistics> savedRuns = new ArrayList<>();
    private long start;

    public QueryTimer(QueryTimer other) {
        this.recordedResults.addAll(other.recordedResults);
        this.savedRuns.addAll(other.savedRuns);
    }

    public void start() {
        start = System.nanoTime();
    }

    public void end() {
        double diff = (System.nanoTime() - start) / 1000_000d;
        recordedResults.add(diff);
    }

    public void resetBucket() {
        recordedResults.clear();
    }

    public void resetAll() {
        resetBucket();
        savedRuns.clear();
    }

    public void saveCurrentResults(String name, double scale) {
        savedRuns.add(calculateStatistics(name, scale));
    }

    public void saveCurrentResults(String name) {
        savedRuns.add(calculateStatistics(name, 1d));
    }

    record SummaryStatistics(String name, double avg, double first, double min, double max, double std) {

    }

    private SummaryStatistics calculateStatistics(String name, double scale) {
        var statistics = recordedResults.stream().mapToDouble(i -> i).summaryStatistics();

        double std = 0;

        for (double l : recordedResults) {
            std += Math.pow(l * scale - statistics.getAverage() * scale, 2) / recordedResults.size();
        }

        std = Math.sqrt(std);

        return new SummaryStatistics(name, statistics.getAverage() * scale, recordedResults.getFirst() * scale, statistics.getMin() * scale, statistics.getMax() * scale, std);
    }

    public static QueryTimer combineTimerResults(QueryTimer... timers) {
        QueryTimer combined = new QueryTimer();
        for (var t : timers) {
            combined.savedRuns.addAll(t.savedRuns);
        }
        return combined;
    }

    public String getPrettyResults() {
        double scale = 1d;
        if (savedRuns.isEmpty()) {
            String[] header = new String[]{"Avg", "First", "Min", "Max", "Std"};

            var statistics = calculateStatistics("default", scale);

            String[][] content = new String[][]{{statistics.avg() + "", statistics.first() + "", statistics.min() + "", statistics.max() + "", df.format(statistics.std())}};
            return FlipTable.of(header, content);
        }

        if (!recordedResults.isEmpty()) {
            saveCurrentResults("Last Run", scale);
        }

        String[] header = new String[]{"Run", "Avg", "First", "Min", "Max", "Std"};

        String[][] content = savedRuns.stream().map(statistics -> new String[]{statistics.name(), statistics.avg() + "", statistics.first() + "", statistics.min() + "", statistics.max() + "", df.format(statistics.std())}).toArray(String[][]::new);
        return FlipTable.of(header, content);
    }

}
