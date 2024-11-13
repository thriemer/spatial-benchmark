package de.thriemer.spatial.framework;

import de.thriemer.spatial.evaluation.Evaluation;
import de.thriemer.spatial.evaluation.ScenarioStatisticsEntity;
import de.thriemer.spatial.evaluation.SummaryStatistics;
import de.thriemer.spatial.evaluation.SummaryStatisticsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static de.thriemer.spatial.framework.Helper.sensiblePrint;

@Service
@Slf4j
@RequiredArgsConstructor
public class DatabaseStatisticCollector {

    public static final String QUERY_TIME = "Query Time";
    public static final String CPU_USAGE = "CPU usage";
    public static final String MEMORY_USAGE = "Memory usage";
    public static final String DISK_USAGE = "Disk Usage";

    final Settings settings;

    final SummaryStatisticsRepository repository;
    final Evaluation evaluation;

    public static boolean fastLane = false;
    public static boolean useVolume = true;

    final DatabaseAbstraction[] databaseAbstractions;
    final Scenario[] scenarios;
    final ResourceMonitor monitor;

    @EventListener(ApplicationReadyEvent.class)
    public void collectStatistics() {
        var shuffled = new ArrayList<>(List.of(databaseAbstractions));
        Collections.sort(shuffled, Comparator.comparing(DatabaseAbstraction::getName));

        // cleanup faulty runs
        if (settings.cleanFaultyRuns()) {
            var nullValueEntities = repository.getNullScenarioStatistics();
            if (!nullValueEntities.isEmpty()) {
                log.info("Cleaning up faulty runs....");
                for (var e : nullValueEntities) {
                    String scenario = e.name();
                    if (scenario.contains("-")) {
                        scenario = scenario.split("-")[0].trim();
                    }
                    log.info("Cleaning up scenario {} of database {} with param {}", scenario, e.getDatabase(), e.getParam());
                    repository.delete(e.getDatabase(), scenario, e.getParam());
                }
            }
        }

        if (settings.runBenchmark()) {

            for (var db : shuffled) {
                log.info("Preparing database '{}'", db.getName());
                db.setup();
                monitor.setDatabaseToMonitor(db);
                for (var scenario : scenarios) {
                    run(db, scenario);
                }
                db.cleanUp();
                System.gc();
            }
        }

        evaluation.analyseMetrics();
        monitor.stopMonitoring();
    }

    public void run(DatabaseAbstraction databaseAbstraction, Scenario scenario) {
        long globalStart = System.currentTimeMillis();
        log.info("Starting Scenario: '{}'", scenario.name);

        for (Object p : scenario.getParams()) {
            if (repository.exists(databaseAbstraction.getName(), scenario.name, toString(p))) {
                log.info("Skipping combination {} - {} - {} because there are already statistics for it", databaseAbstraction.getName(), scenario.name, toString(p));
                continue;
            }
            scenario.prepare(databaseAbstraction);
            monitor.mark(databaseAbstraction.getName(), p, scenario.name, ResourceMonitor.TimeMark.Event.START);
            try {
                log.info("Running scenario with parameter: {}", p);
                double confidenceLevel = 0.9;
                double intervalSize = 0.1;
                long warmUpStart = System.currentTimeMillis();
                log.info("Estimate sample count for {}% confidence level and {}% interval size", confidenceLevel * 100, intervalSize * 100);
                // estimate how many samples are necessary to read a 5% confidence level
                int testSamples = fastLane ? 30 : 100;
                while (scenario.getSampleCount() < testSamples) {
                    scenario.iterate(databaseAbstraction, p);
                }
                var result = scenario.getResult();
                int sampleCount = Evaluation.getSampleCount(result.avg(), result.std(), confidenceLevel, intervalSize);

                long warmUpTime = System.currentTimeMillis() - warmUpStart;
                double millisPerIteration = warmUpTime / (double) testSamples;
                double minuteLimit = fastLane ? 2 : 5;
                int iterationsInTwoMinutes = (int) (minuteLimit * 60_000 / millisPerIteration);
                if (iterationsInTwoMinutes < sampleCount) {
                    log.warn("Reducing the sample count from {} to {} to limit the estimated execution time to {} minutes. Estimation took: {}", sampleCount, iterationsInTwoMinutes, minuteLimit, sensiblePrint(warmUpTime));
                    sampleCount = iterationsInTwoMinutes;
                } else {
                    log.info("Sample count is {}. took {}", sampleCount + testSamples, sensiblePrint(warmUpTime));
                }

                log.info("Benchmarking... Estimated time is: {}", sensiblePrint((long) (millisPerIteration * sampleCount)));
                long benchmarkStart = System.currentTimeMillis();
                // iterate at least 30 times or one minute
                while (scenario.getSampleCount() < sampleCount || System.currentTimeMillis() - benchmarkStart < (fastLane ? 30_000 : 60_000)) {
                    scenario.iterate(databaseAbstraction, p);
                }
                log.info("Benchmark with param '{}' took: {}", p, sensiblePrint(System.currentTimeMillis() - benchmarkStart));

                var benchmarkResult = scenario.getResult();
                repository.save(ScenarioStatisticsEntity.from(databaseAbstraction.getName(), toString(p), QUERY_TIME, benchmarkResult));
            } catch (Exception x) {
                log.warn("{} failed because of {}", scenario.name, x.getMessage());
                x.printStackTrace();
                var benchmarkResult = new SummaryStatistics(scenario.name, SummaryStatistics.ERROR_IN_RUN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 0);
                repository.save(ScenarioStatisticsEntity.from(databaseAbstraction.getName(), toString(p), QUERY_TIME, benchmarkResult));
            }
            scenario.cleanup(databaseAbstraction);
            monitor.mark(databaseAbstraction.getName(), p, scenario.name, ResourceMonitor.TimeMark.Event.STOP);
            persistResourceStatistics(databaseAbstraction, scenario.name, p);
            System.gc();
        }

        log.info("Scenario '{}' took: {}", scenario.name, sensiblePrint(System.currentTimeMillis() - globalStart));
    }


    private void persistResourceStatistics(DatabaseAbstraction db, String scenario, Object param) {
        long amount = monitor.getDiskUsage(db);
        SummaryStatistics diskUsage = new SummaryStatistics(scenario, "bytes", amount, amount, amount, amount, 0, 3);
        repository.save(ScenarioStatisticsEntity.from(db.getName(), toString(param), DISK_USAGE, diskUsage));

        List<ResourceMonitor.SystemMetrics> metrics = monitor.queryMetricsFor(db, scenario, param);
        if (metrics.isEmpty()) {
            return;
        }
        SummaryStatistics avgCPUUsage = Evaluation.calculateArithmeticStatistics(scenario, "%", metrics.stream().map(ResourceMonitor.SystemMetrics::cpuPercentage).collect(Collectors.toList()));
        SummaryStatistics avgMemoryUsage = Evaluation.calculateArithmeticStatistics(scenario, "bytes", metrics.stream().map(ResourceMonitor.SystemMetrics::usedMemory).collect(Collectors.toList()));
        repository.saveAll(List.of(
                ScenarioStatisticsEntity.from(db.getName(), toString(param), CPU_USAGE, avgCPUUsage),
                ScenarioStatisticsEntity.from(db.getName(), toString(param), MEMORY_USAGE, avgMemoryUsage)
        ));
    }

    private static String toString(Object p) {
        return p == null ? "" : p.toString();
    }

}
