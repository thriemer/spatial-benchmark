package de.thriemer.spatial.framework;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ResourceMonitor implements BeanNameAware, ApplicationContextAware {

    private final Settings settings;

    private ApplicationContext applicationContext;
    private String beanName;
    Runtime runtime = Runtime.getRuntime();
    List<TimeMark> marks = new ArrayList<>();
    List<SystemMetrics> metrics = new ArrayList<>();

    AtomicLong dataUpdated = new AtomicLong(Instant.now().getEpochSecond());

    public List<SystemMetrics> queryMetricsFor(DatabaseAbstraction db, String scenario, Object param) {

        long start = marks.stream().filter(m -> m.database.equals(db.getName()) && m.scenarioName.equals(scenario) && m.parameter == param && m.event == TimeMark.Event.START).findFirst().get().unixTime;
        long stop = marks.stream().filter(m -> m.database.equals(db.getName()) && m.scenarioName.equals(scenario) && m.parameter == param && m.event == TimeMark.Event.STOP).findFirst().get().unixTime;
        waitForDataUpdate();
        return metrics.stream().filter(m -> m.unixTimestamp >= start && m.unixTimestamp <= stop).toList();
    }

    private void waitForDataUpdate() {
        var now = Instant.now().getEpochSecond();
        // the resource snapshot could contain data that is not yet in the list so this method has to wait
        int count = 0;
        while (dataUpdated.get() < now && count++ < 20) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            log.info("Waiting for data update. Waited {}s", count);
        }
    }

    String pids;
    private boolean headerSaved = false;
    final DatabaseAbstraction[] dbs;

    @SneakyThrows
    public ResourceMonitor(DatabaseAbstraction[] dbs, Settings settings) {
        this.settings = settings;
        this.dbs = dbs;
        pids = getPIDs(Arrays.stream(dbs).map(DatabaseAbstraction::getProcess).toArray(String[]::new));
        Files.deleteIfExists(Paths.get("systemMonitor.csv"));
    }

    public void setDatabaseToMonitor(DatabaseAbstraction db) {
        pids = getPIDs(new String[]{db.getProcess()});
        waitForDataUpdate();
    }

    List<String> csv = new ArrayList<>();

    @Scheduled(fixedRate = 10, timeUnit = TimeUnit.SECONDS)
    void recordSnapshot() {
        if(settings.runBenchmark()) {
            String[] lines = takeResourceSnapshot();
            List<SystemMetrics> rawMetrics = new ArrayList<>();
            for (String s : lines) {
                if (s.startsWith("1") || s.startsWith("#") && csv.isEmpty() && !headerSaved) {
                    headerSaved = true;
                    String[] splitWhiteSpace = s.replace("# ", "").split("\\s+", 19);
                    if (s.startsWith("1")) {
                        rawMetrics.add(SystemMetrics.from(splitWhiteSpace));
                    }
                    csv.add(String.join("|", splitWhiteSpace));
                }
            }
            metrics.addAll(condense(rawMetrics));
            dataUpdated.set(Instant.now().getEpochSecond());
        }
    }

    private List<SystemMetrics> condense(List<SystemMetrics> rawMetrics) {
        Map<Integer, List<SystemMetrics>> timestampedMetrics = new HashMap<>();
        for (var s : rawMetrics) {
            if (pids.contains(s.PID() + "")) {
                s.PID = Integer.parseInt(pids.split(",")[0]);
                int hash = Pair.of(s.PID, s.unixTimestamp()).hashCode();
                var list = timestampedMetrics.computeIfAbsent(hash, (k) -> new ArrayList<>());
                list.add(s);
            }
        }
        return timestampedMetrics.values().stream().map(l -> {
            var first = l.getFirst();
            return l.stream().reduce(new SystemMetrics(first.unixTimestamp(), first.PID(), 0, 0, 0, 0, 0, 0),
                    SystemMetrics::add);
        }).toList();
    }

    public void stopMonitoring() {
        ScheduledAnnotationBeanPostProcessor bean = applicationContext.getBean(ScheduledAnnotationBeanPostProcessor.class);
        bean.postProcessBeforeDestruction(this, beanName);
        ((ConfigurableApplicationContext) applicationContext).close();
    }


    @Override
    public void setBeanName(String name) {
        this.beanName = name;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }


    public record TimeMark(String database, Object parameter, String scenarioName, Event event, long unixTime) {
        public enum Event {
            START,
            STOP
        }
    }

    public long getDiskUsage(DatabaseAbstraction da) {
        try {

            var dirProcess = runtime.exec(new String[]{"docker", "volume", "inspect", "--format", "{{ .Mountpoint }}", da.getVolume()});
            dirProcess.waitFor();
            if (dirProcess.exitValue() != 0) {
                throw new RuntimeException("Something went wrong during docker command: " + new BufferedReader(new InputStreamReader(dirProcess.getErrorStream())).lines().collect(Collectors.joining()));
            }
            var dir = new BufferedReader(new InputStreamReader(dirProcess.getInputStream())).lines().collect(Collectors.joining());

            ProcessBuilder builder = new ProcessBuilder("sudo", "/usr/bin/du", "-sb", dir);
            Process process = builder.start();
            process.waitFor();
            if (process.exitValue() != 0) {
                throw new RuntimeException("Something went wrong during du command: " + new BufferedReader(new InputStreamReader(process.getErrorStream())).lines().collect(Collectors.joining()));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            return Long.parseLong(reader.lines().findFirst().get().split("\t")[0].trim());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class SystemMetrics {
        private final long unixTimestamp;
        long PID;
        private double cpuPercentage;
        private double usedCores;
        private double memoryPercentage;
        private double usedMemory;
        private double kBReadPerSecond;
        private double kbWritePerSecond;

        public SystemMetrics(long unixTimestamp, long PID, double cpuPercentage, double usedCores,
                             double memoryPercentage, double usedMemory,
                             double kBReadPerSecond, double kbWritePerSecond) {
            this.unixTimestamp = unixTimestamp;
            this.PID = PID;
            this.cpuPercentage = cpuPercentage;
            this.usedCores = usedCores;
            this.memoryPercentage = memoryPercentage;
            this.usedMemory = usedMemory;
            this.kBReadPerSecond = kBReadPerSecond;
            this.kbWritePerSecond = kbWritePerSecond;
        }

        public static SystemMetrics from(String[] row) {
            long timestamp = Long.parseLong(row[0]);
            long pid = Long.parseLong(row[2]);
            double cpuPercentUsage = Double.parseDouble(row[7]);
            var runtime = Runtime.getRuntime();
            double cpuUsage = cpuPercentUsage / 100.0 * runtime.availableProcessors();
            double memoryPercentUsage = Double.parseDouble(row[13]);
            double memoryUsage = memoryPercentUsage / 100.0 * runtime.maxMemory();
            double kbRead = Double.parseDouble(row[14]);
            double kbWrite = Double.parseDouble(row[15]);
            return new SystemMetrics(timestamp, pid, cpuPercentUsage, cpuUsage, memoryPercentUsage, memoryUsage, kbRead, kbWrite);
        }

        public long unixTimestamp() {
            return unixTimestamp;
        }

        public long PID() {
            return PID;
        }

        public double cpuPercentage() {
            return cpuPercentage;
        }

        public double usedCores() {
            return usedCores;
        }

        public double memoryPercentage() {
            return memoryPercentage;
        }

        public double usedMemory() {
            return usedMemory;
        }

        public double kBReadPerSecond() {
            return kBReadPerSecond;
        }

        public double kbWritePerSecond() {
            return kbWritePerSecond;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (SystemMetrics) obj;
            return this.unixTimestamp == that.unixTimestamp &&
                    this.PID == that.PID &&
                    Double.doubleToLongBits(this.cpuPercentage) == Double.doubleToLongBits(that.cpuPercentage) &&
                    Double.doubleToLongBits(this.usedCores) == Double.doubleToLongBits(that.usedCores) &&
                    Double.doubleToLongBits(this.memoryPercentage) == Double.doubleToLongBits(that.memoryPercentage) &&
                    Double.doubleToLongBits(this.usedMemory) == Double.doubleToLongBits(that.usedMemory) &&
                    Double.doubleToLongBits(this.kBReadPerSecond) == Double.doubleToLongBits(that.kBReadPerSecond) &&
                    Double.doubleToLongBits(this.kbWritePerSecond) == Double.doubleToLongBits(that.kbWritePerSecond);
        }

        @Override
        public int hashCode() {
            return Objects.hash(unixTimestamp, PID, cpuPercentage, usedCores, memoryPercentage, usedMemory, kBReadPerSecond, kbWritePerSecond);
        }

        @Override
        public String toString() {
            return "SystemMetrics[" +
                    "unixTimestamp=" + unixTimestamp + ", " +
                    "PID=" + PID + ", " +
                    "cpuPercentage=" + cpuPercentage + ", " +
                    "usedCores=" + usedCores + ", " +
                    "memoryPercentage=" + memoryPercentage + ", " +
                    "usedMemory=" + usedMemory + ", " +
                    "kBReadPerSecond=" + kBReadPerSecond + ", " +
                    "kbWritePerSecond=" + kbWritePerSecond + ']';
        }


        public SystemMetrics add(SystemMetrics other) {
            this.cpuPercentage += other.cpuPercentage();
            this.usedCores += other.usedCores();
            this.memoryPercentage += other.memoryPercentage();
            this.usedMemory += other.usedMemory();
            this.kBReadPerSecond += kBReadPerSecond();
            this.kbWritePerSecond += kbWritePerSecond();
            return this;
        }
    }

    public void mark(String database, Object parameter, String scenarioName, TimeMark.Event event) {
        marks.add(new TimeMark(database, parameter, scenarioName, event, Instant.now().getEpochSecond()));
    }

    @SneakyThrows
    String[] takeResourceSnapshot() {
        // this is possible because the command is set to not include a password in the sudoers file
        // it is necessary to have root privileges to get hard drive reads and writes
        ProcessBuilder builder = new ProcessBuilder("sudo", "/usr/bin/pidstat", "-dru", "-Hh", "-p", pids, "1", "10");
        builder.environment().put("LC_NUMERIC", "C.UTF-8");
        var process = builder.start();
        process.waitFor();
        if (process.exitValue() != 0) {
            throw new RuntimeException("Something went wrong during pidstat command: " + new BufferedReader(new InputStreamReader(process.getErrorStream())).lines().collect(Collectors.joining()));
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        return reader.lines().toArray(String[]::new);
    }


    String getPIDs(String[] processes) {
        return Arrays.stream(processes).map(p -> {
            try {
                Process process = runtime.exec(new String[]{"pgrep", p});
                process.waitFor();
                BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                return reader.lines().collect(Collectors.joining(","));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.joining(","));
    }

}
