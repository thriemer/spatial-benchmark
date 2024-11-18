package de.thriemer.spatial.evaluation;

import com.jakewharton.fliptables.FlipTable;
import de.thriemer.spatial.benchmark.scenarios.InsertScenario;
import de.thriemer.spatial.benchmark.scenarios.PaginationScenario;
import de.thriemer.spatial.evaluation.ahp.AhpSolver;
import de.thriemer.spatial.framework.DatabaseStatisticCollector;
import de.thriemer.spatial.framework.Helper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.awt.*;
import java.text.DecimalFormat;
import java.util.List;
import java.util.*;

@Service
@RequiredArgsConstructor
public class Evaluation {

    public static final DecimalFormat df = new DecimalFormat("0.00");
    final SummaryStatisticsRepository summaryStatisticsRepository;
    final ComparisonRepository comparisonRepository;
    final AhpSolver ahpSolver;
    final DataSource source;

    final static int chartWidth = 700;
    final static int chartHeight = (int) ((12 / 16f) * chartWidth);

    String baseline = "Azure Data Explorer";

    @SneakyThrows
    public void analyseMetrics() {

        List<String> scenarioNames = summaryStatisticsRepository.getScenarioNames();
        List<String> databases = summaryStatisticsRepository.getAllDatabases();

        StringBuilder latexFileContent = new StringBuilder();
        for (String db : databases) {
            var s = summaryStatisticsRepository.getAllByDatabase(db);

            latexFileContent.append(formatLatexStatistics(db + " statistics", "table:" + db.replace(" ", "_" + "statistics"), s));

            System.out.println("------ " + db + " ------");
            System.out.println(formatStatistics(s));
            System.out.println();
        }

        comparisonRepository.deleteAll();

        new JdbcTemplate(source).update("""
                insert into Comparison_Entity (base_Database, COMPARED_DATABASE, scenario, type, param, speed_up, speed_up_std, SPEED_UPSE)
                select
                   base.database,
                   other.database,
                   base.name,
                   base.type,
                   base.param,
                   base.avg / other.avg,
                   base.avg / other.avg * SQRT(POWER(base.std / base.avg, 2.0) + POWER(other.std / other.avg, 2.0)),
                   base.avg / other.avg * SQRT(POWER(base.std / SQRT(base.sample_count) / base.avg, 2.0) + POWER(other.std / SQRT(other.sample_count) / other.avg, 2.0))
                from
                   Scenario_Statistics_Entity base
                   join
                      Scenario_Statistics_Entity other
                      ON base.name = other.name
                      and base.param = other.param
                      and base.type = other.type
                      and base.unit != 'error'
                      and other.unit != 'error'
                """);

        List<String> pureScenarioNames = scenarioNames.stream().filter(s -> !s.contains("-")).toList();

        System.out.println("---- EFFICIENCY COMPARISON BASELINE: " + baseline + " -----");
        var accumulatedImprovement = comparisonRepository.computeEfficiency(baseline).stream()
                .map(c -> new String[]{c.getDatabase(), c.getMetric(), df.format(c.getSpeedUp()), df.format(c.getStDev()), computeConfidenceInterval(c.getSpeedUp(), c.getStandardError(), 0.9d).toString(df)})
                .toArray(String[][]::new);

        String[] accumEfficiencyHeader = new String[]{"Database", "Metric", "Normalized Average Improvement", "StDev", "Confidence Interval"};
        System.out.println(FlipTable.of(accumEfficiencyHeader, accumulatedImprovement));

        String efficiencyTable = LatexTableGenerator.generateTable(accumEfficiencyHeader, accumulatedImprovement, "|l|l|r|r|r|", false);

        System.out.println("---- FINAL COMPARISON BASELINE: " + baseline + " -----");
        String[] comparisonHeader = new String[]{"Database", "Average Speedup", "StDev", "Confidence Interval"};

        String[][] accumulatedComparison = comparisonRepository.combineComparison(baseline, DatabaseStatisticCollector.QUERY_TIME)
                .stream()
                .map(c -> new String[]{c.getDatabase(), df.format(c.getSpeedUp()), df.format(c.getStDev()), computeConfidenceInterval(c.getSpeedUp(), c.getStandardError(), 0.9d).toString(df)})
                .toArray(String[][]::new);
        System.out.println(FlipTable.of(comparisonHeader, accumulatedComparison));
        String speedUpTable = LatexTableGenerator.generateTable(comparisonHeader, accumulatedComparison, "|l|r|r|c|", false);

        var tree = ahpSolver.solve();

        Helper.saveFile(tree.printLatex(), "ahp_hierarchy");
        Helper.saveFile(latexFileContent.toString(), "result_table");
        Helper.saveFile(speedUpTable, "speedup");
        Helper.saveFile(efficiencyTable, "efficiency");

        createNormalisedChart(new InsertScenario().name, "Batch size", "Time / Batch size");
        createNormalisedChart(new PaginationScenario().name, "Page size", "Time / Page size");
        createEfficiencyNormalisedBarChart();
        createCombinationBarChart();
        createDatabaseBarChart("Multiple geolocation filters");
        createDatabaseBarChart("Pagination Scenario Random Access");

        var pointDiagrams = Map.of(
                "Polygon filter complexity", "Vertex count",
                "Batch Insert Point", "Batch size",
                "Pagination Scenario Fetch Pages", "Page size",
                "Pagination Scenario First Run", "Page size"
        );

        for (var entry : pointDiagrams.entrySet()) {
            createPointChart(entry.getKey(), entry.getValue());
        }
    }

    private void createNormalisedChart(String scenario, String xAxisTitle, String yAxisTitle) {

        var dbNames = summaryStatisticsRepository.getAllDatabases();
        var list = summaryStatisticsRepository.findAllByNameAndType(scenario, DatabaseStatisticCollector.QUERY_TIME);

        XYChart chart = new XYChartBuilder().width(chartWidth).height(chartHeight)
                .xAxisTitle(xAxisTitle)
                .yAxisTitle("Log " + yAxisTitle + " [" + list.getFirst().unit + "]")
                .build();

        chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        chart.getStyler().setChartTitleVisible(false);
        chart.getStyler().setAxisTitlesVisible(true);
        chart.getStyler().setChartBackgroundColor(new Color(255, 255, 255, 0));
        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
        chart.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);

        chart.getStyler().setYAxisLogarithmic(true);

        chart.getStyler().setXAxisLogarithmic(true);

        try {
            for (String dbName : dbNames) {
                List<Double> xData = new ArrayList<Double>();
                List<Double> yData = new ArrayList<Double>();
                List<Double> errorBars = new ArrayList<Double>();
                for (var s : list.stream().filter(e -> e.getDatabase().equals(dbName)).toList()) {
                    if (!s.unit().equals("error")) {
                        double confidenceSize = confidenceSize(s.standardError(), 0.95);
                        Double param = Double.parseDouble(s.getParam());
                        yData.add(s.avg / param);
                        xData.add(param);
                        errorBars.add(confidenceSize / param);
                    }
                }
                chart.addSeries(dbName, xData, yData, errorBars);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        Helper.savePdf(chart, "normalised_" + scenario);

    }


    private void createCombinationBarChart() {
        String scenario = "Combination of spatial with non spatial filter";
        String xAxisTitle = "Query";
        var dbNames = summaryStatisticsRepository.getAllDatabases();
        var list = summaryStatisticsRepository.findAllByNameAndType(scenario, DatabaseStatisticCollector.QUERY_TIME);
        String yAxisTitle = "Query time" + " [" + list.getFirst().unit() + "]";
        var chart = configureCategoryChart(xAxisTitle, yAxisTitle);

        for (String dbName : dbNames) {
            List<String> xData = new ArrayList<>();
            List<Double> yData = new ArrayList<>();
            List<Double> errorBars = new ArrayList<>();
            for (var s : list.stream().filter(e -> e.getDatabase().equals(dbName)).toList()) {
                if (!s.unit().equals("error")) {
                    double confidenceSize = confidenceSize(s.standardError(), 0.95);
                    yData.add(s.avg);
                    xData.add(s.getParam());
                    errorBars.add(confidenceSize);

                }
            }
            chart.addSeries(dbName, xData, yData, errorBars);
        }

        Helper.savePdf(chart, scenario);
    }

    private void createEfficiencyNormalisedBarChart() {
        String scenario = "Log Normalized Speedup";
        String xAxisTitle = "Resource type";
        String yAxisTitle = "Normalized speedup";
        var dbNames = summaryStatisticsRepository.getAllDatabases();
        var list = comparisonRepository.computeEfficiency(baseline);

        var chart = configureCategoryChart(xAxisTitle, yAxisTitle);
        chart.getStyler().setYAxisLogarithmic(true);
        chart.getStyler().setYAxisMin(0.02);

        for (String dbName : dbNames) {
            List<String> xData = new ArrayList<>();
            List<Double> yData = new ArrayList<>();
            for (var s : list.stream().filter(e -> e.getDatabase().equals(dbName)).toList()) {
                yData.add(s.getSpeedUp());
                xData.add(s.getMetric());
            }
            chart.addSeries(dbName, xData, yData);
        }

        Helper.savePdf(chart, scenario);
    }

    private void createDatabaseBarChart(String scenario) {
        String xAxisTitle = "Database";
        var list = summaryStatisticsRepository.findAllByNameAndType(scenario, DatabaseStatisticCollector.QUERY_TIME);

        String yAxisTitle = "Query time" + " [" + list.getFirst().unit() + "]";
        var chart = configureCategoryChart(xAxisTitle, yAxisTitle);
        chart.getStyler().setLegendVisible(false);

        List<String> xData = new ArrayList<>();
        List<Double> yData = new ArrayList<>();
        List<Double> errorBars = new ArrayList<>();
        for (var s : list) {
            if (!s.unit().equals("error")) {
                double confidenceSize = confidenceSize(s.standardError(), 0.95);
                yData.add(s.avg);
                xData.add(s.getDatabase());
                errorBars.add(confidenceSize);

            }
        }
        chart.addSeries("geolocation", xData, yData, errorBars);

        Helper.savePdf(chart, scenario);
    }

    private CategoryChart configureCategoryChart(String xAxisTitle, String yAxisTitle) {
        CategoryChart chart = new CategoryChartBuilder().width(chartWidth).height(chartHeight)
                .xAxisTitle(xAxisTitle)
                .yAxisTitle(yAxisTitle)
                .build();

        chart.getStyler().setDefaultSeriesRenderStyle(CategorySeries.CategorySeriesRenderStyle.Bar);
        chart.getStyler().setChartTitleVisible(false);
        chart.getStyler().setAxisTitlesVisible(true);
        chart.getStyler().setChartBackgroundColor(new Color(255, 255, 255, 0));
        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
        chart.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);
        return chart;
    }

    private void createPointChart(String scenario, String xAxisTitle) {

        var dbNames = summaryStatisticsRepository.getAllDatabases();
        var list = summaryStatisticsRepository.findAllByNameAndType(scenario, DatabaseStatisticCollector.QUERY_TIME);

        XYChart chart = new XYChartBuilder().width(chartWidth).height(chartHeight)
                .xAxisTitle(xAxisTitle)
                .yAxisTitle("Log Time [" + list.getFirst().unit + "]")
                .build();

        chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        chart.getStyler().setChartTitleVisible(false);
        chart.getStyler().setAxisTitlesVisible(true);
        chart.getStyler().setChartBackgroundColor(new Color(255, 255, 255, 0));
        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
        chart.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);

        chart.getStyler().setYAxisLogarithmic(true);

        chart.getStyler().setXAxisLogarithmic(true);

        try {
            for (String dbName : dbNames) {
                List<Double> xData = new ArrayList<>();
                List<Double> yData = new ArrayList<>();
                List<Double> errorBars = new ArrayList<>();
                for (var s : list.stream().filter(e -> e.getDatabase().equals(dbName)).toList()) {
                    if (!s.unit().equals("error")) {
                        double confidenceSize = confidenceSize(s.standardError(), 0.95);
                        double param = Double.parseDouble(s.getParam());
                        yData.add(s.avg);
                        xData.add(param);
                        errorBars.add(confidenceSize);
                    }
                }
                chart.addSeries(dbName, xData, yData, errorBars);
            }
        } catch (Exception e) {
            System.out.println(scenario);
            e.printStackTrace();
        }
        Helper.savePdf(chart, scenario);
    }


    private static String formatStatistics(List<ScenarioStatisticsEntity> summaryStatistics) {
        String[] header = new String[]{"Name", "Parameter", "Type", "Avg", "StDev", "Samples"};

        String[][] content = summaryStatistics.stream().sorted(Comparator.comparing(ScenarioStatisticsEntity::name)).map(Evaluation::getContentAsString).toArray(String[][]::new);
        return FlipTable.of(header, content);
    }

    private static String formatLatexStatistics(String caption, String label, List<ScenarioStatisticsEntity> summaryStatistics) {
        String[] header = new String[]{"Name", "Parameter", "Type", "Avg", "StDev", "Samples"};

        String[][] content = summaryStatistics.stream().sorted(Comparator.comparing(ScenarioStatisticsEntity::name)).map(Evaluation::getContentAsString).toArray(String[][]::new);
        return LatexTableGenerator.generateLongTable(caption, label, header, content, "|p{3cm}*{7}{|r}|");
    }

    private static String[] getContentAsString(ScenarioStatisticsEntity s) {
        return new String[]{s.name(), s.getParam(), s.getType(), formatUnit(s.avg(), s.unit()), formatUnit(s.std(), s.unit()), s.sampleCount() + ""};
    }

    private static String formatUnit(double value, String unit) {
        if (unit.equals("bytes")) {
            return toHumanReadableWithEnum((long) value);
        }
        return df.format(value) + " " + unit;
    }

    private static String toHumanReadableWithEnum(long size) {
        List<SizeUnit> units = SizeUnit.unitsInDescending();
        if (size < 0) {
            throw new IllegalArgumentException("Invalid file size: " + size);
        }
        String result = null;
        for (SizeUnit unit : units) {
            if (size >= unit.getUnitBase()) {
                result = formatSize(size, unit.getUnitBase(), unit.name());
                break;
            }
        }
        return result == null ? formatSize(size, SizeUnit.Bytes.getUnitBase(), SizeUnit.Bytes.name()) : result;
    }


    private static String formatSize(long size, long divider, String unitName) {
        return df.format((double) size / divider) + " " + unitName;
    }

    enum SizeUnit {
        Bytes(1L),
        KB(Bytes.unitBase * 1000),
        MB(KB.unitBase * 1000),
        GB(MB.unitBase * 1000),
        TB(GB.unitBase * 1000),
        PB(TB.unitBase * 1000),
        EB(PB.unitBase * 1000);

        private final Long unitBase;

        SizeUnit(long l) {
            this.unitBase = l;
        }

        public static List<SizeUnit> unitsInDescending() {
            List<SizeUnit> list = Arrays.asList(values());
            Collections.reverse(list);
            return list;
        }

        public long getUnitBase() {
            return this.unitBase;
        }
        //getter and constructor are omitted
    }

    public static final Map<Double, Double> CONFIDENCE_LEVEL_LOOKUP = Map.of(
            0.70, 1.036,
            0.75, 1.150,
            0.80, 1.282,
            0.85, 1.440,
            0.90, 1.645,
            0.95, 1.960,
            0.98, 2.326,
            0.99, 2.576
    );

    public static SummaryStatistics calculateArithmeticStatistics(String name, String unit, List<Double> recordedResults) {
        var statistics = recordedResults.stream().mapToDouble(i -> i).summaryStatistics();

        double std = 0;

        for (double l : recordedResults) {
            std += Math.pow(l - statistics.getAverage(), 2);
        }

        std = Math.sqrt(std / (recordedResults.size() - 1));

        return new SummaryStatistics(name, unit, statistics.getAverage(), recordedResults.getFirst(), statistics.getMin(), statistics.getMax(), std, recordedResults.size());
    }

    public static int getSampleCount(double mean, double std, double confidenceLevel, double intervalSize) {
        return (int) Math.round(Math.pow((CONFIDENCE_LEVEL_LOOKUP.get(confidenceLevel) * std) / (mean * intervalSize), 2));
    }

    public static Interval computeConfidenceInterval(double avg, double std, int sampleCount, double confidenceLevel) {
        return computeConfidenceInterval(avg, std / Math.sqrt(sampleCount), confidenceLevel);
    }

    public static Interval computeConfidenceInterval(double avg, double se, double confidenceLevel) {
        var level = confidenceSize(se, confidenceLevel);
        return new Interval(avg - level, avg + level);
    }

    public static double confidenceSize(double se, double confidenceLevel) {
        return CONFIDENCE_LEVEL_LOOKUP.get(confidenceLevel) * se;
    }

}
