package de.thriemer.spatial.evaluation.ahp;

import de.thriemer.spatial.evaluation.ComparisonRepository;
import de.thriemer.spatial.evaluation.Evaluation;
import de.thriemer.spatial.evaluation.LatexTableGenerator;
import de.thriemer.spatial.framework.DatabaseStatisticCollector;
import de.thriemer.spatial.framework.Helper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class AhpSolver {

    final ComparisonRepository databaseComparisonRepository;
    boolean useVanillaAdx = false;
    MatrixCache matrixCache = new MatrixCache();

    private static final Map<Integer, Float> CONSISTENCY_MAP = Map.of(
            1, 0f,
            2, 0f,
            3, 0.58f,
            4, 0.9f,
            5, 1.12f,
            6, 1.24f,
            7, 1.32f,
            8, 1.41f,
            9, 1.45f
    );

    public SimpleTree solve() {
        matrixCache.loadCriteriaFromCsv("criteria");
        matrixCache.loadComparisonsFromCsv("comparisons");
        loadBenchmarkComparisons();
        loadEfficiencyComparisons();
        var tree = buildTree();
        System.out.println(tree.printLatex());
        System.out.println(tree.propagate(this::weightLookup));
        Helper.saveFile(constructCriteriaMatrices(),"ahp_matrix");

        for(var e:matrixCache.comparisonMatrixCache.entrySet()){
            String fileName = e.getKey().replace(" ","_");
            Helper.saveFile(matrixToTable(e),fileName,"generated/");
        }

        return tree;
    }

    void loadBenchmarkComparisons() {
        Map<String, String> map = Map.of(
                "Creation", "Pagination Scenario First Run",
                "Page Read", "Pagination Scenario Fetch Pages",
                "Random Access", "Pagination Scenario Random Access",
                "Additional Filter", "Combination of spatial with non spatial filter",
                "Insert Speed", "Batch Insert Point",
                "Multiple Geolocation Filters", "Multiple geolocation filters",
                "Polygon Complexity", "Polygon filter complexity"
        );
        var databaseNames = List.of("Aerospike", "Azure Data Explorer", "PostGIS", "OpenSearch");
        for (var entry : map.entrySet()) {
            Map<String, Float> speedUps = new HashMap<>();
            for (String databaseName : databaseNames) {
                for (String other : databaseNames) {
                    float weight = (float) databaseComparisonRepository.calculateSpeedup(transform(databaseName), transform(other), entry.getValue());
                    speedUps.put(databaseName + other, Math.min(weight, 9));
                }
            }
            matrixCache.putComparisonsMatrix(entry.getKey(), matrixCache.matrixFromMap(speedUps, databaseNames));
        }
    }

    void loadEfficiencyComparisons() {
        var databaseNames = new ArrayList<>(List.of("Aerospike", "Azure Data Explorer", "PostGIS", "OpenSearch"));
        var map = Map.of("CPU", DatabaseStatisticCollector.CPU_USAGE,
                "RAM", DatabaseStatisticCollector.MEMORY_USAGE,
                "Disk", DatabaseStatisticCollector.DISK_USAGE
        );

        for (var entry : map.entrySet()) {
            Map<String, Float> speedUps = new HashMap<>();
            for (String databaseName : databaseNames) {
                var comparisons = databaseComparisonRepository.combineComparison(transform(databaseName), entry.getValue());
                for (var comparison : comparisons) {
                    float weight = Math.min((float) comparison.getSpeedUp(), 9);
                    speedUps.put(databaseName + comparison.getDatabase(), Math.min(weight, 9));
                }
            }
            matrixCache.putComparisonsMatrix(entry.getKey(), matrixCache.matrixFromMap(speedUps, databaseNames));
        }
    }

    private String transform(String dbName){
        if(useVanillaAdx&&dbName.equals("Azure Data Explorer")){
            return "ADX Vanilla";
        }
        return dbName;
    }

    private Map<String, Float> weightLookup(String criterion) {
        Matrix m = matrixCache.getComparisonMatrix(criterion);
        var consistency = calculateConsistency(m);
        if (consistency > 0.1) {
            log.warn("Consistency of {} is too big for options in criterion {}", consistency, criterion);
        }
        return calculateWeight(m);
    }

    Map<String, Float> calculateWeight(Matrix m) {
        for (int i = 0; i < 5; i++) {
            m = m.multiply(m);
        }
        Matrix rowTotal = m.calculateRowTotal();
        float total = rowTotal.columnTotal(0);
        Matrix finalM = m;
        return m.names.stream().collect(Collectors.toMap(s -> s, s -> rowTotal.m[finalM.names.indexOf(s)][0] / total));
    }

    SimpleTree buildTree() {
        var t = new SimpleTree();
        t.criteriaName = "Evaluation";
        t.weight = 1.0f;
        t.children = findChildren("Comparison");
        return t;
    }

    private List<SimpleTree> findChildren(String parent) {
        if (!matrixCache.containsCriterion(parent)) {
            return List.of();
        }
        Matrix criteria = matrixCache.getCriterionMatrix(parent);
        var consistency = calculateConsistency(criteria);
        if (consistency > 0.1) {
            log.warn("Consistency of {} is too big {}", parent, consistency);
        }
        Map<String, Float> weight = calculateWeight(criteria);

        return weight.entrySet().stream().map(e -> {
            var t = new SimpleTree();
            t.criteriaName = e.getKey();
            t.weight = e.getValue();
            t.children = findChildren(e.getKey());
            return t;
        }).toList();
    }


    float calculateConsistency(Matrix m) {
        // there is no transitivity if there are less than three elements
        if (m.m.length < 3) {
            return 0f;
        }
        var priorities = m.normalizeColumns().calculateRowMean();
        var result = m.multiply(priorities);
        var eigenVector = result.dividedBy(priorities);
        float colMean = eigenVector.columnMean(0);
        float consistencyIndex = (colMean - m.m.length) / (m.m.length - 1);
        return consistencyIndex / CONSISTENCY_MAP.get(m.m.length);
    }

    public String matrixWithWeightToLatexTable(Matrix m) {
        Map<String, Float> weight = calculateWeight(m);
        var t = m.toTable();
        List<String> header = new ArrayList<>(Arrays.asList(t.getKey()));
        header.add("Weight");

        List<List<String>> rows = Arrays.stream(t.getValue()).map(a -> (List<String>) new ArrayList<>(Arrays.asList(a))).toList();
        for (var row : rows) {
            var w = weight.get(row.getFirst());
            row.add(Evaluation.df.format(w));
        }

        String[] headerArray = header.toArray(String[]::new);
        String[][] contentArray = rows.stream().map(l -> l.toArray(String[]::new)).toArray(String[][]::new);
        return LatexTableGenerator.generateTable(headerArray, contentArray, "|l|*{" + (header.size() - 2) + "}{c|}|r|",true);
    }


    private String matrixToTable(Map.Entry<String, Matrix> e){
        String criterion = e.getKey();
        Matrix m = e.getValue();
        float consistency = calculateConsistency(m);
        String sb = "\\begin{table}[!htbp]\n\\centering\n" +
                matrixWithWeightToLatexTable(m) +
                "\\caption{" + criterion + ", consistency: " + Evaluation.df.format(consistency) + "}\n" +
                "\\end{table}\n\n";
        return sb;
    }

    private String constructCriteriaMatrices() {
        StringBuilder sb = new StringBuilder();
        for (var e : matrixCache.criteriaMatrixCache.entrySet()) {
            sb.append(matrixToTable(e));
        }
        return sb.toString();
    }

}
