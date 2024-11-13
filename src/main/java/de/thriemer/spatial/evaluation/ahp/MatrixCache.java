package de.thriemer.spatial.evaluation.ahp;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MatrixCache {
    FileLoader fl = new FileLoader();
    Map<String, Matrix> criteriaMatrixCache = new HashMap<>();
    Map<String, Matrix> comparisonMatrixCache = new HashMap<>();

    public List<String> getChildren(String parent){
        Matrix p = criteriaMatrixCache.get(parent);
        return p.names.stream().filter(criteriaMatrixCache.keySet()::contains).toList();
    }
    public boolean containsCriterion(String parent) {
        return criteriaMatrixCache.containsKey(parent);
    }


    public Matrix getComparisonMatrix(String matrixName) {
        if (!comparisonMatrixCache.containsKey(matrixName)) {
            throw new RuntimeException("Matrix not found: " + matrixName);
        }
        return comparisonMatrixCache.get(matrixName);
    }

    public Matrix getCriterionMatrix(String criteriaName) {
        return criteriaMatrixCache.get(criteriaName);
    }

    public void loadComparisonsFromCsv(String path) {
        loadCsvToCache(path, comparisonMatrixCache);
    }
    public void loadCriteriaFromCsv(String path) {
        loadCsvToCache(path, criteriaMatrixCache);
    }

    public void putComparisonsMatrix(String criteria, Matrix matrix) {
        comparisonMatrixCache.put(criteria, matrix);
    }

    private void loadCsvToCache(String path,Map<String, Matrix> cacheMatrix) {
        var map = fl.loadAllFiles(path);
        for (var entry : map.entrySet()) {
            String criterion = entry.getKey().replace("_", " ").replace(".csv", "");
            cacheMatrix.put(criterion, csvToMatrix(entry.getValue()));
        }
    }

    Matrix csvToMatrix(String content) {
        Map<String, Float> comparisons = new HashMap<>();
        String[] rows = content.split("\n");
        String[] header = rows[0].split(",");
        List<String> comparedList = Arrays.stream(header).skip(1).toList();
        fillComparisonsMap(comparisons, rows, header);
        return matrixFromMap(comparisons, comparedList);
    }


    private void fillComparisonsMap(Map<String, Float> comparisons, String[] rows, String[] header) {
        for (int i = 1; i < rows.length; i++) {
            String[] row = rows[i].split(",");
            for (int j = 1; j < row.length; j++) {
                if (!row[j].isBlank()) {
                    if (row[j].contains("\"")) {
                        continue;
                    }
                    String base = header[j];
                    String compared = row[0];
                    float comparison = Float.parseFloat(row[j]);
                    comparisons.put(base + compared, comparison);
                }
            }
        }
    }

    public Matrix matrixFromMap(Map<String, Float> comparisons, List<String> names) {

        float[][] result = new float[names.size()][names.size()];
        int row = 0;
        for (String compared : names) {
            int col = 0;
            for (String base : names) {
                if (base.equals(compared)) {
                    result[row][col] = 1.0f;
                } else {
                    result[row][col] = extractWeight(comparisons, base, compared);
                }
                col++;
            }
            row++;
        }
        return new Matrix(names, result);
    }

    float extractWeight(Map<String, Float> comparisons, String base, String compared) {

        if (comparisons.containsKey(base + compared)) {
            return comparisons.get(base + compared);
        }
        if (comparisons.containsKey(compared + base)) {
            return 1f / comparisons.get(compared + base);
        }

        throw new RuntimeException("Comparison between " + base + " and " + compared + " is not found");
    }


}
