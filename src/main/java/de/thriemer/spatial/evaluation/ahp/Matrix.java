package de.thriemer.spatial.evaluation.ahp;

import com.jakewharton.fliptables.FlipTable;
import de.thriemer.spatial.evaluation.Evaluation;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class Matrix {

    List<String> names;
    protected float[][] m;

    public Matrix(float[][] m) {
        this.m = m;
    }

    public Matrix(List<String> names, float[][] m) {
        this.m = m;
        this.names = names;
    }

    public Matrix multiply(Matrix other) {
        float[][] result = new float[this.m.length][other.m[0].length];

        for (int row = 0; row < result.length; row++) {
            for (int col = 0; col < result[row].length; col++) {
                float cell = 0;
                for (int i = 0; i < other.m.length; i++) {
                    cell += this.m[row][i] * other.m[i][col];
                }
                result[row][col] = cell;
            }
        }
        if(this.names != null) {
            return new Matrix(new ArrayList<>(this.names), result);
        }else{
            return new Matrix(result);
        }
    }

    public Matrix normalizeColumns() {
        float[] colSum = new float[m[0].length];
        for (int row = 0; row < m.length; row++) {
            for (int col = 0; col < m[row].length; col++) {
                colSum[col] += m[row][col];
            }
        }
        float[][] result = new float[m.length][m[0].length];
        for (int row = 0; row < m.length; row++) {
            for (int col = 0; col < m[row].length; col++) {
                result[row][col] = m[row][col] / colSum[col];
            }
        }
        return new Matrix(result);
    }

    public Matrix calculateRowMean() {
        float[][] rowSum = new float[m.length][1];
        for (int row = 0; row < m.length; row++) {
            for (int col = 0; col < m[row].length; col++) {
                rowSum[row][0] += m[row][col] / (float) m[row].length;
            }
        }
        return new Matrix(rowSum);
    }

    public Matrix calculateRowTotal() {
        float[][] rowSum = new float[m.length][1];
        for (int row = 0; row < m.length; row++) {
            for (int col = 0; col < m[row].length; col++) {
                rowSum[row][0] += m[row][col];
            }
        }
        return new Matrix(rowSum);
    }

    public float columnMean(int index) {
        return columnTotal(index) / (float) m.length;
    }

    public Matrix dividedBy(Matrix other) {
        float[][] result = new float[m.length][other.m[0].length];
        for (int row = 0; row < m.length; row++) {
            for (int col = 0; col < m[row].length; col++) {
                result[row][col] = m[row][col] / other.m[row][col];
            }
        }
        return new Matrix(result);
    }

    public Pair<String[], String[][]> toTable() {
        if (names == null) {
            String[] header = getRowAsString(0).toArray(String[]::new);
            String[][] content = IntStream.range(1, m[0].length).mapToObj(r -> getRowAsString(r).toArray(String[]::new)).toArray(String[][]::new);
            return Pair.of(header, content);
        } else {
            var firstEmpty = new ArrayList<>(names);
            firstEmpty.add(0, "Compared/Base");
            String[] header = firstEmpty.toArray(String[]::new);
            String[][] content = IntStream.range(0, m[0].length).mapToObj(r -> {
                var c = getRowAsString(r);
                c.add(0, names.get(r));
                return c.toArray(String[]::new);
            }).toArray(String[][]::new);
            return Pair.of(header, content);
        }
    }


    private List<String> getRowAsString(int row) {
        return new ArrayList<>(IntStream.range(0, m[0].length).mapToObj(i -> Evaluation.df.format(m[row][i])).toList());
    }

    public float columnTotal(int index) {
        float sum = 0;
        for (int row = 0; row < m.length; row++) {
            sum += m[row][index];
        }
        return sum;

    }
}
