package de.thriemer.spatial.evaluation.ahp;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MatrixTest {

    @Test
    void multiplyTest() {
        Matrix a = new Matrix(new float[][]{
                new float[]{1, 5},
                new float[]{2, 3},
                new float[]{1, 7}
        });
        Matrix b = new Matrix(new float[][]{
                new float[]{1, 2, 3, 7},
                new float[]{5, 2, 8, 1}
        });

        Matrix result = a.multiply(b);
        float[][] expected = {
                new float[]{26, 12, 43, 12},
                new float[]{17, 10, 30, 17},
                new float[]{36, 16, 59, 14}
        };
        assertArrayEquals(expected, result.m);
    }

    @Test
    void normalizeColumnsTest() {
        Matrix a = new Matrix(new float[][]{
                new float[]{1, 5, 4},
                new float[]{0.2f, 1, 1f / 3f},
                new float[]{0.25f, 3, 1f}
        });
        Matrix actual = a.normalizeColumns();
        float[][] expected = new float[][]{
                new float[]{0.689655f, 0.555556f, 0.75f},
                new float[]{0.137931f, 0.111111f, 0.0625f},
                new float[]{0.172414f, 0.333333f, 0.1875f}
        };

        fuzzyEquals(expected, actual.m);
    }

    @Test
    void rowMean(){
        Matrix a = new Matrix(new float[][]{
                new float[]{0.689655f, 0.555556f, 0.75f},
                new float[]{0.137931f, 0.111111f, 0.0625f},
                new float[]{0.172414f, 0.333333f, 0.1875f}
        });
        var actual = a.calculateRowMean();

        float[][] expected = new float[][]{
                new float[]{0.66507f},
                new float[]{0.103847f},
                new float[]{0.231082f},
        };

        fuzzyEquals(expected, actual.m);
    }

    @Test
    void columnMean(){
        Matrix a = new Matrix(new float[][]{
                new float[]{5},
                new float[]{3},
                new float[]{1}
        });
        assertEquals(3, a.columnMean(0));
    }


    private static void fuzzyEquals(float[][] expected, float[][] actual) {
        for (int i = 0; i < expected.length; i++) {
            for (int j = 0; j < expected[i].length; j++) {
                fuzzyEquals(expected[i][j], actual[i][j]);
            }
        }
    }

    private static void fuzzyEquals(float expected, float actual) {
        assertTrue(Math.abs(expected - actual) < 0.005, "Expected " + expected + " but got " + actual);
    }



}