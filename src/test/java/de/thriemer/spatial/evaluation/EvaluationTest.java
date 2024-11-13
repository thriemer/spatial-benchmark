package de.thriemer.spatial.evaluation;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EvaluationTest {

    @Test
    void computeConfidenceInterval() {
        // from https://www.calculator.net/confidence-interval-calculator.html?size=50&mean=20.6&sd=3.2&cl=95&x=Calculate
        double mean = 20.6;
        double std = 3.2;
        double confidenceLevel = 0.95;
        int samples = 50;

        var interval = Evaluation.computeConfidenceInterval(mean, std, samples, confidenceLevel);

        assertTrue(Math.abs(interval.getMin() - 19.713) < 0.005);
        assertTrue(Math.abs(interval.getMax() - 21.487) < 0.005);
    }

}