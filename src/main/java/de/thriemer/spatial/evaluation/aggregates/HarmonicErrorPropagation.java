package de.thriemer.spatial.evaluation.aggregates;

import org.h2.api.Aggregate;
import org.h2.value.ValueDouble;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class HarmonicErrorPropagation implements Aggregate {

    double summedReciprocalValues;

    List<Double> rates = new ArrayList<>();
    List<Double> errors = new ArrayList<>();

    @Override
    public int getInternalType(int[] ints) throws SQLException {
        return ValueDouble.DOUBLE;
    }

    @Override
    public void add(Object o) throws SQLException {
        Object[] c = (Object[]) o;
        double rate = (Double) c[0];
        double error = (Double) c[1];
        rates.add(rate);
        errors.add(error);
        summedReciprocalValues += 1.0 / rate;
    }

    @Override
    public Object getResult() throws SQLException {
        double variance = 0;
        double hWithRespectToSummedReciprocalValues = rates.size() / Math.pow(summedReciprocalValues, 2.0);
        for (int i = 0; i < rates.size(); i++) {
            double partialDerivative = hWithRespectToSummedReciprocalValues / Math.pow(rates.get(i), 2.0);
            variance += Math.pow(partialDerivative, 2.0) * Math.pow(errors.get(i), 2.0);
        }
        return Math.sqrt(variance);
    }
}
