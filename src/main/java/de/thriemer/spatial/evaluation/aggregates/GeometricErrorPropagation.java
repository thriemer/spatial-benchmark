package de.thriemer.spatial.evaluation.aggregates;

import org.h2.api.Aggregate;
import org.h2.value.ValueDouble;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GeometricErrorPropagation implements Aggregate {

    double product = 1.0;

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
        product *= rate;
    }

    @Override
    public Object getResult() throws SQLException {
        double variance = 0;
        double productRoot = Math.pow(product, 1d/rates.size());
        for (int i = 0; i < rates.size(); i++) {
            double partialDerivative = productRoot / (rates.size() * rates.get(i));
            variance += Math.pow(partialDerivative*errors.get(i), 2.0);
        }
        return Math.sqrt(variance);
    }
}
