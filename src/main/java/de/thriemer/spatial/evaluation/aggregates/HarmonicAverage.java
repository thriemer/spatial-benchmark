package de.thriemer.spatial.evaluation.aggregates;

import org.h2.api.Aggregate;
import org.h2.value.ValueDouble;

import java.sql.SQLException;

public class HarmonicAverage implements Aggregate {

    double sum = 0.0;
    double count = 0.0;

    @Override
    public int getInternalType(int[] ints) throws SQLException {
        return ValueDouble.DOUBLE;
    }

    @Override
    public void add(Object o) throws SQLException {
        sum += 1.0 / (Double) o;
        count++;
    }

    @Override
    public Object getResult() throws SQLException {
        return count / sum;
    }
}
