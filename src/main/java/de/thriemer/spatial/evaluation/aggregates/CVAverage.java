package de.thriemer.spatial.evaluation.aggregates;

import org.h2.api.Aggregate;
import org.h2.value.ValueDouble;

import java.sql.SQLException;

public class CVAverage implements Aggregate {

    double sum = 0.0;

    @Override
    public int getInternalType(int[] ints) throws SQLException {
        return ValueDouble.DOUBLE;
    }

    @Override
    public void add(Object o) throws SQLException {
        sum += Math.pow((Double) o, 2.0);
    }

    @Override
    public Object getResult() throws SQLException {
        return Math.sqrt(sum);
    }
}
