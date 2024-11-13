package de.thriemer.spatial.framework;

import de.thriemer.spatial.evaluation.Evaluation;
import de.thriemer.spatial.evaluation.QueryTimer;
import de.thriemer.spatial.evaluation.SummaryStatistics;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class Scenario {

    public final String name;
    public QueryTimer timer = new QueryTimer();

    public Object[] getParams() {
        return new Object[]{null};
    }

    public abstract void prepare(DatabaseAbstraction database);

    public abstract void iterate(DatabaseAbstraction database, Object param);

    public void cleanup(DatabaseAbstraction database) {
    }

    public SummaryStatistics getResult() {
        return Evaluation.calculateArithmeticStatistics(name, timer.getUnit(), timer.getRecordedResults());
    }

    public int getSampleCount(){
        return timer.getRecordedResults().size();
    }

}
