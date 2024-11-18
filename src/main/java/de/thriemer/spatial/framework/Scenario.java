package de.thriemer.spatial.framework;

import de.thriemer.spatial.evaluation.Evaluation;
import de.thriemer.spatial.evaluation.QueryTimer;
import de.thriemer.spatial.evaluation.SummaryStatistics;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.stream.Stream;

@RequiredArgsConstructor
public abstract class Scenario<T> {

    public final String name;
    public QueryTimer timer = new QueryTimer();

    public List<T> getParams() {
        return List.of(null);
    }

    public abstract void prepare(DatabaseAbstraction database);

    public abstract void iterate(DatabaseAbstraction database, T param);

    public void cleanup(DatabaseAbstraction database) {
    }

    public SummaryStatistics getResult() {
        return Evaluation.calculateArithmeticStatistics(name, timer.getUnit(), timer.getRecordedResults());
    }

    public int getSampleCount(){
        return timer.getRecordedResults().size();
    }

}
