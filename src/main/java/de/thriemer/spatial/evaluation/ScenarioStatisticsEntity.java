package de.thriemer.spatial.evaluation;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Entity
@Table
@Setter
@Getter
@NoArgsConstructor
public class ScenarioStatisticsEntity extends SummaryStatistics implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;

    private String param;

    private String database;
    private String type;

    public ScenarioStatisticsEntity(String name, String unit, double avg, double first, double min, double max, double std, int sampleCount, String param, String database, String type) {
        super(name, unit, avg, first, min, max, std, sampleCount);
        this.param = param;
        this.database = database;
        this.type = type;
    }

    public static ScenarioStatisticsEntity from(String db, String param, String type, SummaryStatistics s) {
        return new ScenarioStatisticsEntity(s.name, s.unit, s.avg, s.first, s.min, s.max, s.std, s.sampleCount, param, db, type);
    }

    public boolean isValid() {
        return !unit.equals(ERROR_IN_RUN) && sampleCount > 1 && !Double.isNaN(std);
    }

    public ScenarioStatisticsEntity clone() {
        return new ScenarioStatisticsEntity(this.name, this.unit, this.avg, this.first, this.min, this.max, this.std, this.sampleCount, this.param, this.database, this.type);
    }

}
