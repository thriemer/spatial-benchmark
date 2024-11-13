package de.thriemer.spatial.evaluation;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ComparisonRepository extends CrudRepository<ComparisonEntity, Long> {

    ComparisonEntity findByBaseDatabaseAndAndComparedDatabaseAndScenarioAndParam(String baseDatabase, String comparedDatabase, String scenario, String param);

    List<ComparisonEntity> findByBaseDatabaseAndScenario(String baseDatabase, String scenario);

    @Query("""
             select
                database as database,
                GEOMETRIC_AVG(speedUp) as speedUp,
                GEOMETRIC_ERR_PROP(speedUp, stDev) as stDev,
                GEOMETRIC_ERR_PROP(speedUp, standardError) as standardError
             from
                (
                   select
                      c.comparedDatabase as database,
                      GEOMETRIC_AVG(c.speedUp) as speedUp,
                      GEOMETRIC_ERR_PROP(c.speedUp, c.speedUpStd) as stDev,
                      GEOMETRIC_ERR_PROP(c.speedUp, c.speedUpSE) as standardError
                   from
                      ComparisonEntity c
                   where
                      c.baseDatabase = :baseline
                      and c.type = :metricType
                   group by
                      c.comparedDatabase,
                      c.scenario
                )
             group by
                database
             order by
                speedUp DESC
            """)
    List<CombinedComparison> combineComparison(String baseline, String metricType);

    @Query("""
                   select
                      GEOMETRIC_AVG(c.speedUp)
                   from
                      ComparisonEntity c
                   where
                      c.baseDatabase = :baseline
                      and c.comparedDatabase = :other
                      and c.scenario = :scenario
                      and c.type = 'Query Time'
            """)
    double calculateSpeedup(String baseline, String other, String scenario);

    // formatted using: https://www.freeformatter.com/sql-formatter.html
    interface CombinedComparison {
        String getDatabase();

        double getSpeedUp();

        double getStDev();

        double getStandardError();
    }

    @Query("""
                     select
                       database as database,
                       GEOMETRIC_AVG(speedUp) as speedUp,
                       GEOMETRIC_ERR_PROP(speedUp, stDev) as stDev,
                       GEOMETRIC_ERR_PROP(speedUp, standardError) as standardError,
                       metric as metric
                    from
                       (
                          select
                             c.comparedDatabase as database,
                             GEOMETRIC_AVG(c.speedUp * m.speedUp) as speedUp,
                             GEOMETRIC_ERR_PROP(c.speedUp * m.speedUp, c.speedUp * m.speedUp * SQRT(POWER(c.speedUpStd/c.speedUp, 2) + POWER(c.speedUpStd/c.speedUp, 2))) as stDev,
                             GEOMETRIC_ERR_PROP(c.speedUp * m.speedUp, c.speedUp * m.speedUp * SQRT(POWER(c.speedUpSE/c.speedUp, 2) + POWER(c.speedUpSE/c.speedUp, 2))) as standardError,
                             m.type as metric
                          from
                             ComparisonEntity c
                             join ComparisonEntity m ON c.scenario = m.scenario and m.baseDatabase = c.baseDatabase and m.comparedDatabase = c.comparedDatabase and m.param = c.param
                          where
                             c.baseDatabase = :baseline and c.type = 'Query Time' and m.type in ('CPU usage', 'Memory usage', 'Disk Usage')
                          group by
                             c.comparedDatabase,
                             c.scenario,
                             m.type
                       )
                    group by database, metric
            """)
    List<EfficiencyComparison> computeEfficiency(String baseline);


    interface EfficiencyComparison {
        String getDatabase();

        String getMetric();

        double getSpeedUp();

        double getStDev();

        double getStandardError();
    }

}