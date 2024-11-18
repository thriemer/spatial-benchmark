package de.thriemer.spatial.evaluation;


import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public interface SummaryStatisticsRepository extends CrudRepository<ScenarioStatisticsEntity, Long> {

    List<ScenarioStatisticsEntity> getAllByDatabase(String database);

    @Query("SELECT DISTINCT database FROM ScenarioStatisticsEntity")
    List<String> getAllDatabases();

    @Query("SELECT DISTINCT name FROM ScenarioStatisticsEntity")
    List<String> getScenarioNames();

    @Query("SELECT COUNT(s)>1 from ScenarioStatisticsEntity s where s.database=:database and s.name=:scenario and s.param=:param")
    boolean exists(String database, String scenario, String param);

    default List<ScenarioStatisticsEntity> getNullScenarioStatistics() {
        List<ScenarioStatisticsEntity> result = new ArrayList<>();
        this.findAll().forEach(s -> {
            if (Double.isNaN(s.std())) {
                result.add(s);
            }
        });
        return result;
    }

    List<ScenarioStatisticsEntity> findAllByNameAndType(String name, String type);

    @Transactional
    void deleteAllByDatabase(String database);

    @Modifying
    @Transactional
    @Query("delete from ScenarioStatisticsEntity s where s.database=:database and s.name like concat(:scenario,'%') and s.param=:param")
    int delete(String database, String scenario, String param);

    @Modifying
    @Transactional
    @Query("delete from ScenarioStatisticsEntity s where s.name=:name")
    void deleteByScenarioName(String name);
}
