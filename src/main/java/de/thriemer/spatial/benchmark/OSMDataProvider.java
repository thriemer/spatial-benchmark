package de.thriemer.spatial.benchmark;

import crosby.binary.osmosis.OsmosisReader;
import de.thriemer.spatial.framework.DatabaseAbstraction;
import de.thriemer.spatial.framework.DatabaseStatisticCollector;
import de.thriemer.spatial.framework.Helper;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.collections4.ListUtils;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class OSMDataProvider implements Sink {
    final int totalAmount = 87_882_484;

    private static final int BATCH_SIZE = 10_000;

    @Getter
    List<DataPoint> parsed = new ArrayList<>(totalAmount);
    Random r = new Random();

    long pointsImported;

    public OSMDataProvider() {
        log.info("Loading OSM file");
        var reader = new OsmosisReader(new File(DatabaseStatisticCollector.fastLane ? "liechtenstein-latest.osm.pbf" : "turkey-latest.osm.pbf"));
        reader.setSink(this);
        reader.run();
    }

    @Override
    public void process(EntityContainer entityContainer) {
        if (entityContainer instanceof NodeContainer nodeContainer) {
            pointsImported++;
            var entity = nodeContainer.getEntity();
            String tags = entity.getTags().stream().map(Tag::toString).collect(Collectors.joining(" "))
                    .replaceAll("\\|+", "-")
                    .replaceAll(",+", " ")
                    .replaceAll("\"+", "'")
                    .replaceAll("\n+", "");
            parsed.add(new DataPoint(
                    entity.getLongitude(), entity.getLatitude(), (int) entity.getId(),
                    r.nextFloat(), tags
            ));
        }
    }

    public int getDataPointsImported() {
        return parsed.size();
    }


    @SneakyThrows
    public void loadDataIntoDatabase(DatabaseAbstraction db, String tableName) {
        log.info("Starting to load OSM Point data. This will take a long while.");

        List<List<DataPoint>> batches = new ArrayList<>(ListUtils.partition(parsed, BATCH_SIZE));
        Collections.shuffle(batches);
        ProgressBar progressBar = new ProgressBar("Load into "+db.getName(), pointsImported);

        batches.forEach(batch -> {
            db.persistMultiplePoints(tableName, batch);
            progressBar.stepBy(BATCH_SIZE);
        });
    }

    @Override
    public void initialize(Map<String, Object> map) {

    }

    @Override
    @SneakyThrows
    public void complete() {
        log.info("Total points loaded to memory: {}", pointsImported);
    }

    @Override
    public void close() {

    }

}