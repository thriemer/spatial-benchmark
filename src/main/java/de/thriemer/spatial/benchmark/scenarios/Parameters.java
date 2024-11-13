package de.thriemer.spatial.benchmark.scenarios;

import de.thriemer.spatial.benchmark.OSMDataProvider;
import de.thriemer.spatial.framework.DataType;
import de.thriemer.spatial.framework.DatabaseAbstraction;
import de.thriemer.spatial.framework.DatabaseStatisticCollector;
import io.vavr.Tuple2;

public class Parameters {

    public static final String OSM_DATA_TABLE = "osm_point_data";

    static OSMDataProvider osmDataProvider;

    public static void createOSMDataTable(DatabaseAbstraction db, String tableName) {
        if (!db.tableExists(tableName)) {
            if (osmDataProvider == null) {
                osmDataProvider = new OSMDataProvider();
            }
            db.createTable(tableName, new Tuple2<>("pos", DataType.GEO_POINT), new Tuple2<>("point_id", DataType.INT), new Tuple2<>("some_float", DataType.DOUBLE), new Tuple2<>("tags", DataType.STRING));
            osmDataProvider.loadDataIntoDatabase(db, tableName);
            System.out.println("Loaded " + db.count(tableName) + "/" + osmDataProvider.getDataPointsImported());
            db.createIndex(tableName, "pos");
        }
    }

    public static long getNumberOfPoints() {
    /*    if (osmDataProvider == null) {
            osmDataProvider = new OSMDataProvider();
        }
       return osmDataProvider.getDataPointsImported();
     */
        // the code above is the correct on, the hard coded values are values for Turkey and Lichtenstein
        return DatabaseStatisticCollector.fastLane ? 318_592 : 87_882_484;
    }

}
