import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 *
 * ./start-cli.\tab
 * create function coalesce as 'org.apache.iotdb.library.util.coalesce';
 */
public class IoTDBV extends benchmarks {
    List<String> labelGarbage = new ArrayList<>();
    int scale_count = 0;
    public Constants constants;
    public Session session;
    FileWriter wlog;
    double param;
    FileWriter wres;
    public Map<String, Tablet> repair = new HashMap<>();
    boolean gen_update_only = false;

    @Override
    public void create() throws SQLException, IoTDBConnectionException, StatementExecutionException {
        session.createDatabase("root."+constants.dataset);
        for(Schema schema: constants.descriptor.mainName) {
//            session.setStorageGroup(schema.storageGroup);
            session.createTimeseries(schema.main, TSDataType.INT32, TSEncoding.TS_2DIFF, CompressionType.UNCOMPRESSED);
            for(String s: constants.descriptor.branches.get(schema.main)) {
                session.createTimeseries(s, TSDataType.INT32, TSEncoding.TS_2DIFF, CompressionType.UNCOMPRESSED);
            }
        }
    }

    @Override
    public void clean() throws SQLException, IoTDBConnectionException, StatementExecutionException {
//        session.deleteDatabase("root."+constants.dataset);
        for(Schema schema: constants.descriptor.mainName) {
            try {
                session.deleteTimeseries(constants.IoTDB_DATABASE + "." + schema.key + ".main");
            } catch (Exception e) {}
            try {
                session.deleteTimeseries(constants.IoTDB_DATABASE + "." + schema.key + ".b0");
            } catch (Exception e) {}
        }
//        session.close();
    }

    @Override
    public void init(int num_branch, DataSet dataset, String ev) throws SQLException, IoTDBConnectionException {
        constants = new Constants(num_branch, dataset, "IoTDB");
        session = new Session.Builder().host("127.0.0.1").port(6667).build();
        session.open();
    }

    @Override
    public void insert() throws SQLException {
        // clean.
    }

    @Override
    public void insertCSV() throws SQLException, IOException {
        //clean.
    }

    @Override
    public void insertDataPrepare(String path, int length, double upd, double delay, double dup, int verbose) throws IOException, SQLException, IoTDBConnectionException, StatementExecutionException {
        Random rd = new Random();
        Deque<Pair<String, Pair<Long, Integer>>> delayed = new LinkedList<>();
        Scanner sc = new Scanner(new File(path));
        Map<String, Range> vr = new HashMap<>(); // guarantee unique colname
        sc.nextLine(); // skip header.
        Map<String, List<Integer>> data = new HashMap<>();
        Map<String, List<Long>> timestamp = new HashMap<>();
        for(int i=0;i< constants.descriptor.mainName.size();i++) {
            Schema schema = constants.descriptor.mainName.get(i);
            data.put(schema.main, new ArrayList<>());
            for(String branch: constants.descriptor.branches.get(schema.main)) {
                data.put(branch, new ArrayList<>());
                timestamp.put(branch, new ArrayList<>());
                data.put(branch, new ArrayList<>());
            }
            vr.put(schema.main, new Range(Double.MAX_VALUE, Double.MIN_VALUE));
            timestamp.put(schema.main, new ArrayList<>());
            data.put(schema.main, new ArrayList<>());
        }
        int pos = 1;
        while (sc.hasNext() && pos < length) {
            String line = sc.nextLine();
            String[] s = line.split(",");
            for(int i=0;i< constants.descriptor.mainName.size();i++) {
                Schema schema = constants.descriptor.mainName.get(i);
                for(String attr: schema.attributes.keySet()) {
                    int toInsert;
                    String reads  = "";
                    if(rd.nextDouble() < param) continue;
                    if(s.length > schema.attributes.get(attr)) reads = s[schema.attributes.get(attr)];
                    if(reads.isEmpty()) reads = "0";
                    if(schema.types.get(attr) == Type.INT) {
                        int x = Integer.parseInt(reads);
                        vr.get(schema.main).update(x);
                        toInsert = x*10000;
                    } else {
                        float x = Float.parseFloat(reads);
                        vr.get(schema.main).update(x);
                        toInsert = (int) (x*10000);
                    }
                    long timex = constants.descriptor.gen.get(i)*pos;
                    if((verbose & 1) == 1) {
                        if(rd.nextDouble() < delay) {
                            delayed.add(new Pair<>(schema.main, new Pair<>(timex, toInsert)));
                        } else {
                            data.get(schema.main).add(toInsert);
                            timestamp.get(schema.main).add(timex);
                        }
                    } //else System.out.println(schema.main + " " + toInsert);
                    if(rd.nextDouble() < upd) {
                        String updateTablex = SchemaDescriptor.random_select_update_table(constants.descriptor.branches.get(schema.main));
                        data.get(updateTablex).add(toInsert + 10);
                        timestamp.get(updateTablex).add(timex);
                    }
                    if(rd.nextDouble() < dup) {
                        if((verbose & 1) == 1) {
                            data.get(schema.main).add(toInsert);
                            timestamp.get(schema.main).add(timex);
                        } //else System.out.println(schema.main + " " + toInsert);
                    }
                }
            }
            pos++;

        }
        while(!delayed.isEmpty()) {
            Pair<String, Pair<Long, Integer>> exec = delayed.pollFirst();
            if((verbose & 1) == 1) {
                data.get(exec.left).add(exec.right.right);
                timestamp.get(exec.left).add(exec.right.left);
            }
            //else System.out.println(exec);
        }
        constants.valueRange = vr;
        for(int i=0;i< constants.descriptor.mainName.size();i++) {
            Schema schema = constants.descriptor.mainName.get(i);
            constants.timeRange.put(schema.main, Range.createRange(0, constants.descriptor.gen.get(i) * pos));
            if (!gen_update_only) {
                session.insertTablet(
                        construct(schema, "main",
                                timestamp.get(schema.main), data.get(schema.main), false));
            }
            for (String branchx : schema.measurements) {
                if (!gen_update_only) {
                    session.insertTablet(
                            construct(schema, branchx,
                                    timestamp.get(schema.deviceId + "." + branchx),
                                    data.get(schema.deviceId + "." + branchx), true), false);
                } else {
                    construct(schema, branchx,
                            timestamp.get(schema.deviceId + "." + branchx),
                            data.get(schema.deviceId + "." + branchx), true);
                }
            }
        }
    }

    Tablet construct(Schema schema, String measurement, List<Long> time, List<Integer> values, boolean save) {
        //System.out.println(measurement);
        List<MeasurementSchema> iotx = List.of(new MeasurementSchema(measurement, TSDataType.INT32));
        Tablet mainTablet = new Tablet(schema.deviceId, iotx, time.size());
        mainTablet.initBitMaps();
        for(int j = 0;j<values.size();j++) {
            mainTablet.addTimestamp(j, time.get(j));
            mainTablet.addValue(measurement, j, values.get(j));
        }
        mainTablet.rowSize = values.size();
        if(save) repair.put(schema.deviceId + "." + measurement, mainTablet);
        //System.out.println(mainTablet.rowSize);
        return mainTablet;
    }

    @Override
    public void insertDataPrepare_gene(int length, double upd, double delay, double dup, int verbose) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        Random rd = new Random();
        Deque<Pair<String, Pair<Long, Integer>>> delayed = new LinkedList<>();
        Map<String, Range> vr = new HashMap<>(); // guarantee unique colname
        Map<String, List<Integer>> data = new HashMap<>();
        Map<String, List<Long>> timestamp = new HashMap<>();
        for(int i=0;i< constants.descriptor.mainName.size();i++) {
            Schema schema = constants.descriptor.mainName.get(i);
            data.put(schema.main, new ArrayList<>());
            for(String branch: constants.descriptor.branches.get(schema.main)) {
                data.put(branch, new ArrayList<>());
                timestamp.put(branch, new ArrayList<>());
                data.put(branch, new ArrayList<>());
            }
            vr.put(schema.main, new Range(Double.MAX_VALUE, Double.MIN_VALUE));
            timestamp.put(schema.main, new ArrayList<>());
            data.put(schema.main, new ArrayList<>());
        }
        int pos = 0;
        while (pos < length) {
            for(int i=0;i< constants.descriptor.mainName.size();i++) {
                Schema schema = constants.descriptor.mainName.get(i);
                for(String attr: schema.attributes.keySet()) {
                    int toInsert;
                    if(schema.types.get(attr) == Type.INT) {
                        int x = rd.nextInt();
                        vr.get(schema.main).update(x);
                        toInsert = x;
                    } else {
                        float x = rd.nextFloat();
                        vr.get(schema.main).update(x);
                        toInsert = (int) (x*10000);
                    }
                    long timex = constants.descriptor.gen.get(i)* (pos + scale_count);
                    if((verbose & 1) == 1) {
                        if(rd.nextDouble() < delay) {
                            delayed.add(new Pair<>(schema.main, new Pair<>(timex, toInsert)));
                        } else {
                            data.get(schema.main).add(toInsert);
                            timestamp.get(schema.main).add(timex);
                        }
                    } //else System.out.println(schema.main + " " + toInsert);
                    if(rd.nextDouble() < upd) {
                        String updateTablex = SchemaDescriptor.random_select_update_table(constants.descriptor.branches.get(schema.main));
                        data.get(updateTablex).add(toInsert + 10);
                        timestamp.get(updateTablex).add(timex);
                    }
                    if(rd.nextDouble() < dup) {
                        if((verbose & 1) == 1) {
                            data.get(schema.main).add(toInsert);
                            timestamp.get(schema.main).add(timex);
                        } //else System.out.println(schema.main + " " + toInsert);
                    }
                }
            }
            pos++;
        }
        scale_count += pos;
        while(!delayed.isEmpty()) {
            Pair<String, Pair<Long, Integer>> exec = delayed.pollFirst();
            if((verbose & 1) == 1) {
                data.get(exec.left).add(exec.right.right);
                timestamp.get(exec.left).add(exec.right.left);
            }
            //else System.out.println(exec);
        }
        constants.valueRange = vr;
        for(int i=0;i< constants.descriptor.mainName.size();i++) {
            Schema schema = constants.descriptor.mainName.get(i);
            constants.timeRange.put(schema.main, Range.createRange(0, constants.descriptor.gen.get(i) * pos));
            if(!gen_update_only) {
                session.insertTablet(
                        construct(schema, "main",
                                timestamp.get(schema.main), data.get(schema.main), false));
            }
            for(String branchx: schema.measurements) {
                if(!gen_update_only) {
                    session.insertTablet(
                            construct(schema, branchx,
                                    timestamp.get(schema.deviceId + "." + branchx),
                                    data.get(schema.deviceId + "." + branchx), true), false);
                } else {
                    construct(schema, branchx,
                            timestamp.get(schema.deviceId + "." + branchx),
                            data.get(schema.deviceId + "." + branchx), true);
                }
            }
        }
    }

    @Override
    public void update(List<Schema> main) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        for(Schema schema: main) {
            this.update(schema);
        }
    }

    @Override
    public void update(Schema main) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        String sql = String.format("select coalesce(main, b0) from %s", main.deviceId);
        session.executeQueryStatement(sql);
    }

    @Override
    public void align(Schema tableMain1, Schema tableMain2) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        String sql = String.format
                ("select coalesce(%s.main, %s.b0), coalesce(%s.main, %s.b0) from %s where time < %s",
                        tableMain1.key, tableMain1.key,
                        tableMain2.key, tableMain2.key, constants.IoTDB_DATABASE,
                        Double.min(constants.timeRange.get(tableMain1.main).right, constants.timeRange.get(tableMain2.main).right));
        //System.out.println("V");
        session.executeQueryStatement(sql);
    }

    @Override
    public void align(List<Schema> tableMain) throws Exception {
        for(int i=0;i<tableMain.size()-1;i++) {
            this.align(tableMain.get(i), tableMain.get(i+1));
        }
    }

    @Override
    public void rangeFilter(Schema tableMain, double selectivity) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        Range timeRange = Range.randomBySelectivity(constants.timeRange.get(tableMain.main), selectivity);
        String sql = String.format
                ("select coalesce(%s.main, %s.b0) from %s where time > %s and time < %s;",
                        tableMain.key, tableMain.key, constants.IoTDB_DATABASE,
                        timeRange.left, timeRange.right);
        // IoTDB pushes down filter.
        session.executeNonQueryStatement(sql);
    }

    @Override
    public void rangeFilter(List<Schema> tableMain, double selectivity) throws Exception {
        for(int i=0;i<tableMain.size();i++) {
            this.rangeFilter(tableMain.get(i), selectivity);
        }
    }

    @Override
    public void valueFilter(Schema tableMain, double selectivity) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        Range valueRange = Range.randomBySelectivity(constants.valueRange.get(tableMain.main), selectivity);
        String sql = String.format
                ("select coalesce(main, b0) from %s where b0 is not null or (A > %s and A < %s); ",
                        tableMain.deviceId,
                        valueRange.left, valueRange.right);
        session.executeQueryStatement(sql);
    }

    @Override
    public void valueFilter(List<Schema> tableMain, double selectivity) throws Exception {
        for(int i=0;i<tableMain.size();i++) {
            this.valueFilter(tableMain.get(i), selectivity);
        }
    }

    @Override
    public void downSample(Schema tableMain, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        //
//        session.executeQueryStatement(String.format("delete timeseries %s.%s.A", constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO));
    }


    public void downSample1(Schema tableMain, int window, String label) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        String sql = String.format
                ("select avg(b0-main) into %s.%s(%s) from %s group by session(%ss); ",
                        constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO, label,
                        tableMain.deviceId, window);
//        System.out.println(sql);
        String sql2 = String.format
                ("select avg(main) from %s group by session(%ss);",
                        tableMain.deviceId,window);
        session.executeNonQueryStatement(sql);
        session.executeQueryStatement(sql2);
        labelGarbage.add(label);
    }

    public void clean_temp() throws IoTDBConnectionException, StatementExecutionException {
        for(String label: this.labelGarbage) {
            try {
                session.deleteTimeseries(constants.IoTDB_DATABASE + "." + constants.IoTDB_AGG_INTO + "." + label);
            } catch (Exception e) {}
        }
    }

    @Override
    public void downSample(List<Schema> tableMain, int window) throws Exception {
        for(int i=0;i<tableMain.size();i++) {
            this.downSample1(tableMain.get(i), window, tableMain.get(i).key);
        }
    }

    @Override
    public void downSampleAligned(Schema tableMain1, Schema tableMain2, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        downSample1(tableMain1, window, tableMain1.key + "A");
        downSample1(tableMain2, window, tableMain2.key + "B");
        String sql3 = String.format
                ("select avg(%sA), avg(%sB) from %s.%s;",
                        tableMain1.key, tableMain2.key,
                        constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO);
        session.executeQueryStatement(sql3);
        labelGarbage.add(tableMain1.key + "A"); labelGarbage.add(tableMain2.key + "B");
    }

    @Override
    public void downSampleAligned(List<Schema> tableMain, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.downSampleAligned(tableMain.get(i), tableMain.get(i+1), window);
        }
    }

    @Override
    public void downSampleUnion(Schema tableMain1, Schema tableMain2, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        downSample1(tableMain1, window, tableMain1.key + "A");
        downSample1(tableMain2, window, tableMain2.key + "B");
        String sql3 = String.format
                ("select coalesce(%sA, %sB) from %s.%s;",
                        tableMain1.key, tableMain2.key,
                        constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO);
        session.executeQueryStatement(sql3);
    }

    @Override
    public void downSampleUnion(List<Schema> tableMain, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.downSampleUnion(tableMain.get(i), tableMain.get(i+1), window);
        }
    }

    @Override
    public void seriesJoinValue(Schema tableMain1, Schema tableMain2) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        String sql3 = String.format
                ("select %s.main, %s.main from %s where (%s.main=%s.main and %s.b0 is null) or " +
                                "(%s.main=%s.main and %s.b0 is null) or (%s.b0=%s.main and %s.b0 is not null) or " +
                                "(%s.main=%s.main and %s.b0 is not null);",
                        tableMain1.key, tableMain2.key,
                        constants.IoTDB_DATABASE,
                        tableMain1.key, tableMain2.key, tableMain1.key,
                        tableMain1.key, tableMain2.key, tableMain2.key,
                        tableMain1.key, tableMain2.key, tableMain1.key,
                        tableMain1.key, tableMain2.key, tableMain2.key
                        );
        session.executeQueryStatement(sql3);
    }

    @Override
    public void seriesJoinValue(List<Schema> tableMain) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.seriesJoinValue(tableMain.get(i), tableMain.get(i+1));
        }
    }

    @Override
    public void branchAlign(Schema tableMain1, Schema tableMain2) throws SQLException {

    }

    @Override
    public void branchAlign(List<Schema> tableMain) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        String keys = ""; String filter = "";
        for(int i=0;i<tableMain.size();i++) {
            Schema schema = tableMain.get(i);
            if(i == 0) keys += String.format("coalesce(%s.main,%s.b0) ",schema.key, schema.key);
            else keys += String.format(", coalesce(%s.main,%s.b0) ",schema.key, schema.key);
            if(i == 0) filter += String.format(" %s.b0 is not null ", schema.key);
            else filter += String.format(" and %s.b0 is not null ", schema.key);
        }
        String sql = String.format
                ("select %s from %s where %s; ",
                        keys,
                        constants.IoTDB_DATABASE, filter);
//        System.out.println(sql);
        session.executeQueryStatement(sql);
    }

    @Override
    public void alignPartialReading(List<Schema> tableMain, int attrs) throws SQLException, IoTDBConnectionException, StatementExecutionException, IOException {
        for(int i=2;i<constants.descriptor.mainName.size();i++) {
            long now = System.currentTimeMillis();
            Collections.shuffle(tableMain);
            List<Schema> toTest = tableMain.subList(0, attrs);
            branchAlign(toTest);
            long cost = (System.currentTimeMillis() - now);
            System.out.println(cost);
            wres.write(String.valueOf(cost) + " ");
        }
    }

    public void alignPartialReading(List<Schema> tableMain) throws SQLException, IoTDBConnectionException, StatementExecutionException, IOException {
        for(int i=2;i<constants.descriptor.mainName.size();i++) {
            long now = System.currentTimeMillis();
            Collections.shuffle(tableMain);
            List<Schema> toTest = tableMain.subList(0, i);
            branchAlign(toTest);
            long cost = (System.currentTimeMillis() - now);
            System.out.println(cost);
            wres.write(String.valueOf(cost) + " ");
        }
    }

    public void unionPartialReading(List<Schema> tableMain) throws SQLException, IoTDBConnectionException, StatementExecutionException, IOException {
        for(int i=2;i<constants.descriptor.mainName.size();i++) {
            long now = System.currentTimeMillis();
            Collections.shuffle(tableMain);
            List<Schema> toTest = tableMain.subList(0, i);
            branchAlign(toTest);
            long cost = (System.currentTimeMillis() - now);
            System.out.println(cost);
            wres.write(String.valueOf(cost) + " ");
        }
    }

    @Override
    public void execute() throws Exception {
        long rat = System.currentTimeMillis()%10000;
        String fres = constants.RES_PREFIX + "IoTDBV-" + rat + constants.dataset + constants.RES_POSTFIX;
        String flog = constants.LOG_PREFIX + "IoTDBV-" + rat + constants.dataset + constants.LOG_POSTFIX;
        wres = new FileWriter(new File(fres));
        wlog = new FileWriter(new File(flog));
        System.out.println("Saved as " + fres);
        for(BenchFunctions bf: constants.BENCHMARK_CODE) {
            wlog.write(bf.name() + " ");
            int ti = 2;
//            if(bf == BenchFunctions.VALUE_JOIN) ti = 2;
            long cost = 0;
            for(int i=0;i<ti;i++) {
                try {

                    this.insertDataPrepare(this.constants.getDatasetPath(), Integer.MAX_VALUE, 0.1, 0, 0., 1);
//                    this.insertDataPrepare(8400000, 0.10, 0, 0, 1);
                } catch (Exception e) {
                    System.err.println(e);
                    //System.exit(-1);
                }
                long now = System.currentTimeMillis();
                switch (bf) {
                    case ALIGN: align(constants.descriptor.mainName); break;
                    case VALUE_FILTER: valueFilter(constants.descriptor.mainName, constants.SELECTIVITY); break;
                    case RANGE_FILTER: rangeFilter(constants.descriptor.mainName, constants.SELECTIVITY); break;
                    case DOWNSAMPLE: downSample(constants.descriptor.mainName, constants.WINDOW); break;
                    case DOWNSAMPLE_ALIGN: downSampleAligned(constants.descriptor.mainName, constants.WINDOW); break;
                    case DOWNSAMPLE_UNION: downSampleUnion(constants.descriptor.mainName, constants.WINDOW); break;
                    case VALUE_JOIN: seriesJoinValue(constants.descriptor.mainName); break;
                    case BRANCH_ALIGN: branchAlign(constants.descriptor.mainName); break;
                    case UPDATE: update(constants.descriptor.mainName); break;
                    case ALIGN_PARTIAL:alignPartialReading(constants.descriptor.mainName); break;
                }
                cost += (System.currentTimeMillis() - now);
                //clean
                switch (bf) {
                    case ALIGN:
                    case VALUE_FILTER:
                    case RANGE_FILTER: break;
                    case DOWNSAMPLE:
                    case DOWNSAMPLE_ALIGN:
                    case DOWNSAMPLE_UNION: clean_temp(); break;
                    case VALUE_JOIN:
                    case BRANCH_ALIGN: break;
                    case UPDATE: break;
                    default:
                }
            }
            cost /= ti;
            System.out.println(cost);
            wres.write(String.valueOf(cost) + " ");
        }
        wlog.close();
        wres.close();
    }

    @Override
    public void execute(BenchFunctions benchFunctions) throws Exception {

    }

    public void disk_ssd() throws Exception {
        IoTDBV ioTDB = new IoTDBV();
        ioTDB.init(1, DataSet.Climate, "IoTDB-v");
        ioTDB.gen_update_only  = true;
        ioTDB.insertDataPrepare(ioTDB.constants.getDatasetPath(), Integer.MAX_VALUE, 0.1, 0, 0, 1);
        ioTDB.constants.SELECTIVITY = 1.0;
        ioTDB.execute();
    }

    public void disk() throws Exception {
        IoTDBV ioTDB = new IoTDBV();
        ioTDB.init(1, DataSet.Ship, "IoTDB-v");
        ioTDB.gen_update_only  = true;
        ioTDB.insertDataPrepare(ioTDB.constants.getDatasetPath(), Integer.MAX_VALUE, 0.1, 0, 0, 1);
        ioTDB.constants.SELECTIVITY = 1.0;
        ioTDB.execute();
    }

    public static void benchmarking() throws Exception {
        IoTDBV ioTDB = new IoTDBV();
        ioTDB.init(1, DataSet.Climate, "IoTDB-v");
        //ioTDB.gen_update_only  = true;
//        ioTDB.insertDataPrepare(ioTDB.constants.getDatasetPath(), Integer.MAX_VALUE, 0.1, 0, 0, 1);
        ioTDB.execute();
//        try {
//            ioTDB.clean();
//        } catch (Exception e) {
//
//        }
//        try {
//            ioTDB.create();
//        } catch (Exception e) {
//
//        }
//        try {
//            for(double ratx: new double[]{0, 0.50, 0.75, 0.875, 0.9375}) {
//            for(int t=0;t<3;t++) {
//                ioTDB.clean();
//                ioTDB.insertDataPrepare(ioTDB.constants.getDatasetPath(), Integer.MAX_VALUE, 0.10, 0.50, 0, 1);
//                ioTDB.param = ratx;
//                ioTDB.execute();
//                ioTDB.clean();
//            }
//            }
//            ioTDB.insertDataPrepare(ioTDB.constants.getDatasetPath(), Integer.MAX_VALUE, 0.10, 0, 0.10, 1);
//            for(int i=0;i<10;i++) {
//                ioTDB.insertDataPrepare_gene(1000000, 0.1, 0, 0, 1);
////                ioTDB.execute();
////                ioTDB.repair = new HashMap<>();
//            }
//            ioTDB.execute();
//        } catch (Exception e) {
//            System.err.println(e);
//            //System.exit(-1);
//        }

//        ioTDB.registerCoalesce(); // execute once.
//        for(int i=1;i<=20;i++) {
//            ioTDB.constants.SELECTIVITY = i*0.05;
//            ioTDB.execute();
//        }

//        ioTDB.clean();
    }

    public static void main(String[] args) throws Exception {
        benchmarking();
//        IoTDBV ioTDB = new IoTDBV();
//        ioTDB.init(1, DataSet.Climate, "IoTDB-v");
//        try {
//            ioTDB.clean();
//        } catch (Exception e) {
//
//        }
    }

}
