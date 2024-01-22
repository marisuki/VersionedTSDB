import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.filter.operator.In;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class IoTDB extends benchmarks {
    public Constants constants;
    public Session session;
    FileWriter wlog;
    public Map<String, Tablet> repair = new HashMap<>();
    int scale_count = 0;
    double param = 0;
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
        session.close();
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
        if(path.equals(constants.path_noise)) {
            insertDataPrepare_gene(length, upd, delay, dup, verbose);
            return;
        }
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
//                    if(rd.nextDouble() < param) continue;
                    String reads  = "";
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
            if(!gen_update_only) {
                session.insertTablet(
                        construct(schema, "main",
                                timestamp.get(schema.main), data.get(schema.main), false));
            }
            for(String branchx: schema.measurements) {
                if(!gen_update_only) {
//                    session.insertTablet(
//                            construct(schema, branchx,
//                                    timestamp.get(schema.deviceId + "." + branchx),
//                                    data.get(schema.deviceId + "." + branchx), true), false);
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
                    if(rd.nextDouble() < 0.75) continue;
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
//                    session.insertTablet(
//                            construct(schema, branchx,
//                                    timestamp.get(schema.deviceId + "." + branchx),
//                                    data.get(schema.deviceId + "." + branchx), true), false);
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
        for(String branchx: main.measurements) {
            Tablet tablet = repair.get(main.deviceId + "." + branchx);
            List<MeasurementSchema> iotx = List.of(new MeasurementSchema("main", TSDataType.INT32));
            tablet.setSchemas(iotx);
            session.insertTablet(tablet);
        }
        session.executeQueryStatement(String.format("select main from %s;", main.deviceId));
    }

    @Override
    public void align(Schema tableMain1, Schema tableMain2) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        update(tableMain1); update(tableMain2);
        String sql = String.format
                ("select main from %s, %s;",
                        tableMain1.deviceId, tableMain2.deviceId);
        session.executeQueryStatement(sql);
    }

    @Override
    public void align(List<Schema> tableMain) throws Exception {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.align(tableMain.get(i), tableMain.get(i+1));
        }
    }

    @Override
    public void rangeFilter(Schema tableMain, double selectivity) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        update(tableMain);
        Range timeRange = Range.randomBySelectivity(constants.timeRange.get(tableMain.main), selectivity);
        String sql = String.format
                ("select main from %s where time > %s and time < %s;",
                        tableMain.deviceId,
                        timeRange.left, timeRange.right);
        session.executeQueryStatement(sql);
    }

    @Override
    public void rangeFilter(List<Schema> tableMain, double selectivity) throws Exception {
        for(int i=0;i<tableMain.size();i++) {
            this.rangeFilter(tableMain.get(i), selectivity);
        }
    }

    @Override
    public void valueFilter(Schema tableMain, double selectivity) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        update(tableMain);
        Range valueRange = Range.randomBySelectivity(constants.valueRange.get(tableMain.main), selectivity);
        String sql = String.format
                ("select main from %s where A > %s and A < %s; ",
                        tableMain.deviceId, valueRange.left, valueRange.right);
        session.executeNonQueryStatement(sql);
    }

    @Override
    public void valueFilter(List<Schema> tableMain, double selectivity) throws Exception {
        for(int i=0;i<tableMain.size();i++) {
            this.valueFilter(tableMain.get(i), selectivity);
        }
    }

    @Override
    public void downSample(Schema tableMain, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        update(tableMain);
        String sql2 = String.format
                ("select avg(main) from %s group by session(%ss);",
                        tableMain.deviceId, window);
        session.executeQueryStatement(sql2);
//        session.executeQueryStatement(String.format("delete timeseries %s.%s.A", constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO));
    }

    @Override
    public void downSample(List<Schema> tableMain, int window) throws Exception {
        for(int i=0;i<tableMain.size();i++) {
            this.downSample(tableMain.get(i), window);
        }
    }

    @Override
    public void downSampleAligned(Schema tableMain1, Schema tableMain2, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        update(tableMain1); update(tableMain2);
        String sql = String.format
                ("select avg(main), avg(main) from %s, %s group by session(%ss);",
                        tableMain1.deviceId, tableMain2.deviceId, window);
        session.executeNonQueryStatement(sql);
//        session.executeQueryStatement(String.format("delete timeseries %s.%s.*", constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO));
    }

    @Override
    public void downSampleAligned(List<Schema> tableMain, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.downSampleAligned(tableMain.get(i), tableMain.get(i+1), window);
        }
    }

    @Override
    public void downSampleUnion(Schema tableMain1, Schema tableMain2, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        String sql1 = String.format
                ("select coalesce(%s.main, %s.b0) into %s.%s(A) from %s; ",
                        tableMain1.key, tableMain1.key,
                        constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO,
                        constants.IoTDB_DATABASE);
        String sql2 = String.format
                ("select coalesce(%s.main, %s.b0) into %s.%s(B) from %s; ",
                        tableMain2.key, tableMain2.key,
                        constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO,
                        constants.IoTDB_DATABASE);
        String sql3 = String.format
                ("select coalesce(A, B) into %s.%s(C) from %s.%s;",
                        constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO,
                        constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO);
        String sql4 = String.format
                ("select avg(C) from %s.%s group by session(%ss);",
                        constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO, window);
        session.executeNonQueryStatement(sql1);
        session.executeNonQueryStatement(sql2);
        session.executeNonQueryStatement(sql3);
        session.executeQueryStatement(sql4);
//        session.executeQueryStatement(String.format("delete timeseries %s.%s.*", constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO));
    }

    @Override
    public void downSampleUnion(List<Schema> tableMain, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.downSampleUnion(tableMain.get(i), tableMain.get(i+1), window);
        }
    }

    @Override
    public void seriesJoinValue(Schema tableMain1, Schema tableMain2) throws SQLException, IoTDBConnectionException, StatementExecutionException {
        update(tableMain1); update(tableMain2);
        String sql3 = String.format
                ("select A, B from %s.%s where A=B;",
                        constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO);
        session.executeQueryStatement(sql3);
//        session.executeQueryStatement(String.format("delete timeseries %s.%s.*", constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO));
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
        String keys = "";
        update(tableMain);
        for(int i=0;i<tableMain.size();i++) {
            Schema schema = tableMain.get(i);
//            String sql = String.format
//                    ("select coalesce(%s.main, %s.b0) into %s.%s(%s) from %s; ",
//                            schema.key, schema.key,
//                            constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO, schema.key,
//                            constants.IoTDB_DATABASE);
//            session.executeQueryStatement(sql);
            if(i == 0) keys += schema.key + ".main";
            else keys += ", " + schema.key + ".main";
        }
        String pattern = "select %s from %s;";
        session.executeQueryStatement(
                String.format(pattern, keys, constants.IoTDB_DATABASE));
//        session.executeQueryStatement(String.format("delete timeseries %s.%s.*", constants.IoTDB_DATABASE, constants.IoTDB_AGG_INTO));
    }

    @Override
    public void alignPartialReading(List<Schema> tableMain, int attrs) throws SQLException, IoTDBConnectionException, StatementExecutionException {

    }

    @Override
    public void execute(BenchFunctions benchFunctions) throws Exception {

    }

    public void registerCoalesce() throws IoTDBConnectionException, StatementExecutionException {
        session.executeNonQueryStatement("create function coalesce as 'org.apache.iotdb.library.util.coalesce';");
    }

    @Override
    public void execute() throws Exception {
        long rat = System.currentTimeMillis()%10000;
        String fres = constants.RES_PREFIX + "IoTDB-dup" + rat + constants.dataset + constants.RES_POSTFIX;
        String flog = constants.LOG_PREFIX + "IoTDB-" + rat + constants.dataset + constants.LOG_POSTFIX;
        FileWriter wres = new FileWriter(new File(fres));
        wlog = new FileWriter(new File(flog));
        System.out.println("Saved as " + fres);
        for(BenchFunctions bf: constants.BENCHMARK_CODE) {
            wlog.write(bf.name() + " ");
            int ti = 2;
//            if(bf == BenchFunctions.VALUE_JOIN) ti = 2;
            try {
//                this.clean();
            } catch (Exception e) {
                System.err.println(e);
                //System.exit(-1);
            }
            try {
                this.insertDataPrepare(this.constants.getDatasetPath(), 8400000, 0.1, 0, 0, 1);
//                    this.insertDataPrepare(8400000, 0.10, 0, 0, 1);
            } catch (Exception e) {
                System.err.println(e);
                //System.exit(-1);
            }
            long cost = 0;
            for(int i=0;i<ti;i++) {
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
                }
                cost += (System.currentTimeMillis() - now);
            }
            cost /= ti;
            System.out.println(cost);
            wres.write(String.valueOf(cost) + " ");
//            this.clean();
        }
        wlog.close();
        wres.close();
    }

    public static void benchmarking() throws Exception {
        IoTDB ioTDB = new IoTDB();
        ioTDB.init(1, DataSet.Noise, "IoTDB");
        ioTDB.gen_update_only  = true;
        ioTDB.insertDataPrepare(ioTDB.constants.getDatasetPath(), 100000000, 0.1, 0, 0, 1);
        ioTDB.constants.SELECTIVITY = 1.0;
        ioTDB.execute();
        // ioTDB.registerCoalesce(); // execute once.
//        ioTDB.clean();
//        try {
//            ioTDB.clean();
//        } catch (Exception e) {
//            System.err.println(e);
//            //System.exit(-1);
//        }
//        try {
//            for(int i=1;i<=20;i++) {
//                ioTDB.constants.SELECTIVITY = i*0.05;
//                ioTDB.execute();
//            }
//
////            for(int i=0;i<10;i++) {
////                ioTDB.insertDataPrepare_gene(1000000, 0.1, 0, 0.1, 1);
////
//////                ioTDB.repair = new HashMap<>();
////            }
//            ioTDB.clean();
//        } catch (Exception e) {
//            System.err.println(e);
//            //System.exit(-1);
//        }
////        ioTDB.execute();
////        ioTDB.clean();
////        for(int i=1;i<=20;i++) {
////            ioTDB.constants.SELECTIVITY = i*0.05;
////            ioTDB.execute();
////        }
    }

    public static void main(String[] args) throws Exception {
        benchmarking();
//        IoTDBV ioTDB = new IoTDBV();
//        ioTDB.init(1, DataSet.Climate, "IoTDB");
//        try {
//            ioTDB.clean();
//        } catch (Exception e) {
//
//        }
    }
}
