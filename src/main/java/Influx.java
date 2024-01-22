import com.influxdb.client.*;
import com.influxdb.client.domain.*;
import com.influxdb.client.write.Point;
import com.influxdb.client.write.WriteParameters;
import com.influxdb.query.FluxTable;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * Docker code:
 * docker run --name influxdb -p 8086:8086 influxdb:2.7.4
 * visit: localhost:8086 to set org, pwd, bucket and get token
 * to update the following constant parameters in benchmark class Influx.
 */
public class Influx extends benchmarks {

    final String token = "mXuELfLAhy4mX4bULxwqEtqJElayE3CzFjaL8PvcKlmK-DNilmwssUT-KkKlfnKDLKRhDPMfxw5xLVihlEQM9w==";
    final String user = "rk";
    final String pwd = "rkrkrkrk";
    final String org = "org"; final String bucket = "version";
    FileWriter wlog;
    FileWriter wres;

    InfluxDBClient client;
    Constants constants;
    long maxTime = 0;
    long minTime = Long.MAX_VALUE;
    Map<String, List<Point>> repair = new HashMap<>();
    int scale_count = 0;
    double param = 0;
    long write = 0;
    boolean gen_upd_only = false;

    @Override
    public void create() throws SQLException {
        //clean.
    }

    @Override
    public void clean() throws SQLException {
        DeleteApi deleteApi = client.getDeleteApi();
        String pred = String.format("_measurement=\"%s\"", constants.IoTDB_DATABASE);
        DeletePredicateRequest request = new DeletePredicateRequest();
        request.setPredicate(pred);
        Instant insm = Instant.ofEpochSecond(0);
        request.setStart(OffsetDateTime.ofInstant(insm, ZoneId.of("UTC")));
        Instant insM = Instant.now();
        request.setStop(OffsetDateTime.ofInstant(insM, ZoneId.of("UTC")));
        deleteApi.delete(request, bucket, org);
    }

    @Override
    public void init(int num_branch, DataSet dataset, String ev) throws SQLException {
        client = InfluxDBClientFactory.create("http://127.0.0.1:8086?readTimeout=60m&connectTimeout=60m&writeTimeout=60m", token.toCharArray(), org, bucket);
        constants = new Constants(num_branch, dataset, ev);
    }

    @Override
    public void insert() throws SQLException {

    }

    @Override
    public void insertCSV() throws SQLException {

    }

    @Override
    public void insertDataPrepare(String path, int length, double upd, double delay, double dup, int verbose) throws Exception {
        if(path.equals(constants.path_noise)) {
            insertDataPrepare_gene(length, upd, delay, dup, verbose);
        }
        WriteApiBlocking writeApi = client.getWriteApiBlocking();
        List<Point> dataPoints = new ArrayList<>();
        Random rd = new Random();
        Deque<Pair<String, Pair<Long, Integer>>> delayed = new LinkedList<>();
        Scanner sc = new Scanner(new File(path));
        Map<String, Range> vr = new HashMap<>(); // guarantee unique colname
        sc.nextLine(); // skip header.
        for(int i=0;i< constants.descriptor.mainName.size();i++) {
            Schema schema = constants.descriptor.mainName.get(i);
            repair.put(schema.main, new ArrayList<>());
            vr.put(schema.main, new Range(10000000.0, -100000.0));
        }
        int pos = 0;
        while (sc.hasNext() && pos < length) {
            String line = sc.nextLine();
            String[] s = line.split(",");
            for(int i=0;i< constants.descriptor.mainName.size();i++) {
                Schema schema = constants.descriptor.mainName.get(i);
                int minus = 0;
                for(String attr: schema.attributes.keySet()) {
                    String ps = "0";
//                    if(rd.nextDouble() < param) {
//                        minus = 1;
//                        continue;
//                    }
                    if(s.length > schema.attributes.get(attr)) ps = s[schema.attributes.get(attr)];
                    if(ps.isEmpty()) ps = "0";
                    long toInsert;
                    if(schema.types.get(attr) == Type.INT) {
                        long x = Long.parseLong(ps);
                        toInsert = x*10000;
                        vr.get(schema.main).update(x);
                    } else {
                        double x = Double.parseDouble(ps);
                        toInsert = (int) (x*10000);
                        vr.get(schema.main).update(toInsert);
                    }
                    long timex = constants.descriptor.gen.get(i)*pos;
                    maxTime = Long.max(maxTime, timex);
                    minTime = Long.min(minTime, timex);
                    if((verbose & 1) == 1 && minus == 0 && !gen_upd_only) {
                        if(rd.nextDouble() < delay) {
                            delayed.add(new Pair<>(schema.key, new Pair<>(timex, (int)(toInsert))));
                        } else {
                            Point p = new Point(constants.IoTDB_DATABASE)
                                    .addTag( "attribute", schema.key)
                                    .addField("main", toInsert)
                                    .time(timex, WritePrecision.S);
                            dataPoints.add(p);
                        }
                    } //else System.out.println(schema.main + " " + toInsert);
                    if(rd.nextDouble() < upd&& minus == 0) {
                        String branchSuffix = SchemaDescriptor.random_select_update_table(constants.descriptor.branchPostfix);
                        Point p = new Point(constants.IoTDB_DATABASE)
                                .addTag( "attribute", schema.key)
                                .addField("main" + branchSuffix, toInsert + 10)
                                .time(timex, WritePrecision.S);
                        //dataPoints.add(p);
                        repair.get(schema.main).add(p);
                    }
                    if(rd.nextDouble() < dup&& minus == 0&& !gen_upd_only) {
                        if((verbose & 1) == 1) {
                            Point p = new Point(constants.IoTDB_DATABASE)
                                    .addTag( "attribute", schema.key)
                                    .addField("main", toInsert)
                                    .time(timex, WritePrecision.S);
                            dataPoints.add(p);
                        } //else System.out.println(schema.main + " " + toInsert);
                    }
                }
            }
            if(dataPoints.size() >= 100000) {
                long now = System.currentTimeMillis();
                WriteParameters wp = new WriteParameters(bucket, org, WritePrecision.S, WriteConsistency.ALL);
                writeApi.writePoints(dataPoints, wp);
                write += System.currentTimeMillis() - now;
                dataPoints = new ArrayList<>();
                constants.valueRange = vr;
            }
            pos++;
        }
        for(int i=0;i< constants.descriptor.mainName.size();i++) {
            Schema schema = constants.descriptor.mainName.get(i);
            constants.timeRange.put(schema.main, Range.createRange(0, constants.descriptor.gen.get(i) * pos));
        }
        while(!delayed.isEmpty()) {
            Pair<String, Pair<Long, Integer>> exec = delayed.pollFirst();
            if((verbose & 1) == 1) {
                Point p = new Point(constants.IoTDB_DATABASE)
                        .addTag( "attribute", exec.left)
                        .addField("main", exec.right.right)
                        .time(exec.right.left, WritePrecision.S);
                dataPoints.add(p);
            }
            //else System.out.println(exec);
        }
        long now = System.currentTimeMillis();
        constants.valueRange = vr;
        WriteParameters wp = new WriteParameters(bucket, org, WritePrecision.S, WriteConsistency.ALL);
        writeApi.writePoints(dataPoints, wp);
        write += System.currentTimeMillis() - now;
        System.out.println(write);
    }

    @Override
    public void insertDataPrepare_gene(int length, double upd, double delay, double dup, int verbose) throws Exception {
        WriteApiBlocking writeApi = client.getWriteApiBlocking();
        List<Point> dataPoints = new ArrayList<>();
        Random rd = new Random();
        Deque<Pair<String, Pair<Long, Integer>>> delayed = new LinkedList<>();
        Map<String, Range> vr = new HashMap<>(); // guarantee unique colname
        for(int i=0;i< constants.descriptor.mainName.size();i++) {
            Schema schema = constants.descriptor.mainName.get(i);
            vr.put(schema.main, new Range(Double.MAX_VALUE, Double.MIN_VALUE));
            repair.put(schema.main, new ArrayList<>());
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
                        toInsert = x*10000;
                    } else {
                        float x = rd.nextFloat();
                        vr.get(schema.main).update(x);
                        toInsert = (int) (x*10000);
                    }
                    long timex = constants.descriptor.gen.get(i)*pos;
                    maxTime = Long.max(maxTime, timex);
                    minTime = Long.min(minTime, timex);
                    if((verbose & 1) == 1) {
                        if(rd.nextDouble() < delay) {
                            delayed.add(new Pair<>(schema.key, new Pair<>(timex, toInsert)));
                        } else {
                            Point p = new Point(constants.IoTDB_DATABASE)
                                    .addTag( "attribute", schema.key)
                                    .addField("main", toInsert)
                                    .time(timex, WritePrecision.S);
                            dataPoints.add(p);
                        }
                    } //else System.out.println(schema.main + " " + toInsert);
                    if(rd.nextDouble() < upd) {
                        String branchSuffix = SchemaDescriptor.random_select_update_table(constants.descriptor.branchPostfix);
                        Point p = new Point(constants.IoTDB_DATABASE)
                                .addTag( "attribute", schema.key)
                                .addField("b" + branchSuffix, toInsert + 10)
                                .time(timex, WritePrecision.S);
                        //dataPoints.add(p);
                        repair.get(schema.main).add(p);
                    }
                    if(rd.nextDouble() < dup) {
                        if((verbose & 1) == 1) {
                            Point p = new Point(constants.IoTDB_DATABASE)
                                    .addTag( "attribute", schema.key)
                                    .addField("main", toInsert)
                                    .time(timex, WritePrecision.S);
                            dataPoints.add(p);
                        } //else System.out.println(schema.main + " " + toInsert);
                    }
                }
            }
            if(dataPoints.size() == 1000000) {
//                System.out.println(dataPoints.size() + " " + scale_count + " SZ");
                for(int k=0;k< constants.descriptor.mainName.size();k++) {
                    Schema schema = constants.descriptor.mainName.get(k);
                    constants.timeRange.put(schema.main, Range.createRange(0, constants.descriptor.gen.get(k) * pos));
                }
                constants.valueRange = vr;
                long now = System.currentTimeMillis();
                WriteParameters wp = new WriteParameters(bucket, org, WritePrecision.S, WriteConsistency.ALL);
                writeApi.writePoints(dataPoints, wp);
                write += System.currentTimeMillis() - now;
                dataPoints = new ArrayList<>();

//                process();
                scale_count ++;
            }
            pos++;
        }
        for(int i=0;i< constants.descriptor.mainName.size();i++) {
            Schema schema = constants.descriptor.mainName.get(i);
            constants.timeRange.put(schema.main, Range.createRange(0, constants.descriptor.gen.get(i) * pos));
        }
        while(!delayed.isEmpty()) {
            Pair<String, Pair<Long, Integer>> exec = delayed.pollFirst();
            if((verbose & 1) == 1) {
                Point p = new Point(constants.IoTDB_DATABASE)
                        .addTag( "attribute", exec.left)
                        .addField("main", exec.right.right)
                        .time(exec.right.left, WritePrecision.S);
                dataPoints.add(p);
            }
            //else System.out.println(exec);
        }
//        System.out.println("Created.");
        long now = System.currentTimeMillis();
        constants.valueRange = vr;
        WriteParameters wp = new WriteParameters(bucket, org, WritePrecision.S, WriteConsistency.ALL);
        writeApi.writePoints(dataPoints, wp);
        write += System.currentTimeMillis() - now;
        System.out.println(write);
    }

    @Override
    public void update(List<Schema> main) throws SQLException {
        for(Schema schema: main) {
            this.update(schema);
        }
    }

    @Override
    public void update(Schema main) throws SQLException {
        WriteApiBlocking writeApi = client.getWriteApiBlocking();
        WriteParameters wp = new WriteParameters(bucket, org, WritePrecision.S, WriteConsistency.ALL);
        writeApi.writePoints(repair.get(main.main), wp);
//        repair.replace(main.main, new ArrayList<>());
    }

    @Override
    public void align(Schema tableMain1, Schema tableMain2) throws SQLException {
        update(tableMain1); update(tableMain2);
        String pattern = "import \"join\" import \"array\" " +
                "left = from(bucket:\"version\") " +
                    "|> range(start: 0) " +
                    "|> filter(fn: (r) => r.attribute==\"%s\" and r._field==\"main\") " +
                    "|> group (columns: [\"_time\"]) " +
                "right = from(bucket:\"version\") " +
                    "|> range(start: 0) " +
                    "|> filter(fn: (r) => r.attribute==\"%s\" and r._field==\"main\") " +
                    "|> group (columns: [\"_time\"]) " +
                "join.inner(left:left, right:right, " +
                    "on: (l, r) => l._time==r._time, " +
                    "as:(l,r) => ({l with label: r._value}))"; // |> limit(n:1000)
        String comm = String.format(pattern, tableMain1.key, tableMain2.key);
//        System.out.println(comm);
        QueryApi api = client.getQueryApi();
        Query q = new Query();
        q.query(comm);
        api.query(q);
//        System.out.println(res.size());
//        System.out.println(res.get(0).toString());
    }

    @Override
    public void align(List<Schema> tableMain) throws Exception {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.align(tableMain.get(i), tableMain.get(i+1));
        }
    }

    @Override
    public void rangeFilter(Schema tableMain, double selectivity) throws SQLException {
        update(tableMain);
        Range timeRange = Range.randomBySelectivity(constants.timeRange.get(tableMain.main), selectivity);
        String pattern =
                "from(bucket:\"version\") " +
                "|> range(start: %s, stop:%s) " +
                "|> filter(fn: (r) => r.attribute==\"%s\" and r._field==\"main\")";
        String comm = String.format(pattern, (int)(timeRange.left), (int)(timeRange.right), tableMain.key );
        QueryApi api = client.getQueryApi();
        Query q = new Query();
        q.query(comm);
        api.query(q);
    }

    @Override
    public void rangeFilter(List<Schema> tableMain, double selectivity) throws Exception {
        for(int i=0;i<tableMain.size();i++) {
            this.rangeFilter(tableMain.get(i), selectivity);
        }
    }

    @Override
    public void valueFilter(Schema tableMain, double selectivity) throws SQLException {
        update(tableMain);
        Range valueRange = Range.randomBySelectivity(constants.valueRange.get(tableMain.main), selectivity);
        String pattern =
                "from(bucket:\"version\") " +
                        "|> range(start: 0) " +
                        "|> filter(fn: (r) => r.attribute==\"%s\" and r._field==\"main\" and r._value > %s and r._value < %s)";
        String comm = String.format(pattern, tableMain.key, String.valueOf(valueRange.left), String.valueOf(valueRange.right));
        QueryApi api = client.getQueryApi();
        Query q = new Query();
        q.query(comm);
        api.query(q);
    }

    @Override
    public void valueFilter(List<Schema> tableMain, double selectivity) throws Exception {
        for(int i=0;i<tableMain.size();i++) {
            this.valueFilter(tableMain.get(i), selectivity);
        }
    }

    @Override
    public void downSample(Schema tableMain, int window) throws SQLException {
        update(tableMain);
        String pattern =
                "from(bucket:\"version\") " +
                        "|> range(start: 0) " +
                        "|> filter(fn: (r) => r.attribute==\"%s\" and r._field==\"main\")" +
                        "|> aggregateWindow(every: %ss, fn: mean, createEmpty: false)";
        String comm = String.format(pattern, tableMain.key, window);
        QueryApi api = client.getQueryApi();
        Query q = new Query();
        q.query(comm);
        api.query(q);
    }

    @Override
    public void downSample(List<Schema> tableMain, int window) throws Exception {
        for(int i=0;i<tableMain.size();i++) {
            this.downSample(tableMain.get(i), window);
        }
    }

    @Override
    public void downSampleAligned(Schema tableMain1, Schema tableMain2, int window) throws SQLException {
        update(tableMain1); update(tableMain2);
        String pattern = "import \"join\" import \"array\" " +
                "left = from(bucket:\"version\") " +
                    "|> range(start: 0) " +
                    "|> filter(fn: (r) => r.attribute==\"%s\" and r._field==\"main\") " +
                    "|> aggregateWindow(every: %ss, fn: mean, createEmpty: false)" +
                    "|> group (columns: [\"_time\"]) " +
                "right = from(bucket:\"version\") " +
                    "|> range(start: 0) " +
                    "|> filter(fn: (r) => r.attribute==\"%s\" and r._field==\"main\") " +
                    "|> aggregateWindow(every: %ss, fn: mean, createEmpty: false)" +
                    "|> group (columns: [\"_time\"]) " +
                "join.inner(left:left, right:right, " +
                    "on: (l, r) => l._time==r._time, " +
                    "as:(l,r) => ({l with label: r._value}))";
        String comm = String.format(pattern, tableMain1.key, window, tableMain2.key, window);
        QueryApi api = client.getQueryApi();
        Query q = new Query();
        q.query(comm);
        api.query(q);
    }

    @Override
    public void downSampleAligned(List<Schema> tableMain, int window) throws SQLException {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.downSampleAligned(tableMain.get(i), tableMain.get(i+1), window);
        }
    }

    @Override
    public void downSampleUnion(Schema tableMain1, Schema tableMain2, int window) throws SQLException {
        update(tableMain1); update(tableMain2);
        String pattern = "import \"join\" import \"array\" " +
                "left = from(bucket:\"version\") " +
                "|> range(start: 0) " +
                "|> filter(fn: (r) => r.attribute==\"%s\" and r._field==\"main\") " +
                "|> aggregateWindow(every: %ss, fn: mean, createEmpty: false)" +
                "|> group (columns: [\"_time\"]) " +
                "right = from(bucket:\"version\") " +
                "|> range(start: 0) " +
                "|> filter(fn: (r) => r.attribute==\"%s\" and r._field==\"main\") " +
                "|> aggregateWindow(every: %ss, fn: mean, createEmpty: false)" +
                "|> group (columns: [\"_time\"]) " +
                "union(tables: [left, right])";
        String comm = String.format(pattern, tableMain1.key, window, tableMain2.key, window);
        QueryApi api = client.getQueryApi();
        Query q = new Query();
        q.query(comm);
        api.query(q);
    }

    @Override
    public void downSampleUnion(List<Schema> tableMain, int window) throws SQLException {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.downSampleUnion(tableMain.get(i), tableMain.get(i+1), window);
        }
    }

    @Override
    public void seriesJoinValue(Schema tableMain1, Schema tableMain2) throws SQLException {
        update(tableMain1); update(tableMain2);
        String pattern = "import \"join\" import \"array\" " +
                "left = from(bucket:\"version\") " +
                    "|> range(start: 0) " +
                    "|> filter(fn: (r) => r.attribute==\"%s\" and r._field==\"main\") " +
                    "|> group (columns: [\"_time\"]) " +
                "right = from(bucket:\"version\") " +
                    "|> range(start: 0) " +
                    "|> filter(fn: (r) => r.attribute==\"%s\" and r._field==\"main\") " +
                    "|> group (columns: [\"_time\"]) " +
                "join.inner(left:left, right:right, " +
                    "on: (l, r) => {return l._time==r._time and l._value == r._value}, " +
                    "as:(l,r) => ({l with label: r._value}))";
        String comm = String.format(pattern, tableMain1.key, tableMain2.key);
        QueryApi api = client.getQueryApi();
        Query q = new Query();
        q.query(comm);
        api.query(q);
    }

    @Override
    public void seriesJoinValue(List<Schema> tableMain) throws SQLException {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.seriesJoinValue(tableMain.get(i), tableMain.get(i+1));
        }
    }

    @Override
    public void branchAlign(Schema tableMain1, Schema tableMain2) throws SQLException {

    }

    @Override
    public void branchAlign(List<Schema> tableMain) throws SQLException {
        update(tableMain);
        String pattern_FROM = " import \"join\" import \"array\" ";
        String pattern_GET  = " %s = from(bucket:\"version\") " +
                "|> range(start: 0) " +
                "|> filter(fn: (r) => r.attribute==\"%s\" and r._field==\"main\") " +
                "|> group (columns: [\"_time\"]) ";
        String pattern_JOIN = " %s=join.inner(left:%s, right:%s, " +
                "on: (l, r) => {return l._time==r._time}, " +
                "as: (l,r) => ({l with label: r._value})) ";
        String prev_to_join = tableMain.get(0).key + "GEN";
        String prev = String.format(pattern_GET, prev_to_join, tableMain.get(0).key);
        for(int i=1;i<tableMain.size();i++) {
            String tmp_tojoin = tableMain.get(i).key + "J";
            String tmp_to = tableMain.get(i).key + "GEN";
            prev += String.format(pattern_GET, tmp_tojoin, tableMain.get(i).key);
            prev += String.format(pattern_JOIN, tmp_to, tmp_tojoin, prev_to_join);
            prev_to_join = tmp_to;
        }
        String comm = pattern_FROM + prev + " " + prev_to_join;
        QueryApi api = client.getQueryApi();
        Query q = new Query();
        q.query(comm);
        api.query(q);
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

    @Override
    public void execute() throws Exception {
        long rat = System.currentTimeMillis()%10000;
        String fres = constants.RES_PREFIX + "Influx-" + rat + constants.dataset + constants.RES_POSTFIX;
        String flog = constants.LOG_PREFIX + "Influx-" + rat + constants.dataset + constants.LOG_POSTFIX;
        wres = new FileWriter(new File(fres));
        wlog = new FileWriter(new File(flog));
        System.out.println("Saved as " + fres);
        for(BenchFunctions bf: constants.BENCHMARK_CODE) {
            wlog.write(bf.name() + " ");
            int ti = 1;
            long cost = 0;
            for(int i=0;i<ti;i++) {
                try {
//                    this.clean();
                } catch (Exception e) {
                    System.err.println(e);
                    //System.exit(-1);
                }
                try {
//                    this.insertDataPrepare_gene(1000000, 0.10, 0, 0, 1);
                } catch (Exception e) {
                    System.err.println(e);
                    //System.exit(-1);
                }
//                System.out.println("Benchmark. ");
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
                    case ALIGN_PARTIAL:alignPartialReading(constants.descriptor.mainName, constants.partial_align); break;
                }
                cost += (System.currentTimeMillis() - now);
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

    void process() throws Exception {
        wres.write(scale_count + " ");
        long upd = 0;
        for(BenchFunctions bf: constants.BENCHMARK_CODE) {
            wlog.write(bf.name() + " " + System.currentTimeMillis());
            int ti = 1;
            long cost = 0;
            for(int i=0;i<ti;i++) {
//                System.out.println("Benchmark. ");
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
                    case ALIGN_PARTIAL:alignPartialReading(constants.descriptor.mainName, constants.partial_align); break;
                }
                cost += (System.currentTimeMillis() - now);
            }
            cost /= ti;
            System.out.println(bf.name() + " " + (cost + upd));
            wres.write(String.valueOf(cost + upd) + " ");
            if(bf.equals(BenchFunctions.UPDATE)) upd = cost;
        }
//        wlog.close();
//        wres.close();
    }

    public void selectivity_() throws Exception {
        Influx influx = new Influx();
        influx.init(1, DataSet.Climate, "Influx");
        System.out.println("Done.");
        for(int i=10;i<=10;i++) {
            influx.create();
            influx.constants.SELECTIVITY = i*0.10;
            influx.execute();
            try {
//                influx.clean();
            } catch (Exception e) {
                System.err.println(e);
                //System.exit(-1);
            }
        }
    }

    public void update_rate() throws Exception {
        Influx influx = new Influx();
        influx.init(1, DataSet.Bitcoin, "Influx");
        System.out.println("Done.");
        for(double ratx: influx.constants.ratio) {
            influx.create();
            influx.insertDataPrepare(influx.constants.getDatasetPath(), Integer.MAX_VALUE,
                    ratx/2.0, 0, ratx/2.0, 1);
            influx.insertCSV();
            influx.execute();
            try {
                influx.clean();
            } catch (Exception e) {
                System.err.println(e);
                //System.exit(-1);
            }
        }
    }

    public static void benchmarking() throws Exception {
        Influx influx = new Influx();
        influx.init(1, DataSet.Bitcoin, "Influx");
        System.out.println("Done.");
        try {
            influx.clean();
//            influx.insertDataPrepare(influx.constants.path_climate, 100, 0.10, 0, 0, 1);
        } catch (Exception e) {
            System.err.println(e);
            //System.exit(-1);
        }
        influx.gen_upd_only = false;
        influx.insertDataPrepare(influx.constants.getDatasetPath(), Integer.MAX_VALUE,
                0.10, 0, 0.0, 1);
        influx.execute();
//
//
//        influx.align(influx.constants.descriptor.mainName.get(0), influx.constants.descriptor.mainName.get(1));
//        ioTDB.registerCoalesce(); // execute once.
//        influx.execute();
//        ioTDB.clean();
    }

    public static void scale() throws Exception {
        Influx influx = new Influx();
        long rat = System.currentTimeMillis()%10000;
        influx.init(1, DataSet.Noise, "Influx");
        String fres = influx.constants.RES_PREFIX + "InfluxScale-" + rat + influx.constants.dataset + influx.constants.RES_POSTFIX;
        String flog = influx.constants.LOG_PREFIX + "InfluxScale-" + rat + influx.constants.dataset + influx.constants.LOG_POSTFIX;
        influx.wres = new FileWriter(new File(fres));
        influx.wlog = new FileWriter(new File(flog));
        System.out.println("Saved as " + fres);
        influx.insertDataPrepare_gene(100000000, 0.10, 0, 0, 1);
    }

    public static void main(String[] args) throws Exception {
        benchmarking();
//        scale();
        Influx influx = new Influx();
        influx.init(1, DataSet.Climate, "Influx");
        System.out.println("Done.");
//        influx.insertDataPrepare(influx.constants.path_climate, 100, 0.10, 0, 0, 1);
        try {
            influx.clean();

        } catch (Exception e) {
            System.err.println(e);
            //System.exit(-1);
        }
    }
}
