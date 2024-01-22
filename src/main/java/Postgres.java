
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.utils.Pair;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;


/**
 * docker run --name postgres -e -e POSTGRES_PASSWORD=postgres -d postgres
 * docker exec -it postgres psql -U postgres
 * create database db;
 * \c db
 */
public class Postgres extends benchmarks {
    public Constants constants;
    public Connection db;
    public double param;

    public ConcurrentLinkedDeque<String> insert_stmt = new ConcurrentLinkedDeque<>();
    public ConcurrentLinkedDeque<String> upd_stmt = new ConcurrentLinkedDeque<>();
    FileWriter wlog;
//    public Map<String, Long> updateTime = new HashMap<>(); // for fair comparison
    long upd_tme = 0;
    int scale_count = 0;

    // need thread safe.

    @Override
    public void create() throws SQLException {
        for(String sql: constants.descriptor.create) {
            Statement st = db.createStatement();
            st.executeUpdate(sql);
        }
    }

    @Override
    public void clean() throws SQLException {
        for(String sql: constants.descriptor.drop) {
            Statement st = db.createStatement();
            try {
                st.executeUpdate(sql);
            } catch (Exception e) {
                System.err.println(e.toString());
            }
        }
    }

    @Override
    public void init(int num_branch, DataSet dataset, String ev) throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/db?user=postgres&password=postgres";
        db = DriverManager.getConnection(url);
        constants = new Constants(num_branch, dataset, ev);
    }

    /**
     * method Insert()
     * requires stmt of insertDataPrepare or _gene.
     * write into db thread safely.
     * @throws SQLException
     */
    @Override
    public void insert() throws SQLException {
        System.out.println(insert_stmt.size());
        int total = insert_stmt.size() / 10;
        int pos = 0;
        while(!insert_stmt.isEmpty()) {
            String s = insert_stmt.poll();
            Statement st = db.createStatement();
            st.executeUpdate(s);
            if(pos % total == 0) System.out.println("Inserting " + pos/total + "/10.");
            pos ++;
        }
        while(!upd_stmt.isEmpty()) {
            String s = upd_stmt.poll();
            Statement st = db.createStatement();
            st.executeUpdate(s);
        }
    }

    @Override
    public void insertCSV() throws SQLException, IOException {
        for(Schema schema: constants.descriptor.mainName) {
            String src_loc = constants.DATA_PREFIX + schema.main + constants.DATA_SUFFIX;
            new CopyManager((BaseConnection) db)
                    .copyIn("copy " + schema.main + " from STDIN with (format csv);",
                            new BufferedReader(new FileReader(src_loc)));
            long start = System.currentTimeMillis();
            for(String branch: constants.descriptor.branches.get(schema.main)) {

                String rep_loc = constants.DATA_PREFIX + branch + constants.REPAIR_DATA_SUFFIX;
                new CopyManager((BaseConnection) db)
                        .copyIn("copy " + branch + " from STDIN with (format csv);",
                                new BufferedReader(new FileReader(rep_loc)));
//                updateTime.put(schema.main, System.currentTimeMillis() - start);
            }
            upd_tme = System.currentTimeMillis() - start;
        }
    }

    @Override
    public void insertDataPrepare(String path, int length, double upd, double delay, double dup, int verbose) throws IOException, SQLException {
        Random rd = new Random();
        Deque<Pair<Integer, String>> delayed = new LinkedList<>();
        Scanner sc = new Scanner(new File(path));
        Map<String, Range> vr = new HashMap<>(); // guarantee unique colname
        List<FileWriter> fws = new ArrayList<>();
        List<FileWriter> fwr = new ArrayList<>();
        for(int i=0;i< constants.descriptor.mainName.size();i++) {
            Schema schema = constants.descriptor.mainName.get(i);
            fws.add(new FileWriter(new File(constants.DATA_PREFIX + schema.main + constants.DATA_SUFFIX)));
            for(String attr: schema.attrName) {
                vr.put(attr, new Range(Double.MAX_VALUE, Double.MIN_VALUE));
            }
            for(String branch: constants.descriptor.branches.get(schema.main)) {
                fwr.add(new FileWriter(new File(constants.DATA_PREFIX + branch + constants.REPAIR_DATA_SUFFIX)));
            }
        }
        sc.nextLine(); // skip header.
        int pos = 0;
        while (sc.hasNext() && pos < length) {
            String line = sc.nextLine();
            String[] s = line.split(",");
            for(int i=0;i< constants.descriptor.mainName.size();i++) {
                Schema schema = constants.descriptor.mainName.get(i);
                String ins = "insert into " + schema.main + " values (";
                StringBuilder value = new StringBuilder();
                StringBuilder repair = new StringBuilder();
                value.append(constants.descriptor.gen.get(i) * pos);
                repair.append(constants.descriptor.gen.get(i) * pos);
                int minus = 0;
                for(String attr: schema.attributes.keySet()) {
                    String reads = "0";
                    if(rd.nextDouble() < param) {
                        minus = 1;
                        continue;
                    }
                    if(s.length > schema.attributes.get(attr)) reads = s[schema.attributes.get(attr)];
                    if(reads.isEmpty()) reads = "0";
                    if(schema.types.get(attr) == Type.INT) {
                        int x = Integer.parseInt(reads);
                        vr.get(attr).update(x);
                        repair.append(", ").append(x + 1);
                        value.append(", ").append(reads);
                    } else {
                        float x = Float.parseFloat(reads);
                        vr.get(attr).update(x);
                        repair.append(", ").append(String.format("%.4f", x*2.0f));
                        value.append(", ").append(String.format("%.4f", x));
                    }
                }
                //value.append(") on conflict (time) do nothing;");
                //repair.append(") on conflict (time) do nothing;");
                if((verbose & 1) == 1 && minus == 0) {
                    if(rd.nextDouble() < delay) {
                        delayed.add(new Pair<>(i, value + "\n"));
                    } else {
                        fws.get(i).write(value + "\n");
//                        Statement st = db.createStatement();
//                        st.executeUpdate(ins + value);
//                        insert_stmt.add(ins + value);
                    }
                }
                //else System.out.println(ins+value);
                if(rd.nextDouble() < upd && minus == 0) {
                    fwr.get(i).write(repair + "\n");
//                    String updateTablex = SchemaDescriptor.random_select_update_table(constants.descriptor.branches.get(schema.main));
//                    String inu = "insert into " + updateTablex;
//                    upd_stmt.add(inu + repair);
                }
                if(rd.nextDouble() < dup) {
                    if((verbose & 1) == 1) {
//                        fws.get(i).write(value + "\n");
//                        Statement st = db.createStatement();
//                        st.executeUpdate(ins + value);
//                        insert_stmt.add(ins + value);
                    }
                    else System.out.println(ins+value);
                }
            }
            pos++;
        }
        for(int i=0;i< constants.descriptor.mainName.size();i++) {
            Schema schema = constants.descriptor.mainName.get(i);
            constants.timeRange.put(schema.main, Range.createRange(0, constants.descriptor.gen.get(i) * pos));
        }
        while(!delayed.isEmpty()) {
            Pair<Integer, String> exec = delayed.pollFirst();
            if((verbose & 1) == 1) {
                fws.get(exec.left).write(exec.right);
//                Statement st = db.createStatement();
//                st.executeUpdate(exec);
//                insert_stmt.add(exec);
            }
            else System.out.println(exec);
        }
        for(int i=0;i< constants.descriptor.mainName.size();i++) {
            fws.get(i).close();
            fwr.get(i).close();
        }
        constants.valueRange = vr;
    }

    @Override
    public void insertDataPrepare_gene(int length, double upd, double delay, double dup, int verbose) throws SQLException, IOException {
        Random rd = new Random();
        Deque<Pair<Integer, String>> delayed = new LinkedList<>();
        Map<String, Range> vr = new HashMap<>(); // guarantee unique colname
        List<FileWriter> fws = new ArrayList<>();
        List<FileWriter> fwr = new ArrayList<>();
        for(int i=0;i< constants.descriptor.mainName.size();i++) {
            Schema schema = constants.descriptor.mainName.get(i);
            fws.add(new FileWriter(new File(constants.DATA_PREFIX + schema.main + constants.DATA_SUFFIX)));
            for(String attr: schema.attrName) {
                vr.put(attr, new Range(Double.MAX_VALUE, Double.MIN_VALUE));
            }
            for(String branch: constants.descriptor.branches.get(schema.main)) {
                fwr.add(new FileWriter(new File(constants.DATA_PREFIX + branch + constants.REPAIR_DATA_SUFFIX)));
            }
        }
        int pos = 0;
        while (pos < length) {
            for(int i=0;i< constants.descriptor.mainName.size();i++) {
                Schema schema = constants.descriptor.mainName.get(i);
                String ins = "insert into " + schema.main + " values (";
                StringBuilder value = new StringBuilder();
                StringBuilder repair = new StringBuilder();
                value.append(constants.descriptor.gen.get(i) * (pos + scale_count));
                repair.append(constants.descriptor.gen.get(i) * (pos + scale_count));
                for(String attr: schema.attributes.keySet()) {
                    if(schema.types.get(attr) == Type.INT) {
                        int x = rd.nextInt();
                        vr.get(attr).update(x);
                        repair.append(", ").append(x + 1);
                        value.append(", ").append(x);
                    } else {
                        float x = rd.nextFloat();
                        vr.get(attr).update(x);
                        repair.append(", ").append(String.format("%.4f", x*2.0f));
                        value.append(", ").append(String.format("%.4f", x));
                    }
                }
                //value.append(") on conflict (time) do nothing;");
                //repair.append(") on conflict (time) do nothing;");
                if((verbose & 1) == 1) {
                    if(rd.nextDouble() < delay) {
                        delayed.add(new Pair<>(i, value + "\n"));
                    } else {
                        fws.get(i).write(value + "\n");
//                        Statement st = db.createStatement();
//                        st.executeUpdate(ins + value);
//                        insert_stmt.add(ins + value);
                    }
                }
                else System.out.println(ins+value);
                if(rd.nextDouble() < upd) {
                    fwr.get(i).write(repair + "\n");
//                    String updateTablex = SchemaDescriptor.random_select_update_table(constants.descriptor.branches.get(schema.main));
//                    String inu = "insert into " + updateTablex;
//                    upd_stmt.add(inu + repair);
                }
                if(rd.nextDouble() < dup) {
                    if((verbose & 1) == 1) {
//                        Statement st = db.createStatement();
//                        st.executeUpdate(ins + value);
//                        insert_stmt.add(ins + value);
                    }
                    else System.out.println(ins+value);
                }
            }
            pos++;
        }
        scale_count += pos;
        for(int i=0;i< constants.descriptor.mainName.size();i++) {
            Schema schema = constants.descriptor.mainName.get(i);
            constants.timeRange.put(schema.main, Range.createRange(0, constants.descriptor.gen.get(i) * (pos + scale_count)));
        }
        while(!delayed.isEmpty()) {
            Pair<Integer, String> exec = delayed.pollFirst();
            if((verbose & 1) == 1) {
                fws.get(exec.left).write(exec.right);
//                Statement st = db.createStatement();
//                st.executeUpdate(exec);
//                insert_stmt.add(exec);
            }
            else System.out.println(exec);
        }
        for(int i=0;i< constants.descriptor.mainName.size();i++) {
            fws.get(i).close();
            fwr.get(i).close();
        }
        constants.valueRange = vr;
    }

    @Override
    public void update(List<Schema> main) throws SQLException {
        // merge + series reading. limit branch 1
        for(Schema schema: main) {
            Statement st = db.createStatement();
            st.executeQuery("select m.time, coalesce(u.A, m.A) from " +
                    schema.main + " as m full outer join " + constants.descriptor.branches.get(schema.main).get(0) + " as u;");
            st.close();
        }
    }

    @Override
    public void update(Schema main) throws SQLException {
        Statement st = db.createStatement();
//        st.executeQuery("select m.time, coalesce(u.A, m.A) from " +
//                main.main + " as m full outer join " + constants.descriptor.branches.get(main.main).get(0) + " as u;");
        st.close();
    }

    @Override
    public void align(Schema tableMain1, Schema tableMain2) throws SQLException {
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ")
                .append("(select m.time time1, coalesce(u.%s, m.%s) A from %s m full outer join %s u on m.time=u.time)")
                .append(" join ")
                .append("(select x.time time2, coalesce(y.%s, x.%s) B from %s x full outer join %s y on x.time=y.time)")
                .append(" on time1 = time2 order by time1;");
        String sql = String.format(sb.toString(),
                tableMain1.attrName.get(0), tableMain1.attrName.get(0), tableMain1.main,
                constants.descriptor.branches.get(tableMain1.main).get(0),
                tableMain2.attrName.get(0), tableMain2.attrName.get(0), tableMain2.main,
                constants.descriptor.branches.get(tableMain2.main).get(0));
//        int ub = 0; boolean inFirst = true;
//        if(constants.timeRange.get(tableMain1.main).right > constants.timeRange.get(tableMain2.main).right) {
//            ub = (int)(constants.timeRange.get(tableMain2.main).right); inFirst = false;
//        } else ub = (int)(constants.timeRange.get(tableMain1.main).right);
//        if(inFirst) {
//            sb.append("select * from ")
//                    .append("(select m.time time1, coalesce(u.%s, m.%s) A from %s m left outer join %s u on m.time=u.time where m.time < " + ub + ")")
//                    .append(" join ")
//                    .append("(select x.time time2, coalesce(y.%s, x.%s) B from %s x left outer join %s y on x.time=y.time)")
//                    .append(" on time1 = time2;");
//        } else {
//            sb.append("select * from ")
//                    .append("(select m.time time1, coalesce(u.%s, m.%s) A from %s m left outer join %s u on m.time=u.time)")
//                    .append(" join ")
//                    .append("(select x.time time2, coalesce(y.%s, x.%s) B from %s x left outer join %s y on x.time=y.time where x.time < " + ub + ")")
//                    .append(" on time1 = time2;");
//        }
//        String sql = String.format(sb.toString(),
//                tableMain1.attrName.get(0), tableMain1.attrName.get(0), tableMain1.main,
//                constants.descriptor.branches.get(tableMain1.main).get(0),
//                tableMain2.attrName.get(0), tableMain2.attrName.get(0), tableMain2.main,
//                constants.descriptor.branches.get(tableMain2.main).get(0));
        //System.out.println(sql);
        st.executeQuery(sql);
        st.close();
    }

    @Override
    public void align(List<Schema> tableMain) throws Exception {
        // leave clean.
        //throw new Exception("not implemented.");
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.align(tableMain.get(i), tableMain.get(i+1));
        }
    }

    @Override
    public void rangeFilter(Schema tableMain, double selectivity) throws SQLException {
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        Range timeRange = Range.randomBySelectivity(constants.timeRange.get(tableMain.main), selectivity);
        sb.append("select * from (select m.time time, coalesce(u.%s, m.%s) A")
                .append(" from %s m full outer join %s u on m.time=u.time)")
                .append(" where time > %s and time < %s;");
        String sql = String.format(sb.toString(),
                tableMain.attrName.get(0), tableMain.attrName.get(0), tableMain.main,
                constants.descriptor.branches.get(tableMain.main).get(0),
                String.valueOf(timeRange.left), String.valueOf(timeRange.right));
        //System.out.println(sql);
        st.executeQuery(sql);
        st.close();
    }

    /**
     * run single rangeFilter tableMain.size times.
     * @param tableMain
     * @param selectivity
     * @throws Exception
     */
    @Override
    public void rangeFilter(List<Schema> tableMain, double selectivity) throws Exception {
        // keep clean.
        // throw new Exception("not implemented.");
        for(int i=0;i<tableMain.size();i++) {
            this.rangeFilter(tableMain.get(i), selectivity);
        }
    }

    @Override
    public void valueFilter(Schema tableMain, double selectivity) throws SQLException {
        // now support 1 col.
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        for(String x: tableMain.attrName) {
            // num . attribute == 1
            Range valueRange = Range.randomBySelectivity(constants.valueRange.get(x), selectivity);
            sb.append("select * from ")
                    .append("(select m.time time, coalesce(u.%s, m.%s) A from %s m full outer join %s u on m.time=u.time)")
                    .append(" where A > %s and A < %s;");
            String sql = String.format(sb.toString(),
                    tableMain.attrName.get(0), tableMain.attrName.get(0), tableMain.main,
                    constants.descriptor.branches.get(tableMain.main).get(0),
                    String.valueOf(valueRange.left), String.valueOf(valueRange.right));
            //System.out.println(sql);
            st.executeQuery(sql);
            st.close();
        }
    }

    @Override
    public void valueFilter(List<Schema> tableMain, double selectivity) throws Exception {
        // keep clean.
        //throw new Exception("not implemented.");
        for(Schema schema: tableMain) {
            this.valueFilter(schema, selectivity);
        }
    }

    /**
     * some call resample,
     * down-sample in relational is to count but in ts is to avg by time.
     * @param tableMain
     * @param window
     * @throws SQLException
     */
    @Override
    public void downSample(Schema tableMain, int window) throws SQLException {
        //
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        //st.executeQuery("select m.time time, ceil(m.time/5) grp, coalesce(u.A, m.A) A from climate1 m full outer join climate1b0 u on m.time=u.time;");
        sb.append("select grp, avg(A) from ")
                .append("(select m.time time, ceil(m.time/%s) grp, coalesce(u.%s, m.%s) A " +
                        "from %s m full outer join %s u on m.time=u.time)")
                .append(" group by grp;");
        String sql = String.format(sb.toString(),
                window, tableMain.attrName.get(0), tableMain.attrName.get(0),
                tableMain.main,
                constants.descriptor.branches.get(tableMain.main).get(0));
        //System.out.println(sql);
        st.executeQuery(sql);
        st.close();
    }

    @Override
    public void downSample(List<Schema> tableMain, int window) throws Exception {
        // keep clean.
        //throw new Exception("not implemented.");
        for(Schema schema: tableMain) {
            downSample(schema, window);
        }
    }

    @Override
    public void downSampleAligned(Schema tableMain1, Schema tableMain2, int window) throws SQLException {
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        //st.executeQuery("select m.time time1, m.time/5 grp1, coalesce(u.A, m.A) A from climate1 m full outer join climate1b0 u on m.time=u.time;");
        sb.append("select grp1, avg(A), avg(B) from ")
                .append("((select m.time time1, m.time/%s grp1, coalesce(u.%s, m.%s) A " +
                        "from %s m full outer join %s u on m.time=u.time) join ")
                .append("(select x.time time2, x.time/%s grp2, coalesce(y.%s, x.%s) B " +
                        "from %s x full outer join %s y on x.time=y.time) ")
                .append("on time1=time2) group by grp1;");
        String sql = String.format(sb.toString(),
                window, tableMain1.attrName.get(0), tableMain1.attrName.get(0),
                tableMain1.main,
                constants.descriptor.branches.get(tableMain1.main).get(0),
                window, tableMain2.attrName.get(0), tableMain2.attrName.get(0),
                tableMain2.main,
                constants.descriptor.branches.get(tableMain2.main).get(0));
        //System.out.println(sql);
        st.executeQuery(sql);
        st.close();
    }

    @Override
    public void downSampleAligned(List<Schema> tableMain, int window) throws SQLException {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.downSampleAligned(tableMain.get(i), tableMain.get(i+1), window);
        }
    }

    @Override
    public void downSampleUnion(Schema tableMain1, Schema tableMain2, int window) throws SQLException {
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        sb.append("with R1 as ")
                .append("(select m.time time, coalesce(u.%s, m.%s) A " +
                        "from %s m full outer join %s u on m.time=u.time), R2 as ")
                .append("(select x.time time, coalesce(y.%s, x.%s) A " +
                        "from %s x full outer join %s y on x.time=y.time), R3 as ")
                .append("(select s.time/%s grp, coalesce(p.A, s.A) A from R1 s full outer join R2 p on s.time=p.time)")
                .append("select grp, avg(A) from R3 group by grp;");
        String sql = String.format(sb.toString(),
                tableMain1.attrName.get(0), tableMain1.attrName.get(0),
                tableMain1.main,
                constants.descriptor.branches.get(tableMain1.main).get(0),
                tableMain2.attrName.get(0), tableMain2.attrName.get(0),
                tableMain2.main,
                constants.descriptor.branches.get(tableMain2.main).get(0), window);
        //System.out.println(sql);
        st.executeQuery(sql);
        st.close();
    }

    @Override
    public void downSampleUnion(List<Schema> tableMain, int window) throws SQLException {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.downSampleUnion(tableMain.get(i), tableMain.get(i+1), window);
        }
    }

    @Override
    public void seriesJoinValue(Schema tableMain1, Schema tableMain2) throws SQLException, IOException {
        try {
            Statement stx = db.createStatement();
            stx.executeUpdate("drop table temp;");
        } catch (Exception e) {
            wlog.write(e.toString());
        }
        Statement st = db.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        StringBuilder sb = new StringBuilder();
        sb.append("create unlogged table temp as with R1 as ")
                .append("(select m.time time, coalesce(u.%s, m.%s) A " +
                        "from %s m full outer join %s u on m.time=u.time), R2 as ")
                .append("(select x.time time, coalesce(y.%s, x.%s) A " +
                        "from %s x full outer join %s y on x.time=y.time) ")
                .append("select R1.time t1, R2.time t2, R1.A A1, R2.A A2 from R1, R2 where R1.time=R2.time and R1.A=R2.A;");
        String sql = String.format(sb.toString(),
                tableMain1.attrName.get(0), tableMain1.attrName.get(0),
                tableMain1.main,
                constants.descriptor.branches.get(tableMain1.main).get(0),
                tableMain2.attrName.get(0), tableMain2.attrName.get(0),
                tableMain2.main,
                constants.descriptor.branches.get(tableMain2.main).get(0));
//        sb.append("with UPD(time1, time2, A, B) as (select * from %s m, %s n where ceil(m.%s)=ceil(n.%s)),")
//                .append("JOIN1(time1, A1) as (select l.time, coalesce(UPD.A, l.%s) A from (%s l left join UPD on UPD.time1=l.time)),")
//                .append("JOIN2(time2, B2) as (select l.time, coalesce(UPD.B, l.%s) B from (%s l left join UPD on UPD.time2=l.time))")
//                .append("select * from JOIN1, JOIN2 where ceil(JOIN1.A1)=ceil(JOIN2.B2)");
//        String sql = String.format(sb.toString(),
//                constants.descriptor.branches.get(tableMain1.main).get(0),
//                constants.descriptor.branches.get(tableMain2.main).get(0),
//                tableMain1.attrName.get(0), tableMain2.attrName.get(0),
//                tableMain1.attrName.get(0),
//                tableMain1.main,
//                tableMain2.attrName.get(0),
//                tableMain2.main,
//                tableMain1.attrName.get(0), tableMain2.attrName.get(0));
        //db.prepareStatement(sql).executeQuery();
        st.setFetchSize(0);
        st.executeUpdate(sql);
        st.close();
    }

    @Override
    public void seriesJoinValue(List<Schema> tableMain) throws SQLException, IOException {
        for(int i=0;i<tableMain.size()-1;i+=2) {
            this.seriesJoinValue(tableMain.get(i), tableMain.get(i+1));
        }
    }

    @Override
    public void branchAlign(Schema tableMain1, Schema tableMain2) throws SQLException {
        // use List instead.
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ")
                .append("(select m.time time1, coalesce(u.%s, m.%s) A from %s m full outer join %s u on m.time=u.time)")
                .append(" join ")
                .append("(select x.time time2, coalesce(y.%s, x.%s) B from %s x full outer join %s y on x.time=y.time)")
                .append(" on time1 = time2;");
        String sql = String.format(sb.toString(),
                tableMain1.attrName.get(0), tableMain1.attrName.get(0), tableMain1.main,
                constants.descriptor.branches.get(tableMain1.main).get(0),
                tableMain2.attrName.get(0), tableMain2.attrName.get(0), tableMain2.main,
                constants.descriptor.branches.get(tableMain2.main).get(0));
//        sb.append("select * from ")
//                .append(constants.descriptor.branches.get(tableMain1.main).get(0))
//                .append(" natural join ")
//                .append(constants.descriptor.branches.get(tableMain2.main).get(0))
//                .append(";");
        st.executeQuery(sql);
    }

    @Override
    public void branchAlign(List<Schema> tableMain) throws SQLException {
        Statement st = db.createStatement();
        StringBuilder sb = new StringBuilder();
        sb.append("with ");
        for(int i=0;i<tableMain.size();i++) {
            Schema schema = tableMain.get(i);
            String s = String.format(
                    " R%s as (select m.time time1, coalesce(u.%s, m.%s) A from %s m full outer join %s u on m.time=u.time),",
                    i, schema.attrName.get(0), schema.attrName.get(0),
                    schema.main, constants.descriptor.branches.get(schema.main).get(0));
            sb.append(s);
        }
        sb.deleteCharAt(sb.length()-1); // remove ,
        sb.append(" select * from R0 ");
        for(int i=1;i<tableMain.size();i++) {
            //Schema schema = tableMain.get(i);
            sb.append(" natural join R" + i);
        }
        sb.append(";");
        //System.out.println(sb.toString());
        st.executeQuery(sb.toString());
    }

    @Override
    public void alignPartialReading(List<Schema> tableMain, int attrs) throws SQLException, IoTDBConnectionException, StatementExecutionException {

    }

    @Override
    public void execute() throws Exception {
        long rat = System.currentTimeMillis()%10000;
        String fres = constants.RES_PREFIX + "Postgres-" + rat + constants.dataset + constants.RES_POSTFIX;
        String flog = constants.LOG_PREFIX + "Postgres-" + rat + constants.dataset + constants.LOG_POSTFIX;
        FileWriter wres = new FileWriter(new File(fres));
        wlog = new FileWriter(new File(flog));
        System.out.println("Saved as " + fres + " " + System.currentTimeMillis());
        for(BenchFunctions bf: constants.BENCHMARK_CODE) {
            wlog.write(bf.name() + " " + System.currentTimeMillis());
            int ti = 1;
//            if(bf == BenchFunctions.VALUE_JOIN) ti = 2;
            long now = System.currentTimeMillis();
            for(int i=0;i<ti;i++) {
                switch (bf) {
                    case ALIGN: align(constants.descriptor.mainName); break;
                    case VALUE_FILTER: valueFilter(constants.descriptor.mainName, constants.SELECTIVITY); break;
                    case RANGE_FILTER: rangeFilter(constants.descriptor.mainName, constants.SELECTIVITY); break;
                    case DOWNSAMPLE: downSample(constants.descriptor.mainName, constants.WINDOW); break;
                    case DOWNSAMPLE_ALIGN: downSampleAligned(constants.descriptor.mainName, constants.WINDOW); break;
                    case DOWNSAMPLE_UNION: downSampleUnion(constants.descriptor.mainName, constants.WINDOW); break;
                    case VALUE_JOIN: seriesJoinValue(constants.descriptor.mainName); break;
                    case BRANCH_ALIGN: branchAlign(constants.descriptor.mainName); break;
                    default:
                }
            }
            long cost = (System.currentTimeMillis() - now)/ti;// + upd_tme;
            System.out.println(cost);
            wres.write(String.valueOf(cost)+ " ");
        }
        wlog.close();
        wres.close();
    }

    @Override
    public void execute(BenchFunctions benchFunctions) throws Exception {

    }

    public static void benchmarking() throws Exception {
        Postgres pg = new Postgres();
        pg.init(1, DataSet.Climate, "Postgres");
        try {
            pg.clean();
        } catch (Exception e) {
            System.err.println(e);
            //System.exit(-1);
        }
//        pg.create();
        for(double ratx: new double[]{0.50, 0.75, 0.875, 0.9375}) {
            pg.create();
            pg.param = ratx;
            pg.insertDataPrepare(pg.constants.getDatasetPath(), Integer.MAX_VALUE,
                    0.10, 0, 0.0, 1);
            pg.insertCSV();
            pg.execute();
            try {
                pg.clean();
            } catch (Exception e) {
                System.err.println(e);
                //System.exit(-1);
            }
        }
        try {
//            pg.clean();

            //Integer.MAX_VALUE
//            pg.insertDataPrepare(pg.constants.getDatasetPath(), Integer.MAX_VALUE, 0.10, 0, 0, 1);
//            for(int i=0;i<10;i++) {
//                pg.insertDataPrepare_gene(10000000, 0.10, 0, 0, 1);
//                pg.insertCSV();
//                pg.execute();
//            }
        } catch (Exception e) {
//            pg.clean();
            System.err.println(e);
//            System.exit(-1);
        }
//        pg.execute();
//        pg.clean();
    }


    public static void main(String[] args) throws Exception {
        benchmarking();
        //pg.clean();
        Postgres pg = new Postgres();
        pg.init(1, DataSet.Climate, "Postgres");
        try {
            pg.clean();
        } catch (Exception e) {
            System.err.println(e);
            //System.exit(-1);
        }
    }
}
