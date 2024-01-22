import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class IoTDBVConcurrent extends IoTDB {
    int concurrent = 2;
    long cost_each = 0;

    List<String> labelGarbage = new ArrayList<>();
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
        // time for each concurrent
        long total = 0;
        for(int i=0;i<tableMain.size()-concurrent;i++) {
            Schema schema = tableMain.get(i);
            for(int j=1;j<concurrent;j++) {
                Schema schema1 = tableMain.get(i+j);

            }
//
//            String sql;
//            //System.out.println("V");
//            long now = System.currentTimeMillis();
//            session.executeQueryStatement(sql);
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
        System.out.println(sql);
        session.executeQueryStatement(sql);
    }
}
