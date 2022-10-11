import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class postgre {

    public Connection init(int conn) throws SQLException {
        String url = "jdbc:postgresql://localhost:49153/db1";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "postgrespw");
        //props.setProperty("ssl", "true");
        Connection db = DriverManager.getConnection(url, "postgres", "postgrespw");
        //Connection connect = DriverManager.getConnection(url, props);
        return db;
    }

    public Connection initTimescale(int conn) throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/db1";
        //Properties props = new Properties();
        //props.setProperty("user", "postgres");
        //props.setProperty("password", "postgrespw");
        //props.setProperty("ssl", "true");
        Connection db = DriverManager.getConnection(url, "postgres", "password");
        //Connection connect = DriverManager.getConnection(url, props);
        return db;
    }

    public List<Long> loadScalabilityOriginal(Connection connection) throws SQLException, IOException {
        String pathPrefix = "./dataset/climate_sc/t_70rat=0.";
        //String[] load = {};
        List<Long> ans = new ArrayList<>();
        for(int i=1;i<=10;i++) {
            //File f = new File(pathPrefix + i + ".csv");
            String fileLoc = pathPrefix + i + ".csv";
            long curr = System.currentTimeMillis();
            File f_upd = new File(pathPrefix + i + "-upd.csv");
            Statement st = connection.createStatement();
            String tableName = "t_70sel"+i;
            st.executeUpdate("create table " + tableName + "(T bigint, A float, primary key(T));");
            long rowsInserted = new CopyManager((BaseConnection) connection)
                    .copyIn("copy " + tableName + " from STDIN with (format csv);",
                            new BufferedReader(new FileReader(fileLoc)));
            Scanner sc = new Scanner(f_upd);
            while(sc.hasNext()) {
                String[] parse = sc.nextLine().split(",");
                long time = Long.parseLong(parse[0]);//.parseInt(parse[0]);
                float val = Float.parseFloat(parse[1]);
                String sql = "update " + tableName + " set A=? where T=?;";
                PreparedStatement pstmt = connection.prepareStatement(sql);
                pstmt.setFloat(1, val);
                pstmt.setLong(2, time);
            }
            ans.add(System.currentTimeMillis() - curr);
            System.out.println(System.currentTimeMillis() - curr);
        }
        return ans;
    }

    public List<Long> loadScalabilityVersion(Connection connection) throws SQLException, IOException {
        String pathPrefix = "./dataset/climate_sc/t_70rat=0.";
        //String[] load = {};
        List<Long> ans = new ArrayList<>();
        for(int i=1;i<=10;i++) {
            //File f = new File(pathPrefix + i + ".csv");
            String fileLoc = pathPrefix + i + "-ver.csv";
            long curr = System.currentTimeMillis();
            //File f_upd = new File(pathPrefix + i + "-upd.csv");
            Statement st = connection.createStatement();
            String tableName = "t_70ver"+i;
            st.executeUpdate("create table " + tableName + "(T bigint, V int, A float);");
            long rowsInserted = new CopyManager((BaseConnection) connection)
                    .copyIn("copy " + tableName + " from STDIN with (format csv);",
                            new BufferedReader(new FileReader(fileLoc)));
            st.executeUpdate("create index on " + tableName + "(T ASC);");
            ans.add(System.currentTimeMillis() - curr);
            System.out.println(System.currentTimeMillis() - curr);
        }
        return ans;
    }

    public static void persistAns(List<Long> res, String file) throws IOException {
        //for(Long x: res) System.out.print(x);
        //System.out.println();
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        for(Long x: res) writer.write(String.valueOf(x) + " ");
        writer.write("\n");
        writer.close();
    }

    public void loadAnother(String path, String tableName, Connection connection) throws SQLException, IOException {
        Statement st = connection.createStatement();
        st.executeUpdate("create table " + tableName + "(T bigint, A float, primary key(T));");
        long rowsInserted = new CopyManager((BaseConnection) connection)
                .copyIn("copy " + tableName + " from STDIN with (format csv);",
                        new BufferedReader(new FileReader(path)));
    }

    public void loadAnotherVer(String path, String tableName, Connection connection) throws SQLException, IOException {
        Statement st = connection.createStatement();
        st.executeUpdate("create table " + tableName + "(T bigint, V int, A float);");
        long rowsInserted = new CopyManager((BaseConnection) connection)
                .copyIn("copy " + tableName + " from STDIN with (format csv);",
                        new BufferedReader(new FileReader(path)));
    }


    public void loadingTimeCompare(Connection conn) throws IOException, SQLException {
        //Loading and compare: climate
        List<Long> ans1 = this.loadScalabilityOriginal(conn);
        // require clear the cache
        // Linux: /etc/....cache rm. /Windows: restart
        List<Long> ans2 = this.loadScalabilityVersion(conn);
        persistAns(ans1, "./result/relts_original_scala.dat");
        persistAns(ans2, "./result/relts_version_scala.dat");
    }

    public void loadSufficientData(Connection conn) throws SQLException, IOException {
        loadAnother("./dataset/climate_s/wdir_70.csv", "wdir", conn);
        loadAnother("./dataset/climate_s/rhoair_70.csv", "rhoair", conn);
        loadAnotherVer("./dataset/climate_s/wdir_70-ver.csv", "wdirver", conn);
        loadAnotherVer("./dataset/climate_s/rhoair_70-ver.csv", "rhoairver", conn);
    }

    public void createMaxViewVersion(Connection conn) throws SQLException {
        String nameLeftPrefix = "t_70ver";
        for(int i=1;i<=10;i++) {
            Statement st = conn.createStatement();
            String currTable = nameLeftPrefix + i;
            String version = currTable + "vm";
            st.executeUpdate("create table " + version + " as (select T as T1, max(V) as maxV from " + currTable +" group by T);");
        }
    }

    public List<Long> performanceQ1Align2SerVersion(Connection conn) throws SQLException {
        // remember to restart psql to clear sql cache.
        // scalability
        String nameLeftPrefix = "t_70sel";
        String nameRight1 = "rhoair";
        String nameRight2 = "wdir";
        List<Long> ans = new ArrayList<>();
        for(int i=1;i<=10;i++) {
            long curr = System.currentTimeMillis();
            Statement st = conn.createStatement();
            String nameLeft = "t_70ver" + i;
            String nameLeftVM = nameLeft + "vm";
            st.executeQuery("select " + nameLeft  + ".T from (" + nameLeftVM + " JOIN " + nameLeft + " ON " + nameLeftVM + ".T1=" + nameLeft +".T and "+ nameLeftVM + ".maxV=" + nameLeft +".V)" + " JOIN "+ nameRight1 + " ON " + nameLeft + ".T=" + nameRight1 + ".T;" );
            st.executeQuery("select " + nameLeft  + ".T from (" + nameLeftVM + " JOIN " + nameLeft + " ON " + nameLeftVM + ".T1=" + nameLeft +".T and "+ nameLeftVM + ".maxV=" + nameLeft +".V)" + " JOIN "+ nameRight2 + " ON " + nameLeft + ".T=" + nameRight2 + ".T;" );
            //st.executeQuery("select Tx from " + nameLeftVM + " JOIN (select " + nameLeft  + ".T as Tx, " + nameLeft + ".V as Vx from " + nameLeft + " JOIN "+ nameRight2 + " ON " + nameLeft + ".T=" + nameRight2 + ".T) as Series(Tx, Vx) on Tx=T1 where maxV=Vx;");
            long cost = System.currentTimeMillis() - curr;
            ans.add(cost/2);
            System.out.println(cost);
        }
        return ans;
    }

    public List<Long> performanceQ1Align2SerVersionOptWeakSel(Connection conn) throws SQLException {
        // remember to restart psql to clear sql cache.
        // scalability
        String nameLeftPrefix = "t_70sel";
        String nameRight1 = "rhoair";
        String nameRight2 = "wdir";
        List<Long> ans = new ArrayList<>();
        for(int i=1;i<=10;i++) {
            long curr = System.currentTimeMillis();
            Statement st = conn.createStatement();
            String nameLeft = "t_70ver" + i;
            String nameLeftVM = nameLeft + "vm";
            // optimize: |\cap| = o(n)
            st.executeQuery("select " + nameLeft  + ".T from (" + nameLeft + " JOIN " + nameRight1 + " ON " + nameLeft + ".T=" + nameRight1 + ".T)" + " JOIN "+ nameLeftVM + " ON " + nameLeftVM + ".T1=" + nameLeft +".T and "+ nameLeftVM + ".maxV=" + nameLeft +".V;" );
            st.executeQuery("select " + nameLeft  + ".T from (" + nameLeft + " JOIN " + nameRight2 + " ON " + nameLeft + ".T=" + nameRight2 + ".T)" + " JOIN "+ nameLeftVM + " ON " + nameLeftVM + ".T1=" + nameLeft +".T and "+ nameLeftVM + ".maxV=" + nameLeft +".V;" );
            //st.executeQuery("select Tx from " + nameLeftVM + " JOIN (select " + nameLeft  + ".T as Tx, " + nameLeft + ".V as Vx from " + nameLeft + " JOIN "+ nameRight2 + " ON " + nameLeft + ".T=" + nameRight2 + ".T) as Series(Tx, Vx) on Tx=T1 where maxV=Vx;");
            long cost = System.currentTimeMillis() - curr;
            ans.add(cost/2);
            System.out.println(cost);
        }
        return ans;
    }

    public List<Long> performanceQ1Align2SerOriginal(Connection conn) throws SQLException, IOException {
        // remember to restart psql to clear sql cache.
        // scalability
        String nameLeftPrefix = "t_70ver";
        String nameRight1 = "rhoair";
        String nameRight2 = "wdir";
        List<Long> ans = new ArrayList<>();
        for(int i=1;i<=10;i++) {
            long curr = System.currentTimeMillis();
            Statement st = conn.createStatement();
            String nameLeft = "t_70sel" + i;
            st.executeQuery("select * from " + nameLeft + " JOIN " + nameRight1 + " ON " + nameLeft + ".T=" + nameRight1 + ".T;");
            st.executeQuery("select * from " + nameLeft + " JOIN " + nameRight2 + " ON " + nameLeft + ".T=" + nameRight2 + ".T;");
            long cost = System.currentTimeMillis() - curr;
            ans.add(cost/2);
            System.out.println(cost/2);
        }
        persistAns(ans, "./result/rel_Q1_postgres-final.txt");
        return ans;
    }

    public List<Long> performanceQ2Align2SerOriginal(Connection conn) throws SQLException, IOException {
        // value filtering , varying selectivity
        // remember to restart psql to clear sql cache.
        // scalability
        String prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
        //String nameLeftPrefix = "t_70ver";
        String nameLeftPrefix = "t_70sel";
        String nameRight1 = "rhoair";
        String nameRight2 = "wdir";
        List<Long> ans = new ArrayList<>();
        statistic s = new statistic();

        List<Double> pivots = s.findPivot(s.stat(prefix + 5 + ".csv"), s.cntx);
        for(int i=1;i<=10;i++) {
            Statement st = conn.createStatement();
            //String nameLeft = "t_70sel" + i;
            String nameLeft = nameLeftPrefix + 5;
            String nameAlias = nameLeft + "alia";
            String nameLeftVM = nameLeft + "vm";
            long curr = System.currentTimeMillis();
            //st.executeQuery("select * from " + nameLeft + " JOIN " + nameLeftVM + " ON " + nameLeftVM + ".T1=" + nameLeft +".T and "+ nameLeftVM + ".maxV=" + nameLeft +".V " + " where A >= " + pivots.get(0) + " and A <= " + pivots.get(i) + ";");
            st.executeQuery("select * from " + nameLeft + " where A >= " + pivots.get(0) + " and A <= " + pivots.get(i) + ";");
            //st.executeQuery("select * from " + nameLeft + " where A >= " + pivots.get(0) + " and A <= " + pivots.get(i) + ";");
            //st.executeQuery("select * from " + nameLeft + " JOIN " + nameRight2 + " ON " + nameLeft + ".T=" + nameRight2 + ".T;");
            long cost = System.currentTimeMillis() - curr;
            ans.add(cost);
            System.out.println(cost);
        }
        //persistAns(ans, "./result/rel_perform_Q2_select.txt");
        persistAns(ans, "./result/rel_perform_Q2_original_select.txt");
        return ans;
    }

    public List<Long> performanceQ2Align2SerOriginalScala(Connection conn) throws SQLException, IOException {
        // value filtering , varying selectivity
        // remember to restart psql to clear sql cache.
        // scalability
        String prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
        //String nameLeftPrefix = "t_70ver";
        String nameLeftPrefix = "t_70sel";
        String nameRight1 = "rhoair";
        String nameRight2 = "wdir";
        List<Long> ans = new ArrayList<>();

        for(int i=1;i<=10;i++) {
            statistic s = new statistic();
            List<Double> pivots = s.findPivot(s.stat(prefix + i + ".csv"), s.cntx);
            Statement st = conn.createStatement();
            //String nameLeft = "t_70sel" + i;
            String nameLeft = nameLeftPrefix + i;
            String nameAlias = nameLeft + "alia";
            String nameLeftVM = nameLeft + "vm";
            long curr = System.currentTimeMillis();
            st.executeQuery("select * from " + nameLeft  + " where A >= " + pivots.get(0) + " and A <= " + pivots.get(5) + ";");
            //st.executeQuery("select * from " + nameLeft + " JOIN " + nameRight2 + " ON " + nameLeft + ".T=" + nameRight2 + ".T;");
            long cost = System.currentTimeMillis() - curr;
            ans.add(cost);
            System.out.println(cost);
        }
        //persistAns(ans, "./result/rel_perform_Q2_scala.txt");
        persistAns(ans, "./result/rel_perform_Q2_original_scala.txt");
        return ans;
    }

    public List<Long> performanceQ3Align2SerOriginal(Connection conn) throws SQLException, IOException {
        // value filtering , varying selectivity
        // remember to restart psql to clear sql cache.
        // scalability
        String prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
        //String nameLeftPrefix = "t_70ver";
        String nameLeftPrefix = "t_70sel";
        String nameRight1 = "rhoair";
        String nameRight2 = "wdir";
        List<Long> ans = new ArrayList<>();
        statistic s = new statistic();

        List<Long> pivots = s.findPivotTime(prefix + 5 + ".csv");
        for(int i=1;i<=10;i++) {
            Statement st = conn.createStatement();
            //String nameLeft = "t_70sel" + i;
            String nameLeft = nameLeftPrefix + 5;
            String nameAlias = nameLeft + "alia";
            String nameLeftVM = nameLeft + "vm";
            long curr = System.currentTimeMillis();
            //st.executeQuery("select * from " + nameLeft + " JOIN " + nameLeftVM + " ON " + nameLeftVM + ".T1=" + nameLeft +".T and "+ nameLeftVM + ".maxV=" + nameLeft +".V " + " where T >= " + pivots.get(0) + " and T <= " + pivots.get(i) + ";");
            st.executeQuery("select * from " + nameLeft + " where T >= " + pivots.get(0) + " and T <= " + pivots.get(i) + ";");
            //st.executeQuery("select * from " + nameLeft + " JOIN " + nameRight2 + " ON " + nameLeft + ".T=" + nameRight2 + ".T;");
            long cost = System.currentTimeMillis() - curr;
            ans.add(cost);
            System.out.println(cost);
        }
        //persistAns(ans, "./result/rel_perform_Q3_select.txt");
        persistAns(ans, "./result/rel_perform_Q3_original_select.txt");
        return ans;
    }

    public List<Long> performanceQ3Align2SerOriginalScala(Connection conn) throws SQLException, IOException {
        // value filtering , varying selectivity
        // remember to restart psql to clear sql cache.
        // scalability
        String prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
        //String nameLeftPrefix = "t_70ver";
        String nameLeftPrefix = "t_70sel";
        String nameRight1 = "rhoair";
        String nameRight2 = "wdir";
        List<Long> ans = new ArrayList<>();

        for(int i=1;i<=10;i++) {
            statistic s = new statistic();
            List<Long> pivots = s.findPivotTime(prefix + i + ".csv");
            Statement st = conn.createStatement();
            //String nameLeft = "t_70sel" + i;
            String nameLeft = nameLeftPrefix + i;
            String nameAlias = nameLeft + "alia";
            String nameLeftVM = nameLeft + "vm";
            long curr = System.currentTimeMillis();
            //st.executeQuery("select * from " + nameLeft + " JOIN " + nameLeftVM + " ON " + nameLeftVM + ".T1=" + nameLeft +".T and "+ nameLeftVM + ".maxV=" + nameLeft +".V " + " where T >= " + pivots.get(0) + " and T <= " + pivots.get(5) + ";");
            st.executeQuery("select * from " + nameLeft + " where T >= " + pivots.get(0) + " and T <= " + pivots.get(5) + ";");
            //st.executeQuery("select * from " + nameLeft + " JOIN " + nameRight2 + " ON " + nameLeft + ".T=" + nameRight2 + ".T;");
            long cost = System.currentTimeMillis() - curr;
            ans.add(cost);
            System.out.println(cost);
        }
        //persistAns(ans, "./result/rel_perform_Q3_scala.txt");
        persistAns(ans, "./result/rel_perform_Q3_original_scala.txt");
        return ans;
    }

    public List<Long> performanceQ4Align2SerOriginalScala(Connection conn) throws SQLException, IOException {
        // value filtering , varying selectivity
        // remember to restart psql to clear sql cache.
        // scalability
        // A*A is yto avoid pre-computation
        String prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
        //String nameLeftPrefix = "t_70ver";
        String nameLeftPrefix = "t_70sel";
        String nameRight1 = "rhoair";
        String nameRight2 = "wdir";
        List<Long> ans = new ArrayList<>();

        for(int i=1;i<=10;i++) {
            statistic s = new statistic();
            List<Long> pivots = s.findPivotTime(prefix + i + ".csv");
            Statement st = conn.createStatement();
            //String nameLeft = "t_70sel" + i;
            String nameLeft = nameLeftPrefix + i;
            String nameAlias = nameLeft + "alia";
            String nameLeftVM = nameLeft + "vm";
            long curr = System.currentTimeMillis();
            //st.executeQuery("select T, sum(A*A) over w from " + nameLeft + " JOIN " + nameLeftVM + " ON " + nameLeftVM + ".T1=" + nameLeft +".T and "+ nameLeftVM + ".maxV=" + nameLeft +".V " + " window w as (ORDER BY T RANGE BETWEEN 60 PRECEDING AND CURRENT ROW);");
            st.executeQuery("select T, sum(A*A) over w from " + nameLeft + " window w as (ORDER BY T RANGE BETWEEN 60 PRECEDING AND CURRENT ROW);");
            //st.executeQuery("select * from " + nameLeft + " JOIN " + nameRight2 + " ON " + nameLeft + ".T=" + nameRight2 + ".T;");
            long cost = System.currentTimeMillis() - curr;
            ans.add(cost);
            System.out.println(cost);
        }
        //persistAns(ans, "./result/rel_perform_Q4_scala.txt");
        persistAns(ans, "./result/rel_perform_Q4_original_scala.txt");
        return ans;
    }

    public List<Long> performanceQ5Align2SerOriginalScala(Connection conn) throws SQLException, IOException {
        // value filtering , varying selectivity
        // remember to restart psql to clear sql cache.
        // scalability
        // A*A is yto avoid pre-computation
        String prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
        //String nameLeftPrefix = "t_70ver";
        String nameLeftPrefix = "t_70sel";
        String nameRight1 = "rhoair";
        String nameRight2 = "wdir";
        List<Long> ans = new ArrayList<>();

        for(int i=1;i<=9;i++) {
            //statistic s = new statistic();
            //List<Long> pivots = s.findPivotTime(prefix + i + ".csv");
            Statement st = conn.createStatement();
            //String nameLeft = "t_70sel" + i;
            String nameLeft = nameLeftPrefix + i;
            String nameAlias = nameLeft + "alia";
            String nameLeftVM = nameLeft + "vm";
            long curr = System.currentTimeMillis();
            //st.executeQuery("select T, sum(A) over w from " + nameLeft + " JOIN " + nameLeftVM + " ON " + nameLeftVM + ".T1=" + nameLeft +".T and "+ nameLeftVM + ".maxV=" + nameLeft +".V " + " window w as (ORDER BY T RANGE BETWEEN 60 PRECEDING AND CURRENT ROW);");
            st.executeQuery("select T, sum(A*A) over w from " + nameLeft + " window w as (ORDER BY T RANGE BETWEEN 60 PRECEDING AND CURRENT ROW);");
            nameLeft = nameLeftPrefix + (i+1);
            nameLeftVM = nameLeft + "vm";
            //st.executeQuery("select T, sum(A) over w from " + nameLeft + " JOIN " + nameLeftVM + " ON " + nameLeftVM + ".T1=" + nameLeft +".T and "+ nameLeftVM + ".maxV=" + nameLeft +".V " + " window w as (ORDER BY T RANGE BETWEEN 60 PRECEDING AND CURRENT ROW);");
            st.executeQuery("select T, sum(A*A) over w from " + nameLeft + " window w as (ORDER BY T RANGE BETWEEN 60 PRECEDING AND CURRENT ROW);");
            //st.executeQuery("select * from " + nameLeft + " JOIN " + nameRight2 + " ON " + nameLeft + ".T=" + nameRight2 + ".T;");
            long cost = System.currentTimeMillis() - curr;
            ans.add(cost);
            System.out.println(cost);
        }
        //persistAns(ans, "./result/rel_perform_Q5_scala.txt");
        persistAns(ans, "./result/rel_perform_Q5_original_scala.txt");
        return ans;
    }

    public static void main(String[] args) throws SQLException, IOException {
        postgre pg = new postgre();
        Connection conn = pg.init(1);
        //Connection conn = pg.initTimescale(1);
        Statement st = conn.createStatement();

        //climate
        //pg.loadingTimeCompare(conn);
        //pg.loadSufficientData(conn);
        //pg.createMaxViewVersion(conn);

        // query performance test
        // remember to restart psql to clear sql cache.
        pg.performanceQ5Align2SerOriginalScala(conn);
        //pg.persistAns(ans, "./result/rel_perform_Q1_original_scala4.txt");
        //List<Long> ans = pg.performanceQ1Align2SerVersion(conn);
        //pg.persistAns(ans, "./result/relts_perform_Q1_version_scala1.txt");

        // basic tests
        //st.executeUpdate("create table test2(T int, A float, primary key(T));");
        //st.executeUpdate("insert into test values (12,1, 2.0);");
        //long rowsInserted = new CopyManager((BaseConnection) conn)
        //        .copyIn("copy test2 from STDIN with (format csv);",
        //                new BufferedReader(new FileReader("D://GitHub/relationversion/dataset/test.csv")));
        //String sql = "update test2 set A=? where T=?;";
        //PreparedStatement pstmt = conn.prepareStatement(sql);
        //pstmt.setString(1, "test2");
        //pstmt.setFloat(1, 23);
        //pstmt.setInt(2, 1);
        //pstmt.executeUpdate();
        //ResultSet rs = st.executeQuery("select * from test;");
        //System.out.println(rs.getMetaData());
    }
}
