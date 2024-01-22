import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public abstract class benchmarks {
    public abstract void create() throws SQLException, IoTDBConnectionException, StatementExecutionException;
    public abstract void clean() throws SQLException, IoTDBConnectionException, StatementExecutionException;
    public abstract void init(int num_branch, DataSet dataset, String ev) throws SQLException, IoTDBConnectionException;
    public abstract void insert() throws SQLException;
    public abstract void insertCSV() throws SQLException, IOException;
    public abstract void insertDataPrepare(String path, int length, double upd, double delay, double dup, int verbose) throws Exception;
    public abstract void insertDataPrepare_gene(int length, double upd, double delay, double dup, int verbose) throws Exception;
    public abstract void update(List<Schema> main) throws SQLException, IoTDBConnectionException, StatementExecutionException;
    public abstract void update(Schema main) throws SQLException, IoTDBConnectionException, StatementExecutionException;
    public abstract void align(Schema tableMain1, Schema tableMain2) throws SQLException, IoTDBConnectionException, StatementExecutionException;
    public abstract void align(List<Schema> tableMain) throws Exception;
    public abstract void rangeFilter(Schema tableMain, double selectivity) throws SQLException, IoTDBConnectionException, StatementExecutionException;
    public abstract void rangeFilter(List<Schema> tableMain, double selectivity) throws Exception;
    public abstract void valueFilter(Schema tableMain, double selectivity) throws SQLException, IoTDBConnectionException, StatementExecutionException;
    public abstract void valueFilter(List<Schema> tableMain, double selectivity) throws Exception;
    public abstract void downSample(Schema tableMain, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException;
    public abstract void downSample(List<Schema> tableMain, int window) throws Exception;
    public abstract void downSampleAligned(Schema tableMain1, Schema tableMain2, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException;
    public abstract void downSampleAligned(List<Schema> tableMain, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException;
    public abstract void downSampleUnion(Schema tableMain1, Schema tableMain2, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException;
    public abstract void downSampleUnion(List<Schema> tableMain, int window) throws SQLException, IoTDBConnectionException, StatementExecutionException;
    public abstract void seriesJoinValue(Schema tableMain1, Schema tableMain2) throws SQLException, IOException, IoTDBConnectionException, StatementExecutionException;
    public abstract void seriesJoinValue(List<Schema> tableMain) throws SQLException, IOException, IoTDBConnectionException, StatementExecutionException;
    public abstract void branchAlign(Schema tableMain1, Schema tableMain2) throws SQLException;
    public abstract void branchAlign(List<Schema> tableMain) throws SQLException, IoTDBConnectionException, StatementExecutionException;

    public abstract void alignPartialReading(List<Schema> tableMain, int attrs) throws SQLException, IoTDBConnectionException, StatementExecutionException, IOException;
    public abstract void execute() throws Exception;
    public abstract void execute(BenchFunctions benchFunctions) throws Exception;
}
