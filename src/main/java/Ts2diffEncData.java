//package org.apache.iotdb.tsfile.file.metadata.statistics;

public class Ts2diffEncData {
    public long start;
    public int delta;
    public int length;

    public Ts2diffEncData(long st, int delta, int len) {
        this.start = st;
        this.delta = delta;
        this.length = len;
    }

    /*
    public boolean exist(int timestampBias) {
        return (timestampBias >= stBias && timestampBias <= edBias)
                && ((int) ((timestampBias - stBias) / delta)) * delta == timestampBias;
    }

    public boolean isOverlap(int stBias, int edBias) {
        return (stBias <= edBias) || (edBias >= stBias);
    }
*/
    // public int overlappedLen()

}
