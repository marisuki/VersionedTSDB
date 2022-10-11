//package org.apache.iotdb.db.query.reader.series;

//import org.apache.iotdb.tsfile.utils.BloomFilter;

public class TSIndex {
  //private BloomFilter bf;
  public long minimalTS = 0x7f7f7f7f, maxTS = -1;
  private long initialTS;

  public TSIndex(int indexedTimestampSize, long initTS) {
    int logSize = (int) Math.max(10, Math.log(indexedTimestampSize));
    //bf = BloomFilter.getEmptyBloomFilter(0.05, indexedTimestampSize);
    initialTS = initTS;
    minimalTS = initTS;
    maxTS = initTS;
  }

  public void add(Long timestamp) {
    minimalTS = Math.min(minimalTS, timestamp);
    //bf.add(timestamp.toString());
  }

  public void setInitialTS(Long initialTS) {
    this.initialTS = initialTS;
  }

  public void addByBias(Long Bias) {
    if (initialTS == -1) System.exit(-1);
    if (Bias < 0) {
      minimalTS = Math.min(minimalTS, initialTS + Bias);
    } else {
      maxTS = Math.max(maxTS, initialTS + Bias);
    }
    Long res = (initialTS + Bias);
    //bf.add(res.toString());
  }

  public boolean contains(Long timestamp) {
    if (timestamp < minimalTS || timestamp > maxTS) return false;
    //else return bf.contains(timestamp.toString());
    return true;
  }

  public boolean withinRange(Long timestamp) {
    return timestamp >= minimalTS && timestamp <= maxTS;
  }
}
