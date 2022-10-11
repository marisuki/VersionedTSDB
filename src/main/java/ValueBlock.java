//package org.apache.iotdb.db.query.reader.series;

//import org.apache.iotdb.tsfile.file.metadata.statistics.Ts2diffEncData;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

// template T implements Serializable
public class ValueBlock {

  private List<Long> timeBuffer = new ArrayList<>();
  private List<Double> valueBuffer = new ArrayList<>();
  private List<Ts2diffEncData> encTime = new ArrayList<>();
  // private List<Double> values = new ArrayList<>();
  private boolean useEnc = false;
  private final int maximum_size;
  private boolean hasPersisted = false;
  private String persistedFileName;
  private long initialTS = System.currentTimeMillis();
  private long minimalTS;
  private long maxTS;
  private long assignedTS;
  // private Statistics<Double> stat;
  // private final TSIndex index;

  public ValueBlock(int maxSize) {
    maximum_size = maxSize;
    // index = new TSIndex(maximum_size, initialTS);
  }

  // public ValueBlock(String meta, String val, TSIndex cachedIndex) {
  // index = new TSIndex(maximum_size, -1);
  // this = load(meta, val, cachedIndex);
  // }

  public ValueBlock(List<Ts2diffEncData> encTime, List<Double> values, long startTime, int count) {
    this.encTime = encTime;
    this.valueBuffer = values;
    // this.index = index;
    this.minimalTS = startTime;
    this.useEnc = true;
    this.maximum_size = count;
  }

  public Long getMaxTS() {
    return this.maxTS;
  }

  public boolean isFull() {
    return valueBuffer.size() >= maximum_size;
  }

  public boolean isExistTimestamp(Long timestamp) {
    // if (! index.contains(timestamp)) return false;
    // return valueBuffer.containsKey(timestamp);
    return false;
  }

  public void write(Long timestamp, Double value) {
    timeBuffer.add(timestamp);
    valueBuffer.add(value);
    this.maxTS = Math.max(this.maxTS, timestamp);
    // index.add(timestamp);
  }

  public void update(Long timestamp, Double value) {
    int pos = timeBuffer.lastIndexOf(timestamp);
    valueBuffer.set(pos, value);
  }

  public long getCacheSize() {
    return valueBuffer.size();
  }

  public void merge(List<TSTuple> mayOverlap) {
    // Stream<Long> filtered = keys.stream().filter(this::isExistTimestamp);
    // List<TSTuple> filtered =
    // for(Long ts: mayOverlap.keySet()) {
    //    if(isExistTimestamp(ts)) update(ts, mayOverlap.get(ts));
    // else if(index.withinRange(ts)) write(ts, mayOverlap.get(ts));
    // }
  }

  public String[] serialize() {
    /*
    List<Long> ts = timeBuffer;
    ts.sort(Comparator.naturalOrder());
    StringBuilder meta= new StringBuilder();
    StringBuilder val= new StringBuilder();
    Long st = ts.get(0);
    meta.append(st).append("|");
    List<Integer> lst = new ArrayList<Integer>();
    val.append(valueBuffer.get(st));
    for(int i=1;i<ts.size();i++) {
        //meta.append(",").append(ts.get(i) - ts.get(i-1));
        lst.add((int) (ts.get(i) - ts.get(i-1)));
        val.append(",").append(valueBuffer.get(ts.get(i)));
    }
    int prev = lst.get(0), cnt = 1;
    for(int i=1;i<ts.size()-1;i++) {
        if(lst.get(i) == prev) cnt += 1;
        else {
            if(cnt == 1) meta.append(",").append(prev);
            else {
                meta.append(",").append(prev).append("<").append(cnt);
            }
            prev = lst.get(i);
            cnt = 1;
        }
        //meta.append(",").append(ts.get(i) - ts.get(i-1));
    }
    meta.append("|").append(minimalTS).append(";").append(maxTS);
    meta.append("|").append(valueBuffer.size());
    return new String[] {meta.toString()+":", val.toString()};*/
    return new String[] {"", ""};
  }

  public void clearMemData(String persistedFileName) {
    // this.values = new ArrayList<>();
    this.valueBuffer = new ArrayList<>();
    this.hasPersisted = true;
    this.persistedFileName = persistedFileName;
  }

  public void setUseEnc(boolean useEnc) {
    this.useEnc = useEnc;
  }

  public boolean getHasPersisted() {
    return this.hasPersisted;
  }

  public static List<Ts2diffEncData> loadTimestamp(String metaTime) {
    String[] time = metaTime.split(",");
    List<Ts2diffEncData> ans = new ArrayList<>();
    int bias = 0;
    for (String s : time) {
      String[] diff = s.split("<");
      if (diff.length == 1) {
        int delta = Integer.parseInt(diff[0]);
        bias += delta;
        ans.add(new Ts2diffEncData(bias, 0, 1));
      } else {
        int delta = Integer.parseInt(diff[0]);
        int sz = Integer.parseInt(diff[1]);
        ans.add(new Ts2diffEncData(bias + delta, delta, sz));
        bias += delta * sz;
      }
    }
    return ans;
  }

  public static ValueBlock loadTime(String meta) {
    String[] pkg = meta.split("\\|");
    long start = Long.parseLong(pkg[0]);
    List<Ts2diffEncData> timestamps = loadTimestamp(pkg[1]);
    return new ValueBlock(timestamps, null, start, -1);
  }

  public ValueBlock loadVal(String val) {
    // List<Double> values = new ArrayList<>();
    String[] vals = val.split(",");
    for (String ele : vals) {
      valueBuffer.add(Double.parseDouble(ele));
    }
    return this;
    // return new ValueBlock(timestamps, values, start, values.size());
    // return new ValueBlock(Integer.parseInt(pkg[pkg.length-1]));
  }

  public void restore() throws FileNotFoundException {
    if (!this.hasPersisted) return;
    File f = new File(this.persistedFileName);
    // Scanner sc = new Scanner(f);
    // while
  }
  // public void precompute()
}
