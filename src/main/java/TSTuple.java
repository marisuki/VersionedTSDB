//package org.apache.iotdb.db.query.reader.series;

public class TSTuple {
  long timestamp;
  // List<Double> values = new ArrayList<>();
  Double value;

  public TSTuple(long timestamp, Double value) {
    // values.add(value);
    this.timestamp = timestamp;
    this.value = value;
  }
}
