//package org.apache.iotdb.db.query.reader.series;

import java.util.ArrayList;
import java.util.List;

public class VersionSeries {
  public List<Integer> timestamp = new ArrayList<>();
  public List<Integer> version = new ArrayList<>();
  public List<Double> value = new ArrayList<>();
}
