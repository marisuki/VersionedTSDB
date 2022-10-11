//package org.apache.iotdb.db.query.reader.series;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocationManager {
  private Map<String, List<String>> seriesLocationsT;
  private Map<String, List<String>> seriesLocationsV;
  private Map<String, List<String>> seriesLocationsHeader;
  private Map<String, Boolean> isSeqLocation;

  public LocationManager() {
    seriesLocationsT = new HashMap<>();
    seriesLocationsV = new HashMap<>();
    isSeqLocation = new HashMap<>();
  }

  public void addLocationT(String series, String storedLocation, boolean isSeq) {
    if (!seriesLocationsT.containsKey(series)) seriesLocationsT.put(series, new ArrayList<>());
    seriesLocationsT.get(series).add(storedLocation);
    isSeqLocation.put(storedLocation, isSeq);
  }

  public void addLocation(String locationT, String storedLocationV) {
    if (!seriesLocationsT.containsKey(locationT))
      seriesLocationsT.put(locationT, new ArrayList<>());
    seriesLocationsT.get(locationT).add(storedLocationV);
  }

  public List<String> accessSeriesHeader(String series) {
    return seriesLocationsT.getOrDefault(series, new ArrayList<>());
  }

  public List<String> accessSeriesValue(String locationT) {
    return seriesLocationsV.getOrDefault(locationT, new ArrayList<>());
  }

  public Boolean isSeqFile(String fileName) {
    if (isSeqLocation.containsKey(fileName)) return isSeqLocation.get(fileName);
    return null;
  }
}
