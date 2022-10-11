//package org.apache.iotdb.db.query.reader.series;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class performance {

  public static void Q1climate() throws IOException, InterruptedException {

    String prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
    List<Long> res = new ArrayList<>();
    //for (int i = 1; i <= 10; i++) {
      //List<String> paths = new ArrayList<String>();
      //paths.add(prefix + i + ".csv");
      //long curr = System.currentTimeMillis();
      //vsw.importFromCSV1Dim(paths, "t70.sc" + i, true);
      //long cost = System.currentTimeMillis() - curr;
      //System.out.println(cost);
      //res.add(cost);
    //}

    //persistAns(res, "./result/verext-load+sort-scala3.dat");

    res = new ArrayList<>();
    //vr.executeQ1naive("t70.sc"+1, "rhoair");
    //vr.executeQ1naive("t70.sc"+1, "wdir");
    for(int i=1;i<=10;i++) {
      VersionSeriesWriter vsw = new VersionSeriesWriter();
      prefix = "D://GitHub/relationversion/dataset/climate_s/";
      List<String> path = new ArrayList<String>();
      path.add(prefix + "rhoair.csv");
      vsw.importFromCSV1Dim(path, "rhoair" , true);
      path = new ArrayList<String>();
      path.add(prefix + "wdir.csv");
      vsw.importFromCSV1Dim(path, "wdir" , true);
      prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
      List<String> paths = new ArrayList<String>();
      paths.add(prefix + i + ".csv");
      vsw.importFromCSV1Dim(paths, "t70.sc" + i, true);
      VersionSeriesReader vr = vsw.encAll();
      long curr = System.currentTimeMillis();
      if(i<=0) {
        vr.executeQ1("t70.sc"+i, "rhoair");
        vr.executeQ1("t70.sc"+i, "wdir");
      } else {
        vr.executeQ1naive("t70.sc"+i, "rhoair");
        vr.executeQ1naive("t70.sc"+i, "wdir");
      }
      long cost = System.currentTimeMillis() - curr;
      System.out.println(cost/2);
      res.add(cost/2);
    }
    persistAns(res, "./result/verext-Q1-naive-scala-final.txt");
    //List<TSTuple> ans = vr.valueFilter(vr.data.get(0), new ValueFilter("t70.sc1", 0, 3));
    //for(TSTuple t: ans) System.out.println(t.timestamp);
  }

  public static void Q2climate() throws IOException, InterruptedException {

    String prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
    List<Long> res = new ArrayList<>();

    statistic s = new statistic();
    List<Double> pivots = s.findPivot(s.stat(prefix + 5 + ".csv"), s.cntx);
    //prefix = "D://GitHub/relationversion/dataset/climate_s/";
    //List<String> path = new ArrayList<String>();
    //path.add(prefix + "rhoair.csv");
    //vsw.importFromCSV1Dim(path, "rhoair" , true);
    //path = new ArrayList<String>();
    //path.add(prefix + "wdir.csv");
    //vsw.importFromCSV1Dim(path, "wdir" , true);

    // value filtering varied selectivity
    for(Double v: pivots) {
      VersionSeriesWriter vsw = new VersionSeriesWriter();
      List<String> paths = new ArrayList<String>();
      paths.add(prefix + 5 + ".csv");
      vsw.importFromCSV1Dim(paths, "t70.sc" + 5, true);
      VersionSeriesReader vr = vsw.encAll();
      long curr = System.currentTimeMillis();
      vr.executeQ2ValueFilter("t70.sc" + 5, pivots.get(0), v);
      //vr.executeQ2ValueFilterNaive("t70.sc" + 5, pivots.get(0), v);
      long cost = System.currentTimeMillis() - curr;
      System.out.println(cost);
      res.add(cost);
      //break;
    }
    persistAns(res, "./result/verext-Q2-select-final.txt");
  }

  public static void Q2climateScala() throws IOException, InterruptedException {

    String prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
    List<Long> res = new ArrayList<>();


    //prefix = "D://GitHub/relationversion/dataset/climate_s/";
    //List<String> path = new ArrayList<String>();
    //path.add(prefix + "rhoair.csv");
    //vsw.importFromCSV1Dim(path, "rhoair" , true);
    //path = new ArrayList<String>();
    //path.add(prefix + "wdir.csv");
    //vsw.importFromCSV1Dim(path, "wdir" , true);

    // value filtering varied selectivity
    for (int i = 1; i <= 10; i++) {
      statistic s = new statistic();
      List<Double> pivots = s.findPivot(s.stat(prefix + 5 + ".csv"), s.cntx);
      Double v = pivots.get(5);
      VersionSeriesWriter vsw = new VersionSeriesWriter();
      List<String> paths = new ArrayList<String>();
      paths.add(prefix + i + ".csv");
      vsw.importFromCSV1Dim(paths, "t70.sc" + i, true);
      VersionSeriesReader vr = vsw.encAll();
      long curr = System.currentTimeMillis();
      vr.executeQ2ValueFilter("t70.sc" + i, pivots.get(0), v);
      //vr.executeQ2ValueFilterNaive("t70.sc" + i, pivots.get(0), v);
      long cost = System.currentTimeMillis() - curr;
      System.out.println(cost);
      res.add(cost);
      //break;
    }
    persistAns(res, "./result/verext-Q2-scala1.dat");
  }

  public static void Q3climate() throws IOException, InterruptedException {
    String prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
    List<Long> res = new ArrayList<>();

    statistic s = new statistic();
    List<Long> pivots = s.findPivotTime(prefix + 5 + ".csv");
    //prefix = "D://GitHub/relationversion/dataset/climate_s/";
    //List<String> path = new ArrayList<String>();
    //path.add(prefix + "rhoair.csv");
    //vsw.importFromCSV1Dim(path, "rhoair" , true);
    //path = new ArrayList<String>();
    //path.add(prefix + "wdir.csv");
    //vsw.importFromCSV1Dim(path, "wdir" , true);

    // value filtering varied selectivity
    for(int i=1;i<=10;i++) {
      long v = pivots.get(i);
      VersionSeriesWriter vsw = new VersionSeriesWriter();
      List<String> paths = new ArrayList<String>();
      paths.add(prefix + 5 + ".csv");
      vsw.importFromCSV1Dim(paths, "t70.sc" + 5, true);
      VersionSeriesReader vr = vsw.encAll();
      long curr = System.currentTimeMillis();
      //vr.Q3rangeQuery("t70.sc" + 5, pivots.get(0), v);
      vr.Q3rangeQueryNaive("t70.sc" + 5, pivots.get(0), v);
      long cost = System.currentTimeMillis() - curr;
      System.out.println(cost);
      res.add(cost);
      //break;
    }
    persistAns(res, "./result/verext-Q3-naive-select.dat");
  }

  public static void Q3climateScala() throws IOException, InterruptedException {

    String prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
    List<Long> res = new ArrayList<>();


    //prefix = "D://GitHub/relationversion/dataset/climate_s/";
    //List<String> path = new ArrayList<String>();
    //path.add(prefix + "rhoair.csv");
    //vsw.importFromCSV1Dim(path, "rhoair" , true);
    //path = new ArrayList<String>();
    //path.add(prefix + "wdir.csv");
    //vsw.importFromCSV1Dim(path, "wdir" , true);

    // value filtering varied selectivity
    for (int i = 1; i <= 10; i++) {
      statistic s = new statistic();
      List<Long> pivots = s.findPivotTime(prefix + i + ".csv");
      Long v = pivots.get(5);
      VersionSeriesWriter vsw = new VersionSeriesWriter();
      List<String> paths = new ArrayList<String>();
      paths.add(prefix + i + ".csv");
      vsw.importFromCSV1Dim(paths, "t70.sc" + i, true);
      VersionSeriesReader vr = vsw.encAll();
      long curr = System.currentTimeMillis();
      vr.Q3rangeQueryNaive("t70.sc" + i, pivots.get(0), v);
      //vr.executeQ2ValueFilterNaive("t70.sc" + i, pivots.get(0), v);
      long cost = System.currentTimeMillis() - curr;
      System.out.println(cost);
      res.add(cost);
      //break;
    }
    persistAns(res, "./result/verext-Q3-naive-scala.dat");
  }

  public static void Q4climateScala() throws IOException, InterruptedException {
    // down-sampling
    String prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
    List<Long> res = new ArrayList<>();


    //prefix = "D://GitHub/relationversion/dataset/climate_s/";
    //List<String> path = new ArrayList<String>();
    //path.add(prefix + "rhoair.csv");
    //vsw.importFromCSV1Dim(path, "rhoair" , true);
    //path = new ArrayList<String>();
    //path.add(prefix + "wdir.csv");
    //vsw.importFromCSV1Dim(path, "wdir" , true);

    // value filtering varied selectivity
    for (int i = 1; i <= 10; i++) {
      //statistic s = new statistic();
      //List<Long> pivots = s.findPivotTime(prefix + i + ".csv");
      //Long v = pivots.get(5);
      VersionSeriesWriter vsw = new VersionSeriesWriter();
      List<String> paths = new ArrayList<String>();
      paths.add(prefix + i + ".csv");
      vsw.importFromCSV1Dim(paths, "t70.sc" + i, true);
      VersionSeriesReader vr = vsw.encAll();
      long curr = System.currentTimeMillis();
      //List<TSTuple> prec = vr.Q4DownSamplingUpd("t70.sc" + i);
      List<TSTuple> prec = vr.Q4DownSamplingUpdNaive("t70.sc" + i);
      vr.Q4DownSampling(prec, vr.data.get(0).get(0).startTS, 60L, 60L);//rangeQueryNaive("t70.sc" + i, pivots.get(0), v);
      //vr.executeQ2ValueFilterNaive("t70.sc" + i, pivots.get(0), v);
      long cost = System.currentTimeMillis() - curr;
      System.out.println(cost);
      res.add(cost);
      //break;
    }
    persistAns(res, "./result/verext-Q4-naive-scala.dat");
  }

  public static void Q5climateScala() throws IOException, InterruptedException {
    // down-sampling
    //String prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
    List<Long> res = new ArrayList<>();


    //prefix = "D://GitHub/relationversion/dataset/climate_s/";

    //path = new ArrayList<String>();
    //path.add(prefix + "wdir.csv");
    //vsw.importFromCSV1Dim(path, "wdir" , true);

    // value filtering varied selectivity
    for (int i = 1; i <= 9; i++) {
      //statistic s = new statistic();
      //List<Long> pivots = s.findPivotTime(prefix + i + ".csv");
      //Long v = pivots.get(5);
      VersionSeriesWriter vsw = new VersionSeriesWriter();
      String prefix = "D://GitHub/relationversion/dataset/climate_s/";
      List<String> path = new ArrayList<String>();
      path.add(prefix + "rhoair.csv");
      vsw.importFromCSV1Dim(path, "rhoair" , true);
      prefix = "D://GitHub/relationversion/dataset/climate_sc/t70.sc";
      List<String> paths = new ArrayList<String>();
      paths.add(prefix + i + ".csv");
      vsw.importFromCSV1Dim(paths, "t70.sc" + i, true);
      vsw.importFromCSV1Dim(paths, "t70.sc" + Math.min(i+1, 10), true);
      VersionSeriesReader vr = vsw.encAll();
      vr.addEdge("Related", "t70.sc" + i);
      vr.addEdge("Related", "t70.sc" + Math.min(i+1, 10));
      long curr = System.currentTimeMillis();
      //List<TSTuple> prec = vr.Q4DownSamplingUpd("t70.sc" + i);
      //List<TSTuple> prec = vr.Q4DownSamplingUpdNaive("t70.sc" + i);
      //vr.Q5Batched("Related", vr.data.get(0).get(0).startTS, 60L, 60L);
      List<String> related = new ArrayList<>();
      related.add("t70.sc" + i); related.add("t70.sc" + (i+1));
      vr.Q5BatchedNaive(related, vr.data.get(0).get(0).startTS, 60L, 60L);//rangeQueryNaive("t70.sc" + i, pivots.get(0), v);
      //vr.executeQ2ValueFilterNaive("t70.sc" + i, pivots.get(0), v);
      long cost = System.currentTimeMillis() - curr;
      System.out.println(cost);
      res.add(cost);
      //break;
    }
    persistAns(res, "./result/verext-Q5-naive-scala.dat");
  }

  public static void persistAns(List<Long> res, String file) throws IOException {
    // for(Long x: res) System.out.print(x);
    // System.out.println();
    BufferedWriter writer = new BufferedWriter(new FileWriter(file));
    for (Long x : res) writer.write(String.valueOf(x) + " ");
    writer.write("\n");
    writer.close();
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Q2climate();
  }
}
