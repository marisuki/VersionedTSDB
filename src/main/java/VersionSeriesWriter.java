//package org.apache.iotdb.db.query.reader.series;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

// baseline.

public class VersionSeriesWriter {
  private LocationManager locationManager = new LocationManager();

  private String commonPathPrefix = "./data/";

  private final long maximumTupleSizeSingleFile = 100000000; // 10M tuple* (8byte) + enc
  private final int blockSize = 1000000;
  private long initTimestamp = System.currentTimeMillis();

  private int totalTuples = 0;
   List<List<VersionBlock>> valueBuffer = new ArrayList<>();
   List<String> bufferSeriesName = new ArrayList<>();
   List<Long> lastTS = new ArrayList<>();
   List<List<VersionBlock>> unSeq = new ArrayList<>();

  public VersionSeriesWriter() {}

  public List<List<VersionBlock>> getValueBuffer() {
    return valueBuffer;
  }

  public List<String> getBufferSeriesName() {
    return bufferSeriesName;
  }

  public List<List<VersionBlock>> getUnSeq() {
    return unSeq;
  }

  public void importFromCSV1Dim(
      List<String> originalFilePaths, String seriesName, boolean hasHeader) throws IOException, InterruptedException {
    int pos = bufferSeriesName.lastIndexOf(seriesName);
    List<VersionBlock> seriesBlock;
    List<VersionBlock> unseqBlock;
    long last = -1;
    if (pos != -1) {
      seriesBlock = valueBuffer.get(pos);
      last = lastTS.get(pos);
      unseqBlock = unSeq.get(pos);
    } else {
      seriesBlock = new ArrayList<>();
      valueBuffer.add(seriesBlock);
      this.bufferSeriesName.add(seriesName);
      lastTS.add(-1L);
      unseqBlock = new ArrayList<>();
      unSeq.add(unseqBlock);
    }
    // TODO: parallel
    for (String originalFilePath : originalFilePaths) {

      Path fp = Paths.get(originalFilePath);
      // Stream<String> data = ;
      List<VersionBlock> append = new ArrayList<>();
      //long curr = System.currentTimeMillis();
      List<TSTuple> res =
          Files.lines(fp)
              .map(
                  new Function<String, TSTuple>() {
                    @Override
                    public TSTuple apply(String s) {
                      String[] dec = s.split(",");
                      if(dec[0].equals("Time")) return new TSTuple(-1, -1.0);
                      return new TSTuple(Long.parseLong(dec[0]), Double.parseDouble(dec[1]));
                    }
                  }).skip(0)
              .collect(Collectors.toList());
      //System.out.println(System.currentTimeMillis()-curr);
      long sz = res.size();
      //res = res.subList(1, res.size());
      // long minTS = time.min(Comparator.naturalOrder()).get();
      VersionBlock prev=null;
      if (sz > this.blockSize) {
        long numNewBlk = (long) (sz / this.blockSize);
        for (long i = 0; i < numNewBlk; i++) {
          VersionBlock vb = new VersionBlock(true);

          //res = vb.write(res, last, this.blockSize); control the unseq ones.
          res = vb.write(res, -1, this.blockSize); // append only
          append.add(vb);
          last = vb.endTS;
          vb.setAssignedTS(System.currentTimeMillis());
          if(prev==null) {prev=vb;}
          else {prev.setNext(vb); vb.setAhead(prev); prev = vb;}
        }
      } else {
        VersionBlock vb = new VersionBlock(true);
        res = vb.write(res, -1, -1);
        append.add(vb);
        last = vb.endTS;
        vb.setAssignedTS(System.currentTimeMillis());
      }
      sortVersionBlock(append, true);
      seriesBlock.addAll(append);
    }

    //addUnseqData();
  }

  public void sortVersionBlock(List<VersionBlock> append, boolean isAsc) throws InterruptedException {
    Queue<Thread> que  = new ArrayDeque<>();
    for(VersionBlock vb: append) {
      Thread th = new Thread(new Runnable() {
        @Override
        public void run() {
          vb.sort(isAsc);
        }
      });
      th.start();
      que.add(th);
    }
    for(Thread th: que) th.join();
  }

  public void addUnseqData(List<TSTuple> disorder, List<VersionBlock> unSeqBlocks) {
    long sz = disorder.size();
    // long minTS = time.min(Comparator.naturalOrder()).get();
    if (sz > this.blockSize) {
      long numNewBlk = (long) (sz / this.blockSize);
      for (long i = 0; i < numNewBlk; i++) {
        VersionBlock vb = new VersionBlock(false);
        disorder = vb.write(disorder, -1, this.blockSize);
        unSeqBlocks.add(vb);
        //last = vb.endTS;
        vb.setAssignedTS(System.currentTimeMillis());
      }
    } else {
      VersionBlock vb = new VersionBlock(false);
      disorder = vb.write(disorder, -1, -1);
      unSeqBlocks.add(vb);
      //last = vb.endTS;
      vb.setAssignedTS(System.currentTimeMillis());
    }
  }

  public void encOldValue() {

  }

  public void persistAll() {
    for(int i=0; i<this.valueBuffer.size();i++) {
      List<VersionBlock> lvb = this.valueBuffer.get(i);
      for(VersionBlock vb: lvb) {
        String[] str = vb.serialize(); // 0: t, 1: v, 2: count, 3: sum, 4: min, 5: max, 6: assignTS(version);

      }
    }
  }

  public VersionSeriesReader encAll() throws InterruptedException {
    Queue<Thread> que = new ArrayDeque<>();
    for(int i=0; i<this.valueBuffer.size();i++) {
      List<VersionBlock> lvb = this.valueBuffer.get(i);
      for(VersionBlock vb: lvb) {
        //String[] str = vb.serialize(); // 0: t, 1: v, 2: count, 3: sum, 4: min, 5: max, 6: assignTS(version);
        Thread th = new Thread(new Runnable() {
          @Override
          public void run() {
            String[] str = vb.serialize();
            vb.clearCache();
            //vb.decTimestampToDiff(str[0]);
            vb.encVPath = str[1];
          }
        });
        th.start();
        que.add(th);
      }
    }
    for(Thread th: que) th.join();
    System.gc();
    VersionSeriesReader vr = new VersionSeriesReader(this.valueBuffer, this.bufferSeriesName);
    return vr;
  }

  /*
  private Queue<Thread> mergeOverlapped(int pos, List<TSTuple> overlap) {
      List<VersionBlock> values = valueBuffer.get(pos);
      Queue<Thread> que = new ArrayDeque<>();
      overlap.sort(new Comparator<TSTuple>() {
          @Override
          public int compare(TSTuple tsTuple, TSTuple t1) {
              return tsTuple.timestamp < t1.timestamp? 1: -1;
          }
      });
      for(VersionBlock vb: values) {
          Thread tmp = new Thread(new Runnable() {
              @Override
              public void run() {
                  vb.merge(overlap);
              }
          });
          tmp.start();
          que.add(tmp);
      }
      return que;
  }

  private void flush() throws IOException {
      // find farthest visited cache data
      // clearCache of each valueBolck, only keep the index
      // change overall index lowerbound.
      String flushSeriesName = null; long visTime = -1L;
      StringBuilder sbt = new StringBuilder();
      for(String seriesName: lastVisTime.keySet()) {
          long ts = lastVisTime.get(seriesName);
          if (visTime < 0 || (visTime > 0 && visTime > ts)) {
              visTime = ts;
              flushSeriesName = seriesName;
          }
      }
      int pos = this.bufferSeriesName.lastIndexOf(flushSeriesName);
      long flushedTup = 0;
      int timebias = (int) (System.currentTimeMillis()-this.initTimestamp);
      String pathT = commonPathPrefix + flushSeriesName + "T." + timebias + ".dat";
      locationManager.addLocationT(flushSeriesName, pathT, true);
      FileWriter bft = new FileWriter(new File(pathT));
      for(ValueBlock vb: this.valueBuffer.get(pos)) {
          if (flushedTup >= maximumTupleSizeSingleFile/3.0) {
              totalTuples -= flushedTup;
              break;
          }
          if(vb.getHasPersisted()) continue;
          String[] encData = vb.serialize();
          timebias = (int) (System.currentTimeMillis()-this.initTimestamp);
          String pathV = commonPathPrefix + flushSeriesName + "V." + timebias + ".dat";
          flushedTup += vb.getCacheSize();
          vb.clearMemData(pathV);
          Writer write = new FileWriter(pathV);
          write.write(encData[1]);
          bft.write(encData[0] + "\n");
          write.close();
          locationManager.addLocation(pathT, pathV);
      }
      bft.close();
      System.gc();
  }
  */

}
