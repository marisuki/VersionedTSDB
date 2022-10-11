//package org.apache.iotdb.db.query.reader.series;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VersionBlock {
  List<TSTuple> tuples;
  long startTS, endTS;
  long assignedTS;
  boolean isSeq;
  boolean isEnc;
  Double sum = null;
  int count = -1;
  String encVPath;
  String envTPath;
  List<Ts2diffEncData> encTime = new ArrayList<>();
  List<Double> val = new ArrayList<>();
  List<Integer> validPosST = new ArrayList<>();
  List<Integer> validPosED = new ArrayList<>();
  long validTsEnd=this.endTS;
  boolean filtered=false;
  long id;
  VersionBlock next;
  VersionBlock ahead;
  int pos_ahead, pos_next;

  public VersionBlock(boolean isSeq) { this.isSeq = isSeq; id = System.currentTimeMillis();}

  public List<TSTuple> write(List<TSTuple> res, long tsLargerThan, int writeSz) {
    List<TSTuple> ins = res;
    if (tsLargerThan > 0)
      ins =
          ins.stream()
              .filter(
                  new Predicate<TSTuple>() {
                    @Override
                    public boolean test(TSTuple tsTuple) {
                      return tsTuple.timestamp > tsLargerThan;
                    }
                  })
              .collect(Collectors.toList());
    if (writeSz > 0) tuples = ins.subList(1, Math.min(ins.size(), writeSz));
    else tuples = ins;
    //endTS = -1;
    //for (TSTuple t : tuples) endTS = Math.max(endTS, t.timestamp);
    if(writeSz > 0) return res.subList(writeSz, res.size());
    else return new ArrayList<>();
  }

    public void setAhead(VersionBlock ahead) {
        this.ahead = ahead;
    }

    public void setNext(VersionBlock next) {
        this.next = next;
    }

    public void setAssignedTS(long assignedTS) {
        this.assignedTS = assignedTS;
    }

    public void sort(boolean isAsc) {
      this.tuples.sort(new Comparator<TSTuple>() {
          @Override
          public int compare(TSTuple tsTuple, TSTuple t1) {
              if(isAsc) return tsTuple.timestamp < t1.timestamp? -1: 1;
              else return tsTuple.timestamp > t1.timestamp? -1: 1;
          }
      });
      startTS = this.tuples.get(0).timestamp;
      endTS = this.tuples.get(this.tuples.size()-1).timestamp;
      count = this.tuples.size();
    }

    private String serializeTime() {
      StringBuilder sb = new StringBuilder();
      //List<Integer> removal = new ArrayList<>();
      long st =  startTS, prev = st;
      List<Integer> delta = new ArrayList<>();
      for(int i=1;i<this.tuples.size();i++) {
          delta.add((int) (this.tuples.get(i).timestamp - prev));
          prev = this.tuples.get(i).timestamp;
      }
      sb.append(st);
      int base = -1, cnt = 1, pos = -1;
      this.encTime = new ArrayList<>(); this.isEnc=true;
      for(Integer x: delta) {
          pos ++;
          if(x == 0) { this.tuples.get(pos).timestamp = -1L; continue;}
          if(base == -1) {base=x; continue; }
          if(x.equals(base)) cnt += 1;
          else {
              this.encTime.add(new Ts2diffEncData(st, base, cnt));
              if(cnt == 1) sb.append(",").append(base);
              else sb.append(",").append(base).append(":").append(cnt);
              st += (long) base *cnt;
              base = x; cnt = 1;
          }
      }
      sb.append(",").append(base);
      if(cnt > 1) sb.append(":" + cnt);
      this.encTime.add(new Ts2diffEncData(st, base, cnt));
      tuples = tuples.stream().filter(new Predicate<TSTuple>() {
          @Override
          public boolean test(TSTuple tsTuple) {
              return tsTuple.timestamp > 0;
          }
      }).collect(Collectors.toList());
      //System.out.println(tuples.size());
      return sb.toString();
    }

    private String serializeValueStatistics() {
      StringBuilder sb = new StringBuilder();
      sum = 0d;
      Stream<Double> val = this.tuples.stream().map(new Function<TSTuple, Double>() {
          @Override
          public Double apply(TSTuple tsTuple) {
              return tsTuple.value;
          }
      });
      count = this.tuples.size();
      sum = val.reduce(new BinaryOperator<Double>() {
          @Override
          public Double apply(Double aDouble, Double aDouble2) {
              return aDouble+aDouble2;
          }
      }).get();

      for(TSTuple t: this.tuples) {
          sb.append(t.value).append(",");
      }
      return sb.toString().substring(0, sb.length()-2);
    }

    public String[] serialize() {
      String t = serializeTime();
      return new String[]{t, serializeValueStatistics(), ""+count, ""+sum, ""+startTS, ""+endTS, ""+assignedTS};
    }

    public void clearCache() {
      this.tuples = new ArrayList<>();
      this.isEnc = true;
    }

    public void setCount(int count) {
        this.count = count;
    }

    //public void removeData()

    public void decTimestampToDiff(String encTimes) {
      this.encTime = new ArrayList<>();
      String[] times = encTimes.split(",");
      long st = Long.parseLong(times[0]), curr = st;
      this.startTS = st;
      for(int i=1;i<times.length;i++) {
          String enc = times[i];
          String[] res = encTimes.split(":");
          int delta = Integer.parseInt(res[0]);
          int len = 1;
          if(res.length == 1) this.encTime.add(new Ts2diffEncData(curr, 0, len));
          else {
              len = Integer.parseInt(res[1]);
              this.encTime.add(new Ts2diffEncData(curr, delta, len));
          }
          curr += (long) delta *len;
      }
      this.endTS = curr;
    }

    public List<TSTuple> loadFilteredTimeAfterValFilter(Set<Integer> rmTime, int st, int ed) {
        List<TSTuple> ans = new ArrayList<>();
        if(this.val.isEmpty()) {
            loadVals(this.encVPath);
        }
        if(this.val.isEmpty()) return new ArrayList<>();
      int s = 0, cnts = 0, e = encTime.size() -1, cnte = count;
      while(s < encTime.size() && cnts + encTime.get(s).length < st) {s += 1; cnts += encTime.get(s).length; }
      while(e >= 0 && cnte - encTime.get(e).length > ed) {e -= 1; cnte -= encTime.get(e).length; }
      if(encTime.size() == 0) return new ArrayList<>();
      Ts2diffEncData tmp = encTime.get(s);
      int res_pos = 0; long ts = tmp.start + (long) tmp.delta *(st-cnts);
      for(int i=st-cnts;i<encTime.get(s).length;i++, res_pos++, ts += tmp.delta)
          if(!rmTime.contains(res_pos)) {ans.add(new TSTuple(ts,this.val.get(res_pos)));}
      for(int i=s+1;i<e;i++, res_pos++) {
          tmp = encTime.get(i); ts = tmp.start;
          if (tmp.start > validTsEnd) break;
          for(int x=0;x<tmp.length;x++, ts+= tmp.delta) {
              if(!rmTime.contains(res_pos) && ts < this.validTsEnd) ans.add(new TSTuple(ts, this.val.get(res_pos)));
          }
      }
      // TODO rest
      this.tuples = ans;
      return ans;
    }

    public void decAll() {
        if(!this.tuples.isEmpty()) return;
        int st=-1, ed=0x7f7f7f7f;
        if(!validPosST.isEmpty())
            for(Integer i: validPosST) st = Math.max(i, st);
        if(!validPosED.isEmpty())
            for(Integer i: validPosED) ed = Math.min(i, ed);
        loadFilteredTimeAfterValFilter(new HashSet<>(), st, ed);
    }



    public void alignByDec(VersionBlock vbCompared) {
      int pos1=0, pos2=0;
      List<TSTuple> ans1 = new ArrayList<>();
      List<TSTuple> curr = this.tuples;
      List<TSTuple> other = vbCompared.tuples;
      while(pos1 < this.tuples.size() && pos2 < other.size()) {
          if(curr.get(pos1).timestamp < other.get(pos2).timestamp) { pos1 ++; continue;}
          if(curr.get(pos1).timestamp > other.get(pos2).timestamp) { pos2 ++; continue;}
          if(curr.get(pos1).timestamp == other.get(pos2).timestamp) { ans1.add(curr.get(pos1)); pos1++; pos2++;}
      }
      this.tuples = ans1;
    }

    public void decodeByRange(long st, long ed) {
      long lb=st, ub=ed;
        List<List<TSTuple>> ans = new ArrayList<>();
        //if(lb >= ub) {decAll(); ans.add(this.tuples); ans.add(new ArrayList<>());ans.add(new ArrayList<>()); return ans;}
        //if(this.val==null || this.val.isEmpty()) loadVals(encVPath);
        boolean lb_done = false; int cnt = 0;
        int left_pos=0, right_pos=0;
        List<Long> mid = new ArrayList<>();
        for (Ts2diffEncData enc : this.encTime) {
            long end = enc.start + (long) enc.delta * enc.length;
            if (!lb_done) {
                long curr = enc.start;
                for (int j = 0; j < enc.length; j++, curr += enc.delta) {
                    if (curr < lb) {left_pos ++; right_pos++; }
                    else {mid.add(curr); right_pos++; }
                }
                lb_done = true;
            } else {
                // ub -> inf
                //if (i+1<this.encTime.size() && this.encTime.get(i+1).start >= ub)
                long curr = enc.start;
                for (int j = 0; j < enc.length; j++, curr += enc.delta) {
                    if (curr < ub) {mid.add(curr); right_pos++;}
                }
            }
            cnt += enc.length;
        }
        List<String> tmp = Arrays.asList(this.encVPath.split(","));
        List<Double > valx = tmp.subList(left_pos, right_pos).stream().map(new Function<String, Double>() {
            @Override
            public Double apply(String s) {
                return Double.parseDouble(s);
            }
        }).collect(Collectors.toList());
        this.tuples = new ArrayList<>();
        for(int i=0;i<valx.size();i++) {
            this.tuples.add(new TSTuple(mid.get(i), valx.get(i)));
        }
        this.startTS = mid.get(0); this.endTS = mid.get(mid.size()-1);
  }

    public void loadVals(String valueEnc) {
      if(filtered) {
          int st=0, ed=0x7f7f;
          for(Integer i: validPosST) st = Math.max(i, st);
          for(Integer i: validPosED) ed = Math.min(i, ed);
          if(ed < 0) {this.val = new ArrayList<>(); return;}
          List<String> filtered = new ArrayList<String>(Arrays.asList(valueEnc.split(",")));
          filtered = filtered.subList(st, Math.min(ed, filtered.size()));
          this.val = filtered.stream().map(new Function<String, Double>() {
              @Override
              public Double apply(String s) {
                  return Double.parseDouble(s);
              }
          }).collect(Collectors.toList());
          return;
      }
      List<String> tmp = new ArrayList<String>(Arrays.asList(valueEnc.split(",")));
      this.val = tmp.stream().map(new Function<String, Double>() {
          @Override
          public Double apply(String s) {
              return Double.parseDouble(s);
          }
      }).collect(Collectors.toList());
    }

    public void alignByRangeQueryPrecompute(VersionBlock vbCompared) {
      int pos1=0, pos2=0;
      rangeFilter(vbCompared.startTS, vbCompared.endTS);
    }

    public List<List<TSTuple>> overlappedTuples() {
        long lb = -1, ub = -1;
        if (ahead != null) lb = ahead.endTS;
        else lb = this.startTS;
        if (next != null) ub = next.endTS;
        else ub = this.endTS;
        return overlappedTuples(lb, ub);
    }

    public List<List<TSTuple>> overlappedTuples(long lb, long ub) {
      List<List<TSTuple>> ans = new ArrayList<>();
      if(lb >= ub) {decAll(); ans.add(this.tuples); ans.add(new ArrayList<>());ans.add(new ArrayList<>()); return ans;}
      if(this.val==null || this.val.isEmpty()) loadVals(encVPath);
      boolean lb_done = false; int cnt = 0;
      List<TSTuple> prev = new ArrayList<>(), foll = new ArrayList<>(), mid = new ArrayList<>();
        for (Ts2diffEncData enc : this.encTime) {
            long end = enc.start + (long) enc.delta * enc.length;
            if (!lb_done) {
                long curr = enc.start;
                for (int j = 0; j < enc.length; j++, curr += enc.delta) {
                    if (curr < lb) prev.add(new TSTuple(curr, this.val.get(cnt + j)));
                    else mid.add(new TSTuple(curr, this.val.get(cnt + j)));
                }
                lb_done = true;
            } else {
                // ub -> inf
                //if (i+1<this.encTime.size() && this.encTime.get(i+1).start >= ub)
                long curr = enc.start;
                for (int j = 0; j < enc.length; j++, curr += enc.delta) {
                    if (curr >= ub) foll.add(new TSTuple(curr, this.val.get(cnt + j)));
                    else mid.add(new TSTuple(curr, this.val.get(cnt + j)));
                }
            }
            cnt += enc.length;
        }
      ans.add(prev); ans.add(mid); ans.add(foll);
      return ans;
    }

    public List<List<TSTuple>> partialAgg(String type, long windowSz, long windowStep, long st) {
      List<List<TSTuple>> pre = overlappedTuples();
      if(pre.get(1).size() == 0) return pre;
      List<List<TSTuple>> ans = new ArrayList<>();
      List<TSTuple> agg = new ArrayList<>();
      long mid_st, mid_ed;
      if(ahead!=null) mid_st = ahead.endTS; else mid_st = this.startTS;
      if(next!=null) mid_ed = next.endTS; else mid_ed = this.endTS;
      int pos_start = (int) Math.ceil((mid_st - st)*1.0/(windowSz+windowStep));
      int pos_end = (int) Math.floor((mid_ed - st+windowStep)*1.0/(windowStep+windowSz));
      return null;
    }

    public int moveOverlappedData() {
      // asks filtered=false;
      if(ahead==null) return -1;
      int pos=ahead.encTime.size()-1, count = 0;
      for(;pos>=0;pos--) {
          count += ahead.encTime.get(pos).length;
          if(ahead.encTime.get(pos).start <= this.startTS) {
              break;
          }
      }
      ahead.filtered = true;
      ahead.validPosST.add(ahead.count-count);
      ahead.validPosED.add(ahead.count);
      List<Ts2diffEncData> rest = ahead.encTime.subList(pos, ahead.encTime.size());
      rest.addAll(this.encTime); this.encTime = rest;
      return count;
    }

    public List<TSTuple> loadValFilter(String valueEnc, ValueFilter vf) {
      int prev = 0; this.filtered = true;
      if(ahead != null && ahead.endTS > this.startTS) prev = moveOverlappedData();
      if(next  != null) validTsEnd = next.startTS;
      return loadValSafeFilter(this.encVPath, vf, prev);
    }

    public List<TSTuple> loadValSafeFilter(String valueEnc, ValueFilter vf){
      return loadValSafeFilter(valueEnc, vf, -1);
    }

    public List<TSTuple> loadValSafeFilter(String valueEnc, ValueFilter vf, int concat) { // final ans
      Set<Integer> rmTime = new HashSet<>();
      int st=0, ed=0x7f7f7f7f;
        if(filtered) {
            for(Integer i: validPosST) st = Math.max(i, st);
            for(Integer i: validPosED) ed = Math.min(i, ed);
            validPosST = new ArrayList<>(); validPosST.add(st);
            validPosED = new ArrayList<>(); validPosED.add(ed);
            List<String> filtered;
            if(ahead!=null && concat >= 0) {
                filtered = new ArrayList<String>(Arrays.asList(ahead.encVPath.split(",")));
                filtered = filtered.subList(concat, filtered.size());
                List<String> tmp = new ArrayList<String>(Arrays.asList(valueEnc.split(",")));
                filtered.addAll(tmp.subList(st, Math.min(ed, tmp.size())));
            }
            else {
                List<String> tmp = new ArrayList<String>(Arrays.asList(valueEnc.split(",")));
                filtered = tmp.subList(st, Math.min(ed, tmp.size()));
            }
            filtered.sort(Comparator.naturalOrder());
            this.val = filtered.stream().map(new Function<String, Double>() {
                @Override
                public Double apply(String s) {
                    return Double.parseDouble(s);
                }
            }).collect(Collectors.toList());
        }
        else {
            this.val = Arrays.stream(valueEnc.split(",")).map(new Function<String, Double>() {
                @Override
                public Double apply(String s) {
                    return Double.parseDouble(s);
                }
            }).collect(Collectors.toList());
        }
        //List<Double> newVal = new ArrayList<>();
        for(int i=0;i<val.size();i++) {
            if(!vf.test(this.val.get(i))) {rmTime.add(i);}
            //else newVal.add(this.val.get(i));
        }
        //this.val = newVal;
        return loadFilteredTimeAfterValFilter(rmTime, st, ed);
    }

    public double alignedIntersectEncEstimateSelectivity(VersionBlock vb) {
      long sz = this.count;
      if(vb.isEnc) {
          if (this.isEnc) return Utils.estimate2EncTime(this.encTime, vb.encTime);
          else return Utils.estimateEncDecTime(this.tuples, vb.encTime);
      } else {
          if (this.isEnc) return Utils.estimateEncDecTime(vb.tuples, this.encTime);
      }
      return 1;
    }

    public void rangeFilter(long st, long ed) {
        this.validPosST = new ArrayList<>();
        this.validPosED = new ArrayList<>();
        filtered = true;
        if (st > endTS || ed < startTS) {
            validPosST.add(-1);
            validPosED.add(-1);
            return;
        }
      if(isEnc) {
          List<Ts2diffEncData> filteredEnc = new ArrayList<>();
          int pos = 0;
          for(Ts2diffEncData enc: this.encTime) {
              long edTime = enc.start + (long) enc.delta*enc.length;
              if(edTime < st || enc.start > ed) {pos += enc.length; continue;}
              else {
                  int addTop = (int) Math.ceil((st-enc.start) * 1.0 / enc.delta);
                  validPosST.add(pos + addTop); break;
              }
          }
          pos = count; int i=this.encTime.size()-1;
          long edTime = 0;
          Ts2diffEncData enc;
          while (true) {
              enc = this.encTime.get(i);
              //edTime = enc.start + (long) enc.delta*enc.length;
              if(enc.start > ed) { i--; pos -= enc.length;}
              else break;
          }
          //enc = this.encTime.get(i);
          edTime = enc.start + (long) enc.delta*enc.length;
          int minusTop = (int) Math.ceil((edTime-ed)*1.0/(enc.delta));
          this.validPosED.add(pos-minusTop);
      }
      else {
          int i=0;
          while (i < this.tuples.size() && st > this.tuples.get(i).timestamp) i++;
          validPosST.add(i);
          i = this.tuples.size()-1;
          while(i>=0 && ed < this.tuples.get(i).timestamp) i--;
          validPosED.add(i);
      }
    }

    //public void setEncVPath

    public static void main(String[] args) {
        VersionBlock vb = new VersionBlock(true);
        List<TSTuple> l = new ArrayList<>();
        l.add(new TSTuple(1L, 1.0));
        l.add(new TSTuple(1L, 2.0));
        l.add(new TSTuple(3L, 4.0));
        vb.write(l, -1, -1);
        vb.sort(true);
        String[] s = vb.serialize();
        System.out.println(s[0]);
        System.out.println(s[1]);
    }


}
