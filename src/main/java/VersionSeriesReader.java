/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
//package org.apache.iotdb.db.query.reader.series;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Clone from series reader, for version plans */
public class VersionSeriesReader {
    //private VersionSeriesWriter versionSeriesWriter;
     List<List<VersionBlock>> data;
     List<String> series;
     LocationManager locationManager;
     Map<String, List<String>> graph = new HashMap<>();

    public VersionSeriesReader() {
    }

    public VersionSeriesReader(LocationManager lm) {
        this.locationManager = lm;
        loadAll();
    }

    public VersionSeriesReader(List<List<VersionBlock>> persistedValues, List<String> seriesName) {
        this.data = persistedValues;
        this.series = seriesName;
    }

    private void loadAll() {
        // TODO
    }

    public void executeQ1(String series1, String series2) throws InterruptedException {
        // align two series
        //align op1 = new align(new ArrayList<>());
        //List<List<VersionBlock>>  input = new ArrayList<>();
        List<VersionBlock> vb1 = data.get(series.lastIndexOf(series1));
        List<VersionBlock> vb2 = data.get(series.lastIndexOf(series2));
        Queue<Thread> que = new ArrayDeque<>();
        long st1 = vb1.get(0).startTS, st2 = vb2.get(0).startTS;
        long ed1 = vb1.get(vb1.size()-1).endTS, ed2 = vb2.get(vb2.size()-1).endTS;
        for(VersionBlock vb: vb1) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    vb.decodeByRange(st2, ed2);
                }
            });
            th.start();
            que.add(th);
        }
        for(VersionBlock vb: vb2) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    vb.decodeByRange(st1, ed1);
                }
            });
            th.start();
            que.add(th);
        }
        for(Thread th: que) th.join();
        que = new ArrayDeque<>();
        for(VersionBlock vbx: vb1) {
            for(VersionBlock vby: vb2) {
                if(vbx.endTS > vby.startTS || vbx.startTS < vby.endTS) {
                    Thread th = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            vbx.alignByDec(vby);
                        }
                    });
                    th.start();
                    que.add(th);
                }
            }
        }
        for(Thread th: que) th.join();
        //for(String s: series) System.out.println(s);
        //input.add(this.data.get(series.lastIndexOf(series1)));
        //input.add(this.data.get(series.lastIndexOf(series2)));
        //op1.execute(input);
        //return alignment(input.get(0), input.get(1));
    }

    public List<TSTuple> executeQ1naive(String series1, String series2) throws InterruptedException {
        // align two series
        naiveDec(series1); naiveDec(series2);
        return naiveAlign(series1, series2);
    }

    public void naiveDec(String series1) {
        for(VersionBlock vb: this.data.get(series.lastIndexOf(series1))) {
            vb.decAll();
        }
    }

    public List<TSTuple> naiveAlign(String series1, String series2) {
        List<VersionBlock> d1 = this.data.get(series.lastIndexOf(series1));
        List<VersionBlock> d2 = this.data.get(series.lastIndexOf(series2));
        List<TSTuple> data1 = new ArrayList<>();
        List<TSTuple> data2 = new ArrayList<>();
        for(VersionBlock vb: d1) data1.addAll(vb.tuples);
        for(VersionBlock vb: d2) data2.addAll(vb.tuples);
        data1.sort(new Comparator<TSTuple>() {
            @Override
            public int compare(TSTuple tsTuple, TSTuple t1) {
                return tsTuple.timestamp < t1.timestamp? -1: 1;
            }
        });
        data2.sort(new Comparator<TSTuple>() {
            @Override
            public int compare(TSTuple tsTuple, TSTuple t1) {
                return tsTuple.timestamp < t1.timestamp? -1: 1;
            }
        });
        data1 = update(data1);
        data2 = update(data2);
        return align2Series(data1, data2);
        //return null;
    }

    public List<TSTuple> update(List<TSTuple> ans1) {
        List<TSTuple> ans = new ArrayList<>();
        if(ans1.size() == 0) return ans;
        TSTuple tmp = ans1.get(0);
        for(int i=1;i<ans1.size();i++) {
            TSTuple curr = ans1.get(i);
            if(curr.timestamp != tmp.timestamp) ans.add(tmp);
            tmp = curr;
        }
        ans.add(tmp);
        return ans;
    }

    public List<TSTuple> align2Series(List<TSTuple> data1, List<TSTuple> data2) {
        int pos1=0, pos2 = 0;
        List<TSTuple> ans1 = new ArrayList<>(), ans = new ArrayList<>();
        while(pos1 < data1.size() && pos2 < data2.size()) {
            // add with version order.
            if(data1.get(pos1).timestamp < data2.get(pos2).timestamp) { pos1 ++; continue;}
            if(data1.get(pos1).timestamp > data2.get(pos2).timestamp) { pos2 ++; continue;}
            if(data1.get(pos1).timestamp == data2.get(pos2).timestamp)
            { ans1.add(data1.get(pos1)); pos1++; pos2++;}
        }
        //if(ans1.size() == 0) return ans1;

        return ans1;
    }

    public List<TSTuple> execute(List<Instruct> instructions) {
        // greedy
        // valueFilter: sel=1.0

        return null;
    }

    public List<TSTuple> valueFilter(List<VersionBlock> blks, ValueFilter vf) throws InterruptedException {
        // as the last node
        long prevED = blks.get(0).endTS;
        Queue<Thread> que = new ArrayDeque<>();
        for(VersionBlock vb: blks) {
            //if(prevED < vb.startTS) { // seq
            //    Thread th = new Thread(new Runnable() {
            //        @Override
            //        public void run() {
            //            vb.loadValSafeFilter(vb.encVPath, vf);
            //        }
            //    });
            //    th.start();
            //    que.add(th);
            //}
            //else {
                // TODO unseq, now inconsistent, use consistent query instead
                Thread th = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        vb.loadValFilter(vb.encVPath, vf);
                    }
                });
                th.start();
                que.add(th);
            //}
        }
        for(Thread th: que) th.join();
        List<TSTuple> ans = new ArrayList<>();
        for(VersionBlock vb: blks) ans.addAll(vb.tuples);
        return ans;
    }

    public List<TSTuple> executeQ2ValueFilter(String seriesName, Double lb, Double ub) throws InterruptedException {
        return valueFilter(this.data.get(this.series.lastIndexOf(seriesName)), new ValueFilter(seriesName, lb, ub));
    }

    public List<TSTuple> executeQ2ValueFilterNaive(String seriesName, Double lb, Double ub) {
        naiveDec(seriesName);
        List<VersionBlock> d = this.data.get(series.lastIndexOf(seriesName));
        List<TSTuple> data = new ArrayList<>();
        for(VersionBlock vb: d) data.addAll(vb.tuples);
        data.sort(new Comparator<TSTuple>() {
            @Override
            public int compare(TSTuple tsTuple, TSTuple t1) {
                return tsTuple.timestamp < t1.timestamp? -1: 1;
            }
        });
        data = update(data);
        return data.stream().filter(new Predicate<TSTuple>() {
            @Override
            public boolean test(TSTuple tsTuple) {
                return tsTuple.value >= lb && tsTuple.value < ub;
            }
        }).collect(Collectors.toList());
    }

    public List<TSTuple> Q3rangeQuery(String seriesName, long from, long to) throws InterruptedException {
        List<VersionBlock> d = this.data.get(series.lastIndexOf(seriesName));
        Queue<Thread> que = new ArrayDeque<>();
        for(VersionBlock vb: d) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    vb.rangeFilter(from, to);
                    vb.decAll();
                    //vb.tuples = new ArrayList<>();

                }
            });
            th.start();
            que.add(th);
        }
        for(Thread th: que) th.join();
        List<TSTuple> ans = new ArrayList<>();
        for(VersionBlock vb: d) ans.addAll(vb.tuples);
        ans.sort(new Comparator<TSTuple>() {
            @Override
            public int compare(TSTuple tsTuple, TSTuple t1) {
                return tsTuple.timestamp < t1.timestamp? -1: 1;
            }
        });
        if(!ans.isEmpty()) ans = update(ans);
        return ans;
    }

    public List<TSTuple> Q3rangeQueryNaive(String seriesName, long from, long to) {
        naiveDec(seriesName);
        List<VersionBlock> d = this.data.get(series.lastIndexOf(seriesName));
        List<TSTuple> data = new ArrayList<>();
        for(VersionBlock vb: d) data.addAll(vb.tuples);
        data.sort(new Comparator<TSTuple>() {
            @Override
            public int compare(TSTuple tsTuple, TSTuple t1) {
                return tsTuple.timestamp < t1.timestamp? -1: 1;
            }
        });
        data = update(data);
        List<TSTuple> ans = new ArrayList<>();
        for(TSTuple t: data) if(t.timestamp >= from && t.timestamp <= to) ans.add(t);
        return ans;
    }

    public List<TSTuple> Q4DownSamplingUpd(String seriesName) {
        List<VersionBlock> d = this.data.get(series.lastIndexOf(seriesName));
        long start = d.get(0).startTS;
        List<List<List<TSTuple>>> tr = d.stream().map(new Function<VersionBlock, List<List<TSTuple>>>() {
            @Override
            public List<List<TSTuple>> apply(VersionBlock versionBlock) {
                return versionBlock.overlappedTuples();
            }
        }).collect(Collectors.toList());
        final List<TSTuple> concat = new ArrayList<>();
        concat.addAll(tr.get(0).get(0));
        concat.addAll(tr.get(0).get(1));
        int i, j;
        for (i = 1; i < tr.size(); i++) {
            List<List<TSTuple>> part = tr.get(i);
            List<TSTuple> tmp = tr.get(i - 1).get(2);
            tmp.addAll(part.get(0));
            tmp.sort(new Comparator<TSTuple>() {
                @Override
                public int compare(TSTuple tsTuple, TSTuple t1) {
                    return tsTuple.timestamp < t1.timestamp ? -1 : 1;
                }
            });
            concat.addAll(tmp);
            concat.addAll(part.get(1));
        }
        concat.addAll(tr.get(tr.size() - 1).get(2));
        update(concat);
        return concat;
    }

    public List<TSTuple> Q4DownSamplingUpdNaive(String seriesName) {
        naiveDec(seriesName);
        List<VersionBlock> d = this.data.get(series.lastIndexOf(seriesName));
        List<TSTuple> data = new ArrayList<>();
        for(VersionBlock vb: d) data.addAll(vb.tuples);
        data.sort(new Comparator<TSTuple>() {
            @Override
            public int compare(TSTuple tsTuple, TSTuple t1) {
                return tsTuple.timestamp < t1.timestamp? -1: 1;
            }
        });
        data = update(data);
        return data;
    }

    public List<TSTuple> Q4DownSampling(List<TSTuple> concat, long start, Long windowSize, Long skip){
        int i,j;
        Queue<Thread> que = new ArrayDeque<>();
        List<TSTuple> res = new ArrayList<>();
        for(i=0; i<concat.size();i++) {
            TSTuple t = concat.get(i);
            if((t.timestamp-start)%(windowSize+skip) == 0) {
                double ans = 0d;
                for(j=i;j<concat.size();j++) {
                    TSTuple t2 = concat.get(j);
                    if(t2.timestamp-t.timestamp > windowSize) break;
                    if(t2.timestamp-t.timestamp <= 17) ans += t.value;
                    else ans += t2.value;
                }
                ans /= j;
                res.add(new TSTuple(t.timestamp, ans));
            }
        }
        return res;
    }

    public void addEdge(String group, String subGroupOrSeries) {
        if(this.graph.containsKey(group)) {
            List<String> exist = this.graph.get(group);
            exist.add(subGroupOrSeries);
            this.graph.replace(group, exist);
        }
        else {
            List<String> ans = new ArrayList<>();
            ans.add(subGroupOrSeries);
            this.graph.put(group, ans);
        }
    }

    public void Q5Batched(String seriesGroup, long start, Long windowSize, Long skip) throws InterruptedException {
        // level=1;
        Queue<Thread> que = new ArrayDeque<>();
        for(String series: this.graph.get(seriesGroup)) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    List<TSTuple> prec = Q4DownSamplingUpdNaive(series);
                    Q4DownSampling(prec, start, windowSize,skip);
                }
            });
            th.start();
            que.add(th);
        }
        for(Thread th: que) th.join();
    }

    public void Q5BatchedNaive(List<String> seriesName, long start, Long windowSize, Long skip) throws InterruptedException {
        for(String series: seriesName) {
            List<TSTuple> prec = Q4DownSamplingUpdNaive(series);
            Q4DownSampling(prec, start, windowSize, skip);
        }
    }

    public List<TSTuple> alignment(List<VersionBlock> data1, List<VersionBlock> data2) throws InterruptedException {
        Queue<Thread> que = new ArrayDeque<>();
        for(VersionBlock vb1: data1) {
            for(VersionBlock vb2: data2) {
                Thread th = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        vb1.decAll();
                        vb1.alignByDec(vb2);
                    }
                });
                th.start();
                que.add(th);
                Thread th2 = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        vb2.decAll();
                        vb2.alignByDec(vb1);
                    }
                });
                th2.start(); que.add(th2);
            }
        }
        for(Thread th: que) th.join();
        List<TSTuple> ans = data1.stream().map(new Function<VersionBlock, List<TSTuple>>() {

            @Override
            public List<TSTuple> apply(VersionBlock versionBlock) {
                return versionBlock.tuples;
            }
        }).flatMap(new Function<List<TSTuple>, Stream<TSTuple>>() {
            @Override
            public Stream<TSTuple> apply(List<TSTuple> tsTuples) {
                return tsTuples.stream();
            }
        }).collect(Collectors.toList());
        return update(ans);
    }
}
