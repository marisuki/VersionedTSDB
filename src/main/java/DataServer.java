import java.io.*;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DataServer {
    public List<Timestamp> timeline = new ArrayList<>();
    public List<List<Double>> values = new ArrayList<>();
    public final int timeInterval = 2;

    public DataServer(String path, List<String> subsets,
                      boolean ignoreFirstTitle, List<Integer> series,
                      int lengths, List<String> t0) throws FileNotFoundException {
        for (int i=0;i<lengths;i++) values.add(new ArrayList<>());
        int fileId = 0;
        for(String file: subsets) {
            String filePath = path+file+".csv";
            File f = new File(filePath);
            Scanner sc = new Scanner(f);
            Timestamp ts = Timestamp.valueOf(t0.get(fileId));
            fileId += 1;
            long time = ts.getTime();
            int pos = 0;
            while(sc.hasNext()) {
                String line = sc.nextLine();
                if(ignoreFirstTitle &&pos==0) {pos+=1; continue;}

                String[] s = line.split(",");
                //DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd hh:mm:ss");


                if (timeline.size() == 0) {
                    timeline.add(ts);
                    for(int i=1;i<s.length;i++) {
                        if (series.contains(i))
                            values.get(i).add(Double.parseDouble(s[i]));
                    }
                }
                else {
                    // spline for more data
                    long lastT = timeline.get(timeline.size()-1).getTime()+timeInterval;
                    long delta = time-lastT;

                    for(long t = lastT; t<= time; t+=timeInterval) {
                        timeline.add(new Timestamp(t));
                        for(int i=1;i<s.length;i++) {
                            if (!series.contains(i)) continue;
                            double curr = Double.parseDouble(s[i]);
                            //System.out.println(curr);
                            List<Double> tmp = values.get(i);
                            double last = tmp.get(tmp.size()-1);
                            if (series.contains(i))
                                values.get(i).add(curr*((Double)((time-t)/(1.0*delta))) + last*((Double)((t-lastT)/(1.0*delta))));
                        }
                    }
                }
                time += 300;
            }
        }
    }

    public void writeFile(Integer series, String prefixPath, String name,
                          long st, long ed, double drop,
                          double later, double upd, boolean addVersion) throws IOException {
        String filename = "./dataset/" + prefixPath + "/"+name + ".csv";
        String fileVer = "./dataset/" + prefixPath +"/"+name + "-ver.csv";
        String metaname = "./dataset/" + prefixPath +"/"+name + "-meta.csv";
        String update = "./dataset/" + prefixPath +"/"+name + "-upd.csv";
        BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
        BufferedWriter verw = new BufferedWriter(new FileWriter(fileVer));
        BufferedWriter metaw = new BufferedWriter(new FileWriter(metaname));
        BufferedWriter updw = new BufferedWriter(new FileWriter(update));

        metaw.write("T,v,"+name + "\n");
        metaw.close();

        List<Double> data = values.get(series);
        Random rd = new Random();
        Map<Integer, List<Integer>> laterValue = new HashMap<>();
        Map<Integer, List<Integer>> updValue = new HashMap<>();
        //long stTime = timeline.get(0).getTime();
        int revTime = 0;
        for(int i=0;i<data.size();i++) {
            long currTime = timeline.get(i).getTime();
            if(st!=-1 && (currTime < st || currTime > ed)) continue;
            double rand = rd.nextDouble();
            revTime += timeInterval;

            if(rand < drop) continue;
            if(rand < later) {
                int tupRevTime = revTime + rd.nextInt(300);
                if(laterValue.containsKey(tupRevTime)) laterValue.get(tupRevTime).add(i);
                else {
                    List<Integer> tmp = new ArrayList<>();
                    tmp.add(i);
                    laterValue.put(tupRevTime, tmp);
                }
                continue;
            }
            if(rand < upd) {
                // pr=upd-later
                int tupRevTime = revTime + rd.nextInt(300);
                if(updValue.containsKey(tupRevTime)) updValue.get(tupRevTime).add(i);
                else {
                    List<Integer> tmp = new ArrayList<>();
                    tmp.add(i);
                    updValue.put(tupRevTime, tmp);
                }
                continue;
            }
            // writeCurr
            double currVal = data.get(i);
            writer.write(compose(currTime, currVal, revTime, false));
            if(addVersion) {
                verw.write(compose(currTime, currVal, revTime, true));
            }

            //find rev
            if (laterValue.containsKey(revTime)) {
                for(int id: laterValue.get(revTime)) {
                    writer.write(compose(timeline.get(id).getTime(), values.get(series).get(id), revTime, false));
                    if(addVersion) {
                        verw.write(compose(timeline.get(id).getTime(), values.get(series).get(id), revTime, true));
                    }
                }
                laterValue.remove(revTime);
            }

            if (updValue.containsKey(revTime)) {
                for(int id: updValue.get(revTime)) {
                    updw.write(compose(timeline.get(id).getTime(), rd.nextDouble(), revTime, false));
                    if(addVersion) {
                        verw.write(compose(timeline.get(id).getTime(), rd.nextDouble(), revTime, true));
                    }
                }
                updValue.remove(revTime);
            }


        }
        writer.close();
        verw.close();
        updw.close();
    }

    private String compose(long time, double val, long version, boolean addVer) {
        if(addVer) return time + "," + version + "," + val + "\n";
        return time + "," + val + "\n";
    }

    private void variedSelectivity(){
        // generate
    }

    private void variedScalability(int series, String name) throws IOException {
        // generate 10 files
        long st = timeline.get(0).getTime();
        long ed = timeline.get(timeline.size()-1).getTime();
        for(int i=1;i<=10;i++) {
            writeFile(series, "climate_sc", name + "rat=0." + i, st, st+((ed-st)/10)*i, 0.05, 0.15, 0.20, true);
        }
    }

    public static void main(String[] args) throws IOException {
        List<String> subset = new ArrayList<>();
        subset.add("1");//subset.add("2");//subset.add("32");//subset.add("33");subset.add("34");

        List<String> time = new ArrayList<>();
        time.add("2017-05-22 00:00:00");
        //time.add("2019-05-22 00:00:00");


        List<Integer> consideredSeries = new ArrayList<>();
        consideredSeries.add(31);//consideredSeries.add(32);//consideredSeries.add(31);
        DataServer ds = new DataServer("./dataset/climate/climate-",subset,true,consideredSeries,35, time);

        //ds.writeFile(30, "climate_s","rhoair_70", -1, -1, 0.05, 0.15, 0.20, true);
        ds.writeFile(31, "climate_s","wdir_70", -1, -1, 0.05, 0.15, 0.20, true);

        //ds.variedScalability(32, "t_70");
    }
}
