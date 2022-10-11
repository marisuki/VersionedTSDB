import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;

public class statistic {
    public final double plus = 10d;
    public int cntx;

    public Map<Integer, Double> stat(String filePath) throws FileNotFoundException {
        File f = new File(filePath);
        Scanner sc = new Scanner(f);
        Map<Integer, Double> ans = new HashMap<>();
        int cnt = 0;
        while (sc.hasNext()) {
            String line = sc.nextLine();
            String[] res = line.split(",");
            if(res[0].equals("Time")) continue;
            double x = Double.parseDouble(res[1]);
            Integer s = (int) Math.ceil(x*plus);
            //Integer prec = Math.ceil(x*10.0);
            if(ans.containsKey(s)) ans.replace(s, ans.get(s) + 1);
            else ans.put(s, 1d);
            cnt ++;
        }
        cntx = cnt;
        //for(Integer x: ans.keySet()) ans.replace(x, ans.get(x));
        return ans;
    }

    public List<Double> findPivot(Map<Integer, Double> in, int count) {
        List<Integer> vals = new ArrayList<>(in.keySet());
        vals.sort(Comparator.naturalOrder());
        List<Double> pivot = new ArrayList<>();
        int prev = 0; double tot=0;
        pivot.add(vals.get(0)*1.0/this.plus);
        for(int i=0;i<vals.size();i++) {
            Integer x = vals.get(i);
            tot += in.get(x);
            int ratio = (int) Math.floor(tot*10.0 /count);
            if(ratio > prev) pivot.add(x*1.0/this.plus);
            prev=  ratio;
        }
        //pivot.add(vals.get(vals.size()-1)*1.0/this.plus);
        return pivot;
    }

    public List<Long> findPivotTime(String filePath) throws FileNotFoundException {
        File f = new File(filePath);
        Scanner sc = new Scanner(f);
        Map<Integer, Double> ans = new HashMap<>();
        int cnt = 0;
        long min=-1, max=-1;
        while (sc.hasNext()) {
            String line = sc.nextLine();
            String[] res = line.split(",");
            if(res[0].equals("Time")) continue;
            long t = Long.parseLong(res[0]);
            if(min == -1) min = t;
            min = Math.min(min, t);
            max = Math.max(max, t);
            //Integer prec = Math.ceil(x*10.0);
            cnt ++;
        }
        cntx = cnt;
        List<Long> res = new ArrayList<>();
        for(int i=0;i<=10;i++) {
            res.add(min + (max-min)*i/10);
        }
        //for(Integer x: ans.keySet()) ans.replace(x, ans.get(x));
        return res;
    }

    public static void main(String[] args) throws FileNotFoundException {
        statistic s = new statistic();
        List<Double> ans = s.findPivot(s.stat("./dataset/climate_sc/t70.sc5.csv"), s.cntx);
        for(Double x: ans) System.out.println(x);
    }
}
