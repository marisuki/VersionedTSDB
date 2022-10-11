import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

public class Utils {
    public static List<TuplePos> alignEncDecTime(List<TSTuple> sortedData, List<Ts2diffEncData> encTime) {
        int pos = 0, pos2 = 0;
        Ts2diffEncData encLast = encTime.get(encTime.size()-1);
        long encEnd = encLast.start + (long) encLast.delta *encLast.length;
        List<TuplePos> ans = new ArrayList<>();
        if (sortedData.get(0).timestamp > encEnd || sortedData.get(sortedData.size()-1).timestamp < encTime.get(0).start) return ans;
        while(pos<sortedData.size() && sortedData.get(pos).timestamp<encTime.get(0).start) pos ++;
        //while(pos<sortedData.size() && pos2 < ) {

        //}
        return null;
    }

    public static int estimateEncDecTime(List<TSTuple> sortedData, List<Ts2diffEncData> encTime) {
        int pos = 0, end = sortedData.size()-1;
        Ts2diffEncData encLast = encTime.get(encTime.size()-1);
        long encEnd = encLast.start + (long) encLast.delta *encLast.length;
        List<TuplePos> ans = new ArrayList<>();
        if (sortedData.get(0).timestamp > encEnd || sortedData.get(sortedData.size()-1).timestamp < encTime.get(0).start)
            return 0;
        while(pos <  sortedData.size() && sortedData.get(pos).timestamp<encTime.get(0).start) pos ++;
        while(end >= 0 && encEnd < sortedData.get(end).timestamp)  end --;
        int minFreq = encTime.stream().map(new Function<Ts2diffEncData, Integer>() {

            @Override
            public Integer apply(Ts2diffEncData ts2diffEncData) {
                return ts2diffEncData.delta;
            }
        }).min(Comparator.naturalOrder()).get();
        //return
        return (int) Math.min(end-pos+1, (sortedData.get(end).timestamp - sortedData.get(pos).timestamp)/(minFreq));
    }

    public static int estimate2EncTime(List<Ts2diffEncData> encTime1, List<Ts2diffEncData> encTime2) {
        Ts2diffEncData encLast1 = encTime1.get(encTime1.size()-1);
        Ts2diffEncData encLast2 = encTime2.get(encTime2.size()-1);
        long encEnd1 = encLast1.start + (long) encLast1.delta *encLast1.length;
        long encEnd2 = encLast2.start + (long) encLast2.delta *encLast2.length;
        int minFreq1 = encTime1.stream().map(new Function<Ts2diffEncData, Integer>() {

            @Override
            public Integer apply(Ts2diffEncData ts2diffEncData) {
                return ts2diffEncData.delta;
            }
        }).min(Comparator.naturalOrder()).get();
        int minFreq2 = encTime2.stream().map(new Function<Ts2diffEncData, Integer>() {

            @Override
            public Integer apply(Ts2diffEncData ts2diffEncData) {
                return ts2diffEncData.delta;
            }
        }).min(Comparator.naturalOrder()).get();
        return (int) ((Math.min(encEnd1, encEnd2) - Math.max(encTime1.get(0).start, encTime2.get(0).start))/(Math.max(minFreq1, minFreq2)));
    }

    public static AllPos estimate2DecTime(List<TSTuple> sortedData1, List<TSTuple> sortedData2) {
        int st1 =0, st2=0,ed1=0,ed2=0;
        if(sortedData1.get(0).timestamp > sortedData2.get(sortedData2.size()-1).timestamp ||
        sortedData1.get(sortedData1.size()-1).timestamp < sortedData2.get(0).timestamp) return null;
        if(sortedData1.get(st1).timestamp>sortedData2.get(st2).timestamp) {
            while(st2 < sortedData2.size() && sortedData1.get(st1).timestamp > sortedData2.get(st2).timestamp) st2++;
        } else {
            while(st1 < sortedData1.size() && sortedData1.get(st1).timestamp < sortedData2.get(st2).timestamp) st1++;
        }
        if(sortedData1.get(ed1).timestamp>sortedData2.get(ed2).timestamp) {
            while(ed1 >= 0 && sortedData1.get(ed1).timestamp > sortedData2.get(ed2).timestamp) ed1--;
        } else {
            while(ed2 >= 0 && sortedData1.get(ed1).timestamp < sortedData2.get(ed2).timestamp) ed2--;
        }
        return new AllPos(st1, st2, ed1, ed2);
    }

    public static void alignPreFiltering(VersionBlock vb1, VersionBlock vb2) {
        vb1.alignByRangeQueryPrecompute(vb2);
        vb2.alignByRangeQueryPrecompute(vb1);
    }

    public static void applyFilterToDec(VersionBlock vb) {
        vb.decAll();
    }

    public static void applyAlign(VersionBlock vb1, VersionBlock vb2) {
        vb1.alignByDec(vb2);
    }

    public class TuplePos {
        TSTuple t1; int corrPos;
    }


}
