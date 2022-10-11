import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class align implements Instruct{

    List<String> param;

    public align(List<String> alignedSeriesName) {
        // load once is supported, afterwhile, the general support with release.
        // TODO: multiple involv of one series.
        param = alignedSeriesName;
    }

    @Override
    public double estimateSelectivity(List<List<VersionBlock>> data) {
        assert data.size() == 2;
        long st1 = data.get(0).get(0).startTS, st2 = data.get(1).get(0).startTS;
        long ed1 = data.get(0).get(data.get(0).size()-1).endTS;
        long ed2 = data.get(1).get(data.get(0).size()-1).endTS;
        return (double) Math.max(0d, Math.min(ed1, ed2)-Math.max(st1, st2))/(Math.max(ed1-st1, ed2-st2));
    }

    @Override
    public String instructName() {
        return "AL";
    }

    @Override
    public List<VersionBlock> execute(List<List<VersionBlock>> data) throws InterruptedException {
        // prefiltering for the pointed two
        List<VersionBlock> data1 = data.get(0), data2 = data.get(1);
        Queue<Thread> que = new ArrayDeque<>();
        for(VersionBlock vb1: data1) {
            for(VersionBlock vb2: data2) {
                Thread th = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        Utils.alignPreFiltering(vb1, vb2);
                    }
                });
                que.add(th);
            }
        }
        for(Thread th: que) th.join();
        return null;
    }

    @Override
    public List<String> getRelatedSeriesNames() {
        return this.param;
    }
}
