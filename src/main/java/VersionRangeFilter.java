import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VersionRangeFilter implements Instruct {
    String instructName;
    //double costEstimated = 0;
    //List<Stream<TSTuple>> relatedSeries = new ArrayList<>();
    long lb, ub;
    String name;


    VersionRangeFilter(String seriesName, long lowb, long upb) {
        instructName = "TR";
        if(lowb==-1) lowb=0;
        if(upb==-1) upb=0x7f7f7f7f;
        this.lb = lowb;
        this.ub = upb;
        this.name = seriesName;
    }

    @Override
    public double estimateSelectivity(List<List<VersionBlock>> data) {
        assert data.size() == 1;
        double ans = data.get(0).stream().map(new Function<VersionBlock, Double>() {
            @Override
            public Double apply(VersionBlock versionBlock) {
                long rst = Math.max(versionBlock.startTS, lb);
                long red = Math.max(versionBlock.endTS, ub);
                if(rst >= red) return 0d;
                return (double) (red - rst) /(versionBlock.endTS-versionBlock.startTS);
            }
        }).reduce(new BinaryOperator<Double>() {
            @Override
            public Double apply(Double aDouble, Double aDouble2) {
                return aDouble+aDouble2;
            }
        }).get();
        return ans /data.get(0).size();
    }

    @Override
    public String instructName() {
        return "TR";
    }

    @Override
    public List<VersionBlock> execute(List<List<VersionBlock>> data) {
        assert data.size() == 1;
        return data.get(0).stream().map(new Function<VersionBlock, VersionBlock>() {
            @Override
            public VersionBlock apply(VersionBlock versionBlock) {
                versionBlock.rangeFilter(lb, ub);
                return versionBlock;
            }
        }).collect(Collectors.toList());
    }

    @Override
    public List<String> getRelatedSeriesNames() {
        return new ArrayList<String>(Collections.singletonList(name));
    }
}
