import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ValueFilter implements  Instruct{
    double geq, leq;
    String name;

    ValueFilter(String seriesName, double g, double l) {
        geq=g; leq=l;
        this.name = seriesName;
    }

    @Override
    public double estimateSelectivity(List<List<VersionBlock>> data) {
        return 0;
    }

    @Override
    public String instructName() {
        return "VF";
    }

    @Override
    public List<VersionBlock> execute(List<List<VersionBlock>> data) {
        return null;
    }

    @Override
    public List<String> getRelatedSeriesNames() {
        return new ArrayList<String>(Collections.singletonList(name));
    }

    public boolean test(Double value) {
        return value >= leq && value <= geq;
    }
}
