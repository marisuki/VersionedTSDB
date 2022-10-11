import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public interface Instruct {
    //String instructName = null;
    //double costEstimated = 0;
    //List<Stream<TSTuple>> relatedSeries = new ArrayList<>();

    double estimateSelectivity(List<List<VersionBlock>> data);

    String instructName();

    List<VersionBlock> execute(List<List<VersionBlock>> data) throws InterruptedException;

    List<String> getRelatedSeriesNames();
}
