import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Schema {
    public Map<String, Integer> attributes;
    List<String> attrName;

    public Map<String, Type> types = new HashMap<>();
    public String main;
    public String deviceId; // root.climate.K0
    public String key; // K0
    public List<String> measurements = new ArrayList<>();
    public Schema() {
        attributes = new HashMap<>();
        attrName = new ArrayList<>();
    }
}
