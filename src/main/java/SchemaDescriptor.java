import java.util.*;

public class SchemaDescriptor {
    public List<Schema> mainName;
    public List<Integer> size;
    public List<Integer> gen;
    public boolean singleBranch = true;
    public List<String> branchPostfix;
    public List<String> create = new ArrayList<>();
    public Map<String, List<String>> branches = new HashMap<>();
    public List<String> drop = new ArrayList<>();
    public final int gen_base = 1; // higher, concurr better for align.
    public final int step = 1;
    public int inst = 2;

    public Map<Integer, List<Integer>> alignedSchema = new HashMap<>(); // for IoTDB, tsid -> schemaid in mainName.

    public SchemaDescriptor(int num_branch, String name, String ev){
        singleBranch = num_branch == 1;
        mainName = new ArrayList<>();
        size = new ArrayList<>();
        gen = new ArrayList<>();
        branchPostfix = new ArrayList<>();
        for(int i=0;i<num_branch;i++) branchPostfix.add(""+i);
        switch (name) {
            case "Climate": {
                switch (ev) {
                    case "Postgres":
                    case "Postgres-v": build_climate_postgres(); break;
                    case "Influx": build_climate_influx(); break;
                    case "IoTDB":
                    case "IoTDB-v": build_climate_iotdb(); break;
                }
                break;
                }
            case "Ship": {
                inst = 10;
                switch (ev) {
                    case "Postgres":
                    case "Postgres-v": build_ship_postgres(); break;
                    case "Influx": build_ship_influx(); break;
                    case "IoTDB":
                    case "IoTDB-v": build_ship_iotdb(); break;
                }
                break;
            }
            case "Bitcoin": {
                switch (ev) {
                    case "Postgres":
                    case "Postgres-v": build_btc_postgres(); break;
                    case "Influx": build_btc_influx(); break;
                    case "IoTDB":
                    case "IoTDB-v": build_btc_iotdb(); break;
                }
                break;
            }
            case "Noise": {

                switch (ev) {
                    case "Postgres":
                    case "Postgres-v": build_noise_postgres(); break;
                    case "Influx": build_noise_influx(); break;
                    case "IoTDB":
                    case "IoTDB-v": build_noise_iotdb(); break;
                }
                break;
            }
        }
    }

    public static String random_select_update_table(List<String> x) {
        Random rd = new Random();
        return x.get(rd.nextInt(x.size()));
    }

    public void build_climate_postgres() {
        service_postgres1attr("Climate", 4, 2);
//        Schema sc1 = new Schema();
//        sc1.attributes.put("A", 1); sc1.main = "climate1"; sc1.types.put("A", Type.FLOAT);
//
//        Schema sc2 = new Schema();
//        sc2.attributes.put("B", 2); sc2.main = "climate2"; sc2.types.put("B", Type.FLOAT);
//
//        Schema sc3 = new Schema();
//        sc3.attributes.put("C", 3); sc3.main = "climate3"; sc3.types.put("C", Type.FLOAT);
//
//        Schema sc4 = new Schema();
//        sc4.attributes.put("D", 4); sc4.main = "climate4"; sc4.types.put("D", Type.FLOAT);
//        mainName.add(sc1); mainName.add(sc2); mainName.add(sc3); mainName.add(sc4);
//        sc1.attrName = List.copyOf(sc1.attributes.keySet());
//        sc2.attrName = List.copyOf(sc2.attributes.keySet());
//        sc3.attrName = List.copyOf(sc3.attributes.keySet());
//        sc4.attrName = List.copyOf(sc4.attributes.keySet());
//
//        String s1 = " (time integer not null, A numeric(32, 4) not null, primary key(time) );";
//        String s2 = " (time integer not null, B numeric(32, 4) not null, primary key(time) );";
//        String s3 = " (time integer not null, C numeric(32, 4) not null, primary key(time) );";
//        String s4 = " (time integer not null, D numeric(32, 4) not null, primary key(time) );";
//        create.add("create unlogged table climate1" + s1);
//        create.add("create unlogged table climate2" + s2);
//        create.add("create unlogged table climate3" + s3);
//        create.add("create unlogged table climate4" + s4);
//        drop.add("drop table climate1;");
//        drop.add("drop table climate2;");
//        drop.add("drop table climate3;");
//        drop.add("drop table climate4;");
//
//        branches.put(sc1.main, new ArrayList<>());
//        branches.put(sc2.main, new ArrayList<>());
//        branches.put(sc3.main, new ArrayList<>());
//        branches.put(sc4.main, new ArrayList<>());
//        for(String post: branchPostfix) {
//            create.add("create table climate1b" + post + s1);
//            branches.get(sc1.main).add("climate1b" + post);
//            create.add("create table climate2b" + post + s2);
//            branches.get(sc2.main).add("climate2b" + post);
//            create.add("create table climate3b" + post + s3);
//            branches.get(sc3.main).add("climate3b" + post);
//            create.add("create table climate4b" + post + s4);
//            branches.get(sc4.main).add("climate4b" + post);
//
//            drop.add("drop table " + "climate1b" + post + ";");
//            drop.add("drop table " + "climate2b" + post + ";");
//            drop.add("drop table " + "climate3b" + post + ";");
//            drop.add("drop table " + "climate4b" + post + ";");
//        }
//        gen.add(2); gen.add(1); gen.add(2); gen.add(1);
    }

    public void service_postgres1attr(String prefix, int attr_num, int inst) {
        for(int i=0;i<attr_num;i++) {
            Schema sc = new Schema();
            sc.attributes.put("K" + i, i + 1);
            sc.main = prefix + i;
            sc.types.put("K" + i, Type.FLOAT);
            mainName.add(sc);
            sc.attrName = List.copyOf(sc.attributes.keySet());
            String s = String.format(
                    " (time integer not null, %s numeric(32, 4) not null );",
                    "K" + i); //, primary key(time)
            create.add("create unlogged table " + sc.main + s);
            drop.add(String.format("drop table %s;", sc.main));
            branches.put(sc.main, new ArrayList<>());
            for(String post: branchPostfix) {
                String brh = sc.main + "b" + post;
                create.add("create table "+ brh + s);
                branches.get(sc.main).add(brh);
                drop.add("drop table " + brh + ";");
            }
            gen.add((i%inst)*step + gen_base);
        }
    }

    public void service_postgresAligned(String prefix, int instance, int attr_num_each) {
        for(int i=0;i<instance;i++) {
            Schema sc = new Schema();
            StringBuilder sb = new StringBuilder(" (time integer not null");
            for(int j=0;j<attr_num_each;j++) {
                sc.attributes.put("K" + i + "A" + j, attr_num_each * i + j + 1);
                sc.types.put("K" + i + "A" + j, Type.FLOAT);
                sb.append(String.format(", %s numeric(32, 4) not null", "K" + i + "A" + j));
            }
            sc.main = prefix + i;
            mainName.add(sc);
            sc.attrName = List.copyOf(sc.attributes.keySet());
            String s = sb + " );"; //, primary key(time)
            create.add("create unlogged table " + sc.main + s);
            drop.add(String.format("drop table %s;", sc.main));
            branches.put(sc.main, new ArrayList<>());
            for(String post: branchPostfix) {
                String brh = sc.main + "b" + post;
                create.add("create table "+ brh + s);
                branches.get(sc.main).add(brh);
                drop.add("drop table " + brh + ";");
            }
            gen.add(i);
        }
    }

    public void build_btc_postgres() {
        service_postgres1attr("Bitcoin", 7, 2);
    }
    public void build_noise_postgres() {
        service_postgres1attr("Noise", 4, 2);
    }
    public void build_ship_postgres() {
        service_postgres1attr("Ship", 210, 10);
    }

    public void service_influx(String prefix, int attr_num, int inst) {

    }

    public void build_climate_influx() {
        service_iotdb1attr("Climate", 4, 2);
    }
    public void build_ship_influx() {
        service_iotdb1attr("Ship", 210, 10);
    }
    public void build_btc_influx() {
        service_iotdb1attr("Bitcoin", 7, 2);
    }
    public void build_noise_influx() {
        service_iotdb1attr("Noise", 4, 2);
    }

    public void service_iotdb1attr(String prefix, int attr_num, int inst) {
        create.add("create database root;");
        drop.add("delete database root;");
        for(int i=0;i<attr_num;i++) {
            Schema sc = new Schema();
            sc.attributes.put("K" + i, i + 1);
            String pattern = "create timeseries root.%s.%s.%s with datatype=Int32, encoding=PLAIN";
            // IoTDb use qualification.
            String s = String.format(pattern,
                    prefix, "K" + i, "main");
            create.add(s);
            sc.main = String.format("root.%s.%s.%s", prefix, "K" + i, "main");
            sc.key = "K" + i;
            sc.deviceId = String.format("root.%s.%s", prefix, "K" + i);
            sc.types.put("K" + i, Type.FLOAT);
            mainName.add(sc);
            sc.attrName = List.copyOf(sc.attributes.keySet());
            branches.put(sc.main, new ArrayList<>());
            for(String post: branchPostfix) {
                String brh = "b" + post;
                sc.measurements.add(brh);
                create.add(String.format(pattern, prefix, "K"+i, brh));
                branches.get(sc.main).add(String.format("root.%s.%s.%s", prefix, "K" + i, brh));
            }
            gen.add(i%inst + 1);
            if(!alignedSchema.containsKey(i%inst + 1)) {
                alignedSchema.put(i%inst + 1, new ArrayList<>());
            }
            alignedSchema.get(i%inst + 1).add(mainName.size()-1);
        }
    }
    public void build_climate_iotdb() {
        service_iotdb1attr("Climate", 4, 2);
    }

    public void build_ship_iotdb() {
        service_iotdb1attr("Ship", 210, 10);
    }

    public void build_btc_iotdb() {
        service_iotdb1attr("Bitcoin", 7, 2);
    }
    public void build_noise_iotdb() {
        service_iotdb1attr("Noise", 4, 2);
    }
}
