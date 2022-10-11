import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class process {

    public static void changeVersionLogToModify(String prefixPath, String name_ori, String nameLSM) throws IOException {
        String openFileVer = "./dataset/" + prefixPath +"/"+ name_ori + "-ver.csv";
        String newFileName = "./dataset/" + prefixPath +"/"+ nameLSM + ".csv";
        File f = new File(openFileVer);
        BufferedWriter writer = new BufferedWriter(new FileWriter(newFileName));
        Scanner sc = new Scanner(f);
        String header = "Time,root." + nameLSM + "\n";
        writer.write(header);
        while (sc.hasNext()) {
            String input = sc.nextLine();
            String[] res = input.split(",");
            String output = res[0]  + "," + res[2] + "\n";
            writer.write(output);
        }
        sc.close();
        writer.close();
    }

    public static void main(String[] args) throws IOException {
        //for(int i=2;i<=10;i++) {
        //    changeVersionLogToModify("climate_sc", "t_70rat=0." + i, "t70.sc" + i);
        //}
        changeVersionLogToModify("climate_s", "wdir_70", "wdir");
    }
}
