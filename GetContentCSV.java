import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class GetContentCSV {
  public static void main(String[] args) throws Exception {
	  Path filename = new Path("folder/arbres.csv");
	  Configuration conf = new Configuration();
	  FileSystem fs = FileSystem.get(conf);
	  FSDataInputStream inStream = fs.open(filename);
    try {
    	InputStreamReader isr = new InputStreamReader(inStream);
		BufferedReader br = new BufferedReader(isr);
		
		for (String line = br.readLine(); line != null; line = br.readLine()) {
            String[] data = line.split(";");
            System.out.println(data[5] + "\t" + data[6]);
        }
    } finally {
    	inStream.close();
		fs.close();
    }
  }
}
