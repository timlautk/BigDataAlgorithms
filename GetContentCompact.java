import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


public class GetContentCompact {
	
	
	public static void main(String[] args) throws Exception {
		
		Path filename = new Path("question2_8/isd-history.txt");

		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		
		try{
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			int num = 0;
            for (String line = br.readLine(); line != null; line = br.readLine())
            {
                num++;
                if (num > 22)
                {
        			// fetching the USAF code
        			String uasf = line.substring(0, 6);
        			// fetching the name
        			String name = line.substring(13, 42);
        			// fetching the FIPS
        			String fips = line.substring(43, 45);
        			// fetching the altitude
        			String alt = line.substring(74, 81);
        			// Process of the current line
        			System.out.println(uasf + "\t" + name + "\t" + fips + "\t" +  alt);
        			} 
                }
		}finally{
			//close the file
			inStream.close();
			fs.close();
		}		
	}
}
