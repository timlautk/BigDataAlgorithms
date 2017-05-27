import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordFrequenceInDocument extends Configured implements Tool {
    
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "Word Frequence In Document");
 
        job.setJarByClass(WordFrequenceInDocument.class);
        job.setMapperClass(WordFrequenceInDocMapper.class);
        job.setReducerClass(WordFrequenceInDocReducer.class);
        job.setCombinerClass(WordFrequenceInDocReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
 
    public static void main(String[] args) throws Exception {
    	int res = ToolRunner.run(new Configuration(), new WordFrequenceInDocument(), args);
        System.exit(res);
    }
}
