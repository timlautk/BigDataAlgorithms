import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
 
public class WordFrequenceInDocMapper 
	extends Mapper<LongWritable, Text, Text, IntWritable> {
 
    public WordFrequenceInDocMapper() {
    }
 
    /**
     * Google's search Stopwords
     */
    private static Set<String> googleStopwords;
 
    static {
        googleStopwords = new HashSet<String>();
        googleStopwords.add("I"); googleStopwords.add("a");
        googleStopwords.add("about"); googleStopwords.add("an");
        googleStopwords.add("are"); googleStopwords.add("as");
        googleStopwords.add("at"); googleStopwords.add("be");
        googleStopwords.add("by"); googleStopwords.add("com");
        googleStopwords.add("de"); googleStopwords.add("en");
        googleStopwords.add("for"); googleStopwords.add("from");
        googleStopwords.add("how"); googleStopwords.add("in");
        googleStopwords.add("is"); googleStopwords.add("it");
        googleStopwords.add("la"); googleStopwords.add("of");
        googleStopwords.add("on"); googleStopwords.add("or");
        googleStopwords.add("that"); googleStopwords.add("the");
        googleStopwords.add("this"); googleStopwords.add("to");
        googleStopwords.add("was"); googleStopwords.add("what");
        googleStopwords.add("when"); googleStopwords.add("where");
        googleStopwords.add("who"); googleStopwords.add("will");
        googleStopwords.add("with"); googleStopwords.add("and");
        googleStopwords.add("the"); googleStopwords.add("www");
    }
 
    public void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException {
        // Compile all the words using regex
        Pattern p = Pattern.compile("\\w+");
        Matcher m = p.matcher(value.toString());
 
        // Get the name of the file from the inputsplit in the context
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
 
        // build the values and write <k,v> pairs through the context
        StringBuilder valueBuilder = new StringBuilder();
        while (m.find()) {
            String matchedKey = m.group().toLowerCase();
            // remove names starting with non letters, digits, considered stopwords or containing other chars
            if (!Character.isLetter(matchedKey.charAt(0)) || Character.isDigit(matchedKey.charAt(0))
                    || googleStopwords.contains(matchedKey) || matchedKey.contains("_")) {
                continue;
            }
            valueBuilder.append(matchedKey);
            valueBuilder.append("@");
            valueBuilder.append(fileName);
            // emit the partial <k,v>
            context.write(new Text(valueBuilder.toString()), new IntWritable(1));
        }
    }
}
