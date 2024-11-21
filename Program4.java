import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
 
import java.io.IOException; 
import java.util.StringTokenizer; 
 
public class Q4 { 
 
     
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> { 
 
        private final static IntWritable one = new IntWritable(1); 
        private final static Text tokenKey = new Text("TokenCount"); 
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
{ 
            StringTokenizer itr = new StringTokenizer(value.toString()); 
            while (itr.hasMoreTokens()) { 
                String token = itr.nextToken(); 
                if (token.length() >= 4) { 
                    context.write(tokenKey, one);  // Emit 'TokenCount' key with value 1 
                } 
            } 
        } 
    } 
 
    
    public static class TokenCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
        private IntWritable result = new IntWritable(); 
 
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
InterruptedException { 
            int sum = 0; 
            for (IntWritable val : values) { 
                sum += val.get(); 
            } 
            result.set(sum); 
            context.write(new Text("Total count for token ="), result);  // Emit total token count 
        } 
    } 
 
    public static void main(String[] args) throws Exception { 
        Configuration conf = new Configuration(); 
        Job job = Job.getInstance(conf, "token length count"); 
        job.setJarByClass(TokenLengthCount.class); 
        job.setMapperClass(TokenizerMapper.class); 
        job.setReducerClass(TokenCountReducer.class); 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input path 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Output path 
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
} 