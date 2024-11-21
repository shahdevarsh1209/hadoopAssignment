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
 
public class AverageCount { 
 
     
    public static class SumMapper extends Mapper<Object, Text, Text, IntWritable> { 
        private final static Text tokenKey = new Text("TokenCount"); 
        private IntWritable count = new IntWritable(); 
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
{ 
            String[] parts = value.toString().split("\\s+"); 
            if (parts.length == 2) { 
                count.set(Integer.parseInt(parts[1])); 
                context.write(tokenKey, count);  // Emit a single key for summing all counts 
            } 
        } 
    } 
 
     
    public static class AverageReducer extends Reducer<Text, IntWritable, Text, Text> { 
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
InterruptedException { 
            int sum = 0; 
            int count = 0; 
 
            for (IntWritable value : values) { 
                sum += value.get(); 
                count++; 
            } 
 
            double average = (double) sum / count;  // Calculate the average count 
            context.write(new Text("AverageCount = "), new Text(String.valueOf(average))); 
        } 
    } 
 
    public static void main(String[] args) throws Exception { 
        Configuration conf = new Configuration(); 
        Job job = Job.getInstance(conf, "average count"); 
        job.setJarByClass(AverageCount.class); 
        job.setMapperClass(SumMapper.class); 
        job.setReducerClass(AverageReducer.class); 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input path 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Output path 
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
} 
} 
