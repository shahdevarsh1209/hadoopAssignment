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
 
public class Q7_3 { 
 
    public static class MissingGenreMapper extends Mapper<Object, Text, Text, IntWritable> { 
 
        private final static IntWritable one = new IntWritable(1); 
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
{ 
            String[] fields = value.toString().split(","); 
            if (fields.length < 3 || fields[2].isEmpty()) { 
                context.write(new Text("MissingGenres"), one); 
            } 
        } 
    } 
 
    public static class MissingGenreReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
InterruptedException { 
            int sum = 0; 
            for (IntWritable val : values) { 
                sum += val.get(); 
            } 
            context.write(key, new IntWritable(sum)); 
        } 
    } 
 
    public static void main(String[] args) throws Exception { 
Configuration conf = new Configuration(); 
Job job = Job.getInstance(conf, "missing genres count"); 
job.setJarByClass(MissingGenreCount.class); 
job.setMapperClass(MissingGenreMapper.class); 
job.setReducerClass(MissingGenreReducer.class); 
job.setOutputKeyClass(Text.class); 
job.setOutputValueClass(IntWritable.class); 
FileInputFormat.addInputPath(job, new Path(args[0])); 
FileOutputFormat.setOutputPath(job, new Path(args[1])); 
System.exit(job.waitForCompletion(true) ? 0 : 1); 
} 
}