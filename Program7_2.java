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
 
public class Q7_2 { 
 
    public static class DocumentaryMapper extends Mapper<Object, Text, Text, IntWritable> { 
 
        private final static IntWritable one = new IntWritable(1); 
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        { 
            String[] fields = value.toString().split(","); 
            if (fields.length == 3) { 
                String title = fields[1]; 
                String genres = fields[2]; 
                 
                if (title.contains("(1995)") && genres.contains("Documentary")) { 
                    context.write(new Text("DocumentaryMovies1995"), one); 
                } 
            } 
        } 
    } 
 
    public static class DocumentaryReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
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
Job job = Job.getInstance(conf, "documentary 1995 count"); 
job.setJarByClass(Documentary1995Count.class); 
job.setMapperClass(DocumentaryMapper.class); 
job.setReducerClass(DocumentaryReducer.class); 
job.setOutputKeyClass(Text.class); 
job.setOutputValueClass(IntWritable.class); 
FileInputFormat.addInputPath(job, new Path(args[0])); 
FileOutputFormat.setOutputPath(job, new Path(args[1])); 
System.exit(job.waitForCompletion(true) ? 0 : 1); 
} 
} 
