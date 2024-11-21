import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
 
import java.io.IOException; 
 
public class Q7_4 { 
 
    // Mapper Class 
    public static class GoldMovieMapper extends Mapper<Object, Text, Text, Text> { 
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
{ 
            // Split the CSV line by commas 
            String[] fields = value.toString().split(","); 
             
            if (fields.length > 1) { 
                String title = fields[1]; // Assuming the title is in the second column 
                // Check if the title contains "Gold" 
                if (title.contains("Gold")) { 
                    // Emit the movie title if it contains "Gold" 
                    context.write(new Text(title), new Text("")); 
                } 
            } 
        } 
    } 
 
    // Reducer Class 
    public static class GoldMovieReducer extends Reducer<Text, Text, Text, Text> { 
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
InterruptedException { 
            // Directly write the movie title to the output 
            context.write(key, new Text("")); 
        } 
} 
public static void main(String[] args) throws Exception { 
Configuration conf = new Configuration(); 
Job job = Job.getInstance(conf, "gold movie titles"); 
job.setJarByClass(GoldMovies.class); 
job.setMapperClass(GoldMovieMapper.class); 
job.setReducerClass(GoldMovieReducer.class); 
job.setOutputKeyClass(Text.class); 
job.setOutputValueClass(Text.class); 
FileInputFormat.addInputPath(job, new Path(args[0]));  // Input path on HDFS 
FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Output path on HDFS 
System.exit(job.waitForCompletion(true) ? 0 : 1); 
} 
} 
