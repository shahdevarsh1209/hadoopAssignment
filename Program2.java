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
 
public class Q2 { 
 
     
    public static class TempMapper extends Mapper<Object, Text, Text, IntWritable> { 
 
        private Text year = new Text(); 
        private IntWritable temperature = new IntWritable(); 
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
{ 
            
            String[] parts = value.toString().split("\\s+"); 
            if (parts.length == 2) { 
                
                year.set(parts[0]);  // Set the year as the key 
                temperature.set(Integer.parseInt(parts[1]));  // Set the temperature as the value 
                context.write(year, temperature);  // Emit year as key and temperature as value 
            } 
        } 
    } 
 
     
    public static class MinTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
 
        private IntWritable minTemperature = new IntWritable(); 
 
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
InterruptedException { 
            int minTemp = Integer.MAX_VALUE; 
 
             
            for (IntWritable value : values) { 
                minTemp = Math.min(minTemp, value.get()); 
            } 
 
            minTemperature.set(minTemp); 
            context.write(key, minTemperature);  // Emit the year and the minimum temperature 
        } 
    } 
 
   
    public static void main(String[] args) throws Exception { 
        Configuration conf = new Configuration(); 
        Job job = Job.getInstance(conf, "min temperature"); 
        job.setJarByClass(MinTemperature.class); 
        job.setMapperClass(TempMapper.class); 
        job.setReducerClass(MinTempReducer.class); 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input path 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Output path 
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
} 