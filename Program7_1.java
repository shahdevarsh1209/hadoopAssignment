import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
 
import java.io.IOException; 
 
public class Q7_1 { 
 
    public static class ComedyMapper extends Mapper<Object, Text, Text, Text> { 
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
{ 
            String[] fields = value.toString().split(","); 
            if (fields.length == 3 && fields[2].contains("Comedy")) { 
                context.write(new Text(fields[0]), value); 
            } 
        } 
    } 
 
    public static class ComedyReducer extends Reducer<Text, Text, Text, Text> { 
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
InterruptedException { 
            for (Text val : values) { 
                context.write(key, val); 
            } 
        } 
    } 
 
    public static void main(String[] args) throws Exception { 
        Configuration conf = new Configuration(); 
Job job = Job.getInstance(conf, "comedy movies"); 
job.setJarByClass(ComedyMovies.class); 
job.setMapperClass(ComedyMapper.class); 
job.setReducerClass(ComedyReducer.class); 
job.setOutputKeyClass(Text.class); 
job.setOutputValueClass(Text.class); 
FileInputFormat.addInputPath(job, new Path(args[0])); 
FileOutputFormat.setOutputPath(job, new Path(args[1])); 
System.exit(job.waitForCompletion(true) ? 0 : 1); 
} 
} 
