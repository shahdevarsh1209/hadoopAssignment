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
 
public class Q5 { 
 
    // Mapper Class 
    public static class VoterMapper extends Mapper<Object, Text, Text, IntWritable> { 
 
        private final static IntWritable one = new IntWritable(1); 
        private final static Text femaleVoterKey = new Text("FemaleVoter"); 
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
{ 
            
            String[] fields = value.toString().split(","); 
             
            
            if (fields.length == 4 && fields[2].equalsIgnoreCase("Female")) { 
                context.write(femaleVoterKey, one);  // Emit 'FemaleVoter' key with value 1 
            } 
        } 
    } 
 
    // Reducer Class 
    public static class VoterReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
        private IntWritable result = new IntWritable(); 
 
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
InterruptedException { 
            int sum = 0; 
            // Sum up all the female voter counts 
            for (IntWritable val : values) { 
                sum += val.get(); 
            } 
            result.set(sum); 
            // Emit the total count of female voters 
            context.write(new Text("No. of female voters are :"), result); 
        } 
    } 
 
    public static void main(String[] args) throws Exception { 
        Configuration conf = new Configuration(); 
        Job job = Job.getInstance(conf, "female voter count"); 
        job.setJarByClass(FemaleVoterCount.class); 
        job.setMapperClass(VoterMapper.class); 
        job.setReducerClass(VoterReducer.class); 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input path 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Output path 
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
} 