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
 
public class Q7_5 { 
 
    // Mapper Class 
    public static class GenreMapper extends Mapper<Object, Text, Text, IntWritable> { 
         
        private final static IntWritable one = new IntWritable(1); 
        private Text genreKey = new Text("DramaAndRomanticMovies"); 
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
{ 
            // Split the CSV line by commas 
            String[] fields = value.toString().split(","); 
 
            if (fields.length == 3) { 
                String genres = fields[2]; // Assuming the genres are in the third column 
                 
                // Check if the genres contain both "Drama" and "Romantic" 
                if (genres.contains("Drama") && genres.contains("Romantic")) { 
                    // Emit the key "DramaAndRomanticMovies" with a count of 1 
                    context.write(genreKey, one); 
                } 
            } 
        } 
    } 
 
    // Reducer Class 
    public static class GenreReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
InterruptedException { 
            int sum = 0; 
            // Sum up all the counts for movies belonging to both genres 
            for (IntWritable val : values) { 
                sum += val.get(); 
            } 
            // Output the final count 
            context.write(key, new IntWritable(sum)); 
        } 
    } 
 
    public static void main(String[] args) throws Exception { 
        Configuration conf = new Configuration(); 
        Job job = Job.getInstance(conf, "drama and romantic movie count"); 
        job.setJarByClass(DramaRomanticMovieCount.class); 
        job.setMapperClass(GenreMapper.class); 
        job.setReducerClass(GenreReducer.class); 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input path on HDFS 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Output path on HDFS 
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
}