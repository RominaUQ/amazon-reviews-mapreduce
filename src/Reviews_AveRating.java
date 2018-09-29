import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
	import org.apache.hadoop.util.Tool;
	import org.apache.hadoop.util.ToolRunner;

public class Reviews_AveRating extends Configured implements Tool {

		
	    public static void main(String[] args) throws Exception {
	        System.exit(ToolRunner.run(new Reviews_AveRating(), args));
	    }

	   	    public int run(String[] args) throws Exception {
	        Configuration configuration = getConf();

	        //configuration.set("mapreduce.job.jar");
	        //Initialising Map Reduce Job
	       
	        Job job = new Job(configuration, "Amazon Average Rating");
	        String inputPath= "s3://amazon-reviews-pds/tsv/amazon_reviews_us_Books_v1_02.tsv.gz";
	        FileInputFormat.addInputPath(job, new Path(inputPath));

	        String outputPath= "/tmp/results/";
	        
	        FileSystem fs = FileSystem.newInstance(configuration);
	        if (fs.exists(new Path(outputPath))) {
	        	fs.delete(new Path(outputPath), true);
	        }
	        
	         FileOutputFormat.setOutputPath(job, new Path(outputPath));

	        //Set Map Reduce main jobconf class
	        job.setJarByClass(Reviews_AveRating.class);

	        //Set Mapper class
	        job.setMapperClass(Avg_reviewMapper.class);

	        //Set Combiner class
	       job.setCombinerClass(Avg_reviewReducer.class);

	        //set Reducer class
	        job.setReducerClass(Avg_reviewReducer.class);
	        job.setNumReduceTasks(1);

	        //set Input Format
	        job.setInputFormatClass(TextInputFormat.class);

	        //set Output Format
	         job.setOutputFormatClass(TextOutputFormat.class);

	        //set Output key class
	        job.setOutputKeyClass(Text.class);

	        //set Output value class
	        job.setOutputValueClass(FloatWritable.class);
	        
	        return job.waitForCompletion(true) ? 0 : -1;
	    }
	}

