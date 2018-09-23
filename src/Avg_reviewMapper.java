import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Avg_reviewMapper extends Mapper<LongWritable, Text,Text, LongWritable> {
	 final private static LongWritable ONE = new LongWritable(1);
	    	    private Text tokenValue = new Text();

	      protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	        // As the first line of the data file named "will-ockenden-metadata.csv" contains attributes but not actual data
	        // the line should be excluded.
	        if (key.get() == 0L) return;

	        // For the rest, we are going to compute total phone calls the person made per day.
	        String reviews = value.toString().split("\t")[12];

	        tokenValue.set(reviews);
	        context.write(tokenValue, ONE);
	      	        
	    }

}
