import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class Avg_reviewMapper extends Mapper<LongWritable,Text,Text, FloatWritable> {
	           
				private FloatWritable rate = new FloatWritable();
	    	    private Text tokenValue = new Text();
	    	    //private Text tokenValue2 = new Text();
	    	    
	    	
	    	 	    	
	      protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	        // As the first line of the data file named "will-ockenden-metadata.csv" contains attributes but not actual data
	        // the line should be excluded.
	       
	    	  if (key.get() == 0L) return;

	        // For the rest, we are going to compute total phone calls the person made per day.
	       // 
	         String productid = value.toString().split("\t")[3];
	         String rating = value.toString().split("\t")[7];
	                 
	        Float ratings= Float.parseFloat(rating) ;
	        
	        tokenValue.set(productid);
	        rate.set(ratings);
	       
	        context.write(tokenValue,rate);
	        
	       // tokenValue.set(Allreviews);
	        //context.write(tokenValue, ONE);
	      	        
	    }

}
