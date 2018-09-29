import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
public class Avg_reviewReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	
	private FloatWritable result = new FloatWritable();

       @Override
    protected void reduce(Text token, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
    	   	 FloatWritable result = new FloatWritable();
    	      Float average = 0f;
    	      int count = 0;
    	      float sum = 0;   
        //long n = 0;
        //Calculate sum of counts
        //Text sumText = new Text("average");
    	    
        for (FloatWritable val : values) {
        	sum += val.get();
            count += 1;
        }
        
        average = sum/count;
        result.set(average);
        context.write(token, result);
        
        
//        for (LongWritable count : counts)
//            n += count.get();
//        total.set(n);
//
//        context.write(token, total);
    }
}
