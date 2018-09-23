import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
public class Avg_reviewReducer extends Reducer<Text, LongWritable , Text, FloatWritable> {
	
	//private LongWritable total = new LongWritable();

       @Override
    protected void reduce(Text token, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
    	   	FloatWritable result = new FloatWritable();
    	      Float average = 0f;
    	      Float count = 0f;
    	      int sum = 0;   
        //long n = 0;
        //Calculate sum of counts
        Text sumText = new Text("avegage");
        for (LongWritable val : values) {
            sum += val.get();
        }
        count += 1;
        average = sum/count;
        result.set(average);
        context.write(sumText, result);
        
        
//        for (LongWritable count : counts)
//            n += count.get();
//        total.set(n);
//
//        context.write(token, total);
    }
}
