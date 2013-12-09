package mutual_info_words;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * 
 * @author aswinakhilesh
 *From each line that goes to the Map phase, the reduce phase gets the same key(""), and the same value(1). 
 *Hence, all the 1's will get partitioned together and the reduce will get the all the values (1) in a list, for its 
 *only key (""). It takes the sum of all the 1's, which is nothing but the number of lines. 
 */
public class doclenReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
