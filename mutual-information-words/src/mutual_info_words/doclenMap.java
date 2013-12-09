package mutual_info_words;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/**
 * @author aswinakhilesh
 * Map class which gets the input program line by line and returns the keyval pair(word,1), once. 
 */
public class doclenMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text("");

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(word, one);
	}
} 
