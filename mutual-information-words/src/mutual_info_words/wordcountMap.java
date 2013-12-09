package mutual_info_words;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 
 * @author aswinakhilesh
 *This class maps each word of a line with the first string word of the line. This is because the input file format is:
 *DOC-ID word1 word2 word3, where the first word is the document name and the words that follow the document name occur
 *within the same document, identified by the first word of the line. This is to find out which words occur in which document.
 */

public class wordcountMap extends Mapper<LongWritable, Text, Text, Text> {

	private Text word = new Text("");
	private Text doc = new Text("");

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String arr[] = value.toString().split(" ", 2);
		String docName = arr[0];
		String rest = arr[1];
		StringTokenizer tokenizer = new StringTokenizer(rest);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());    	
			doc.set(docName);
			context.write(word, doc);
		}
	}

} 