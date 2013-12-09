package mutual_info_words;
import java.io.IOException;
import java.util.HashSet; 
import java.util.Iterator; 
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * 
 * @author aswinakhilesh
 *The reducer aggregates the words with the documents in which they occur. A set data structure is used to find the
 *unique documents associated with each word. The reducer outputs the result in a the following format:
 *WORD<SPACE>COUNT_OF_DOCUMENTS#DOCUMENT1#DOCUMENT2#DOCUMENT3#DOCUMENTN
 *We make '#' as our delimiter for getting Na, Nb and Nab easily. The count of Documents is the Na/Nb. 
 */

public class wordcountReduce extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Set docs = new HashSet();
		Text output = new Text();
		StringBuffer result = new StringBuffer();
		for (Text val : values) {
			docs.add(val.toString());
		}       
		result.append(docs.size()+"#");
		Iterator setIter = docs.iterator();
		while(setIter.hasNext())
		{           
			Object setValue = setIter.next();
			result.append(setValue.toString() + "#");
		}
		output.set(result.toString().substring(0, result.length() - 1));
		context.write(key, output);
	}
}
