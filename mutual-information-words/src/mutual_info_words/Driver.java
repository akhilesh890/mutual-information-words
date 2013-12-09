package mutual_info_words;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.*;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

public class Driver {

	public static int findCommonDocs (String a, String b) {
		HashSet docMap = new HashSet();
		int commonDocs = 0;
		int start = 0;

		String []doc_name = a.split("#");
		for (int i = 0 ; i < doc_name.length; i++){
			if(start > 0) {
				docMap.add(doc_name[i]);
			}
			start++;
		}
		start=0;

		doc_name = b.split("#");	      
		for (int i = 0 ; i < doc_name.length; i++){
			if (start > 0 && docMap.contains(doc_name[i])) {
				commonDocs++;
			}
			start++;	      
		}
		return commonDocs;
	}

	public static double findMutualInformation (double Na, double Nb, double Nab, double N) {

		double PAB01 = (Nb - Nab+0.25)/(N+1);
		double PAB10 = (Na - Nab+0.25)/(N+1);
		double PAB11 = (Nab+0.25)/(N+1);
		double PAB00 = 1 - (PAB01+PAB10+PAB11);
		double PA0 = (N-Na+0.5)/(N+1);
		double PA1 = (Na+0.5)/(N+1);
		double PB0 = (N-Nb+0.5)/(N+1);
		double PB1 = (Nb+0.5)/(N+1);

		double answer = (PAB01*Math.log(PAB01/(PA0*PB1)) + PAB10*Math.log(PAB10/(PA1*PB0)) + PAB11*Math.log(PAB11/(PA1*PB1))) + PAB00*Math.log(PAB00/(PA0*PB0) );

		return answer;
	}

	public static void main(String[] args) throws Exception {

		/**
		 * 4 stages of the program:
		 * 
		 * 1] Calculate out N, or the number of lines in the document by Map Reduce. Store it in "line_output".
		 * 2] Find the list of documents where each word appears, by Map Reduce. Store it in "temp_output".
		 * 3] Select the lines of "temp_output" which qualify. i.e, should be present in atleast 3 documents 
		 *    and be present in less than 30% of the total documents.
		 * 4] Select each pair of the selected words and compute Na, Nb, Nab and finally Mutual Information or I.   
		 */


		// 1] Calculate out N, or the number of lines in the document by Map Reduce. Store it in "line_output".
		Configuration conf1 = new Configuration();
		Job job = new Job(conf1, "Document Count");
		job.setJarByClass(Driver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class); 
		job.setMapperClass(doclenMap.class);
		job.setCombinerClass(doclenReduce.class);
		job.setReducerClass(doclenReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("/home/akhiles2/CS598/line_output"));        
		job.waitForCompletion(true);
		System.out.println("First Phase Completed - Number of Lines in Document Received.");



		Path path = new Path("HDFS://cn1.cloud.cs.illinois.edu:9000/home/akhiles2/CS598/line_output/part-r-00000");
		FileSystem fileSystem = FileSystem.get(new Configuration());
		BufferedReader nr = new BufferedReader(new InputStreamReader(fileSystem.open(path)));

		String lineString = nr.readLine();   	 
		String sample_arr[] = lineString.split("\\t");
		int N = Integer.parseInt(sample_arr[1]);
		Configuration conf2 = new Configuration();
		nr.close();
		System.out.println("N : "+ N);


		/**
		 * 2] Find the list of documents where each word appears, by Map Reduce. Store it in "temp_output". 
		 * The structure of the file is:
		 * WORD<SPACE>NO.OF.DOCS#DOCNAME1#DOCNAME2#DOCNAME3....
		 */

		System.out.println("Second Phase Starting - Word and its document matching....");		
		Job job1 = new Job(conf2, "DocumentFrequency");
		job1.setJarByClass(Driver.class);    
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(wordcountMap.class);
		job1.setReducerClass(wordcountReduce.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("/home/akhiles2/CS598/temp_output/"));
		job1.waitForCompletion(true);
		System.out.println("Second Phase completed - Word and its document matching one.");
		System.out.println("Wait for few more minutes.. DO NOT EXIT. ");

		/**		
		 * 3] Select the lines of "temp_output" which qualify. i.e, should be present in atleast 
		 *    3 documents and be present in less than 30% of the total documents. 
		 *    The selected lines(which include words and list of docs where it appears) are input to a list, for 
		 *    easy pairs generation done later. The values Na/Nb(Count of documents) are stored in a HashMap, 
		 *    for easy access later while computing I. No need to parse the line for again finding the 
		 *    count of documents
		 *    	
		 */
		path = new Path("HDFS://cn1.cloud.cs.illinois.edu:9000/home/akhiles2/CS598/temp_output/part-r-00000");
		fileSystem = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(path)));

		String line = null;
		String word = null;
		String rest = null;
		int count_doc=0;
		List<String> selectedLines = new ArrayList<String>(); // List of selected lines.
		HashMap<String, Integer> docCount = new HashMap<String, Integer>(); // Store the Na/Nb for each word

		while ((line = br.readLine()) != null)  
		{  
			String arr[] = line.split("\\t", 2);
			word = arr[0];
			rest = arr[1];			
			arr = rest.split("#", 2);
			count_doc = Integer.parseInt(arr[0]);
			if (count_doc > 3 && count_doc < (N/3.3)){
				selectedLines.add(line);
				docCount.put(line, count_doc);
			}    
		}	
		br.close();
		/**
		 * 4] Select each pair of the selected words and compute Na, Nb, Nab and finally Mutual Information or I.
		 */
		Configuration configuration = new Configuration();
		Path file = new Path("HDFS://cn1.cloud.cs.illinois.edu:9000/home/akhiles2/CS598/Final_Output");
		FileSystem hdfs = FileSystem.get(configuration);
		
		if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
		OutputStream os = hdfs.create( file,
		    new Progressable() {
		        public void progress() {		            
		        } });
		
		BufferedWriter writer = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );

		for (int i = 0 ; i < selectedLines.size() ; i++ ) {
			for( int j = i+1 ; j < selectedLines.size() ; j++ ){
				String a = selectedLines.get(i);
				String b = selectedLines.get(j);
				int Na = docCount.get(a);
				int Nb = docCount.get(b);
				int Nab = findCommonDocs(a,b);
				String []lineA = a.split("\\t",2);
				String []lineB = b.split("\\t",2);
				String wordA = lineA[0];
				String wordB = lineB[0];
				double mutual_info = findMutualInformation(Na,Nb,Nab,N);

				//		writer.println("WORD PAIR: "+wordA + '\t' + wordB+'\t'+"DOC-FREQ("+wordA+"): " +Na+'\t' + "DOC-FREQ("+wordB+"): "+Nb+ '\t' + "MUTUAL-INFORMATION: "+ mutual_info + '\t' + "Nab :" + "\t" + Nab );
				writer.write(mutual_info + "\t" + wordA+","+wordB);
				writer.write("\n");
			}
		}
		System.out.println("All Tasks Complete. Files Closing.");
		writer.close();
		hdfs.close();	

	}

}

