package assignment1;

/**
 *
 * This class solves the problem posed for Assignment1
 *
 */

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Assignment1 {
	// map class header
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		// initialize KeyOut word and ValueOut fname
		private Text word = new Text();
		private Text fname = new Text();

		// map method header
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			ArrayList<String> tokenlist = new ArrayList<String>();

			// split document into words using string tokenizer
			StringTokenizer itr = new StringTokenizer(value.toString());

			// iterate over list of tokens and insert them into a list
			while (itr.hasMoreTokens()) {
				tokenlist.add(itr.nextToken());
			}

			// get value of N from context object's configuration data
			int n = Integer.parseInt(context.getConfiguration().get("N"));

			// iterate over list of tokens to find ngrams
			for (int i = 0; i < tokenlist.size() - n; i++) {
				// initialize empty string
				StringBuffer str = new StringBuffer();

				// append current ngram to string
				for (int j = i; j < i + n; j++) {
					if (j > i) {
						str = str.append(" ");
					}

					str = str.append(tokenlist.get(j));
				}

				// set ngram as KeyOut and file containing it as ValueOut
				word.set(str.toString());
				fname.set(((FileSplit) context.getInputSplit()).getPath().getName().toString());

				// write key-value pair to context object
				context.write(word, fname);
			}
		}
	}

	// reduce class header
	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		// initialize ValueOut result
		private Text result = new Text();

		// reduce class method
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// get value of minimum ngram count from context object's configuration data
			int mincount = Integer.parseInt(context.getConfiguration().get("mincount"));

			HashSet<String> ngramlist = new HashSet<String>();
			StringBuffer str = new StringBuffer();
			int ctr = 0;	// set counter to 0 initially

			// iterate over ValueIn (file list) for each KeyIn (ngram)
			for (Text val : values) {
				ngramlist.add(val.toString());	// add each file name to hash set
				ctr++;							// increment counter
			}

			// append counter value to string
			str = str.append(Integer.toString(ctr));

			// iterate over hash set containing file names and append each to string
			for (String nl : ngramlist) {
				str = str.append(" ");
				str = str.append(nl);
			}

			// check if counter exceeds minimum ngram count
			if (ctr >= mincount) {
				// set ValueOut as combination of ngram count and list of files containing the ngram
				result.set(str.toString());

				// write key-value pair to context object
				context.write(key, result);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// create configuration object
		Configuration conf = new Configuration();
		conf.set("N", args[0]);			// set args[0] as N in configuration data
		conf.set("mincount", args[1]);	// set args[1] as mincount in configuration data

		// create job object
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Assignment1.class);		// set job's jar file by providing class location
		job.setMapperClass(TokenizerMapper.class);	// provide mapper class name
		job.setReducerClass(IntSumReducer.class);	// provide reducer class name
		job.setOutputKeyClass(Text.class);			// set data type of output key
		job.setOutputValueClass(Text.class);		// set data type of output value

		FileInputFormat.addInputPath(job, new Path(args[2]));	// specify input directory
		FileOutputFormat.setOutputPath(job, new Path(args[3]));	// specify output directory

		System.exit(job.waitForCompletion(true) ? 0 : 1);		// submit job to cluster and wait for completion
	}
}
