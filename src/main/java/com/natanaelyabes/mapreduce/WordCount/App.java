package com.natanaelyabes.mapreduce.WordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Class
 * 
 * @version 1.0.0
 * @author Natanael Yabes Wirawan
 *
 */
public class App {

	/**
	 * Map Class
	 * 
	 * The map class will be used as a Map job within the Hadoop MapReduce
	 * implementation. This class will annotate each token of characters present
	 * within the corpus so it can be passed to Reduce job later.
	 *
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// Retrieve all text within the corpus and tokenize each word
			String line = value.toString();
			StringTokenizer st = new StringTokenizer(line);

			// Actual map implementation
			while (st.hasMoreTokens()) {
				word.set(st.nextToken());
				context.write(word, one);
			}
		}
	}

	
	/**
	 * Reduce Class
	 * 
	 * The reduce class will be used as a Reduce job within the Hadoop MapReduce
	 * implementation. This class will sum each value of token of key present
	 * within the context so it's value can be aggregated in a DFS environment.
	 *
	 */
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
		 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// Init the sum variable
			int sum = 0;

			// Loop for each IntWritable value within the context.
			// It will aggregate the value if key is present > 1 within the Context
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		
		// Load common configuration and parse other argument from CLI application instance
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// Input @param pitfall workaround
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		
		// Assign each class for each MapReduce job
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(App.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
