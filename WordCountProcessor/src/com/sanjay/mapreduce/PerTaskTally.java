package com.sanjay.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PerTaskTally {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private Integer one = 1;
		private Text word = new Text();
		private Map<String, Integer> wordcount;
		
		protected void setup(Context context) throws IOException, InterruptedException {
	        Configuration conf = context.getConfiguration();

	        wordcount = new HashMap<String, Integer>();
	    }

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String lower = "";
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				lower = word.toString().toLowerCase();

				if (lower.startsWith("m") || lower.startsWith("n") || lower.startsWith("o") || lower.startsWith("p")
						|| lower.startsWith("q")) {
					if (wordcount.containsKey(word.toString())) {
						wordcount.put(word.toString(), wordcount.get(word.toString()) + 1);
					} else {
						wordcount.put(word.toString(), one);
					}
				}
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
	        Configuration conf = context.getConfiguration();

	        for (Entry<String, Integer> entry : wordcount.entrySet()) {
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
	    }
	}

	public static class WordPartitioner extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			String word = key.toString().toLowerCase();

			if (word.startsWith("m")) {
				return 0;
			} else if (word.startsWith("n")) {
				return 1;
			} else if (word.startsWith("o")) {
				return 2;
			} else if (word.startsWith("p")) {
				return 3;
			} else if (word.startsWith("q")) {
				return 4;
			} else {
				return 0;
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(PerTaskTally.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setPartitionerClass(WordPartitioner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(5);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
