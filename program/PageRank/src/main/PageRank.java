package main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank {
	static enum PageCount {
		sumPageRank
	}

	/**
	 * Mapper<Input Key, Input Value, Output Key, Output Value> 
	 * 
	 * @author morris
	 * 
	 */
	public static class PageRankMapper extends Mapper<Object, Text, Text, Text>
			implements
			org.apache.hadoop.mapred.Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			String site = tokenizer.nextToken();
			double rankValue = Double.parseDouble(tokenizer.nextToken());
			double linkCount = tokenizer.countTokens();
			Text from = new Text(site);
			Text contribute = new Text(String.valueOf(rankValue / linkCount));

			while (tokenizer.hasMoreTokens()) {
				site = tokenizer.nextToken();
				context.write(from, new Text(site));
				context.write(new Text(site), contribute);
			}
		}

		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void map(Object arg0, Text arg1,
				OutputCollector<Text, Text> arg2, Reporter arg3)
				throws IOException {
			StringTokenizer tokenizer = new StringTokenizer(arg1.toString());
			String site = tokenizer.nextToken();
			double rankValue = Double.parseDouble(tokenizer.nextToken());
			double linkCount = tokenizer.countTokens();
			Text from = new Text(site);
			Text contribute = new Text(String.valueOf(rankValue / linkCount));

			while (tokenizer.hasMoreTokens()) {
				site = tokenizer.nextToken();
				arg2.collect(from, new Text(site));
				arg2.collect(new Text(site), contribute);
			}
		}

	}

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text>
			implements
			org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text> {
		public static Double beta = 0.85;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			double p = 0;
			for (Text val : values) {
				try {
					double contri = Double.valueOf(val.toString());
					p += contri;
				} catch (Exception e) {
					sb = sb.append(val.toString() + " ");
				}
			}

			p = p * beta + 1 - beta;
			String t = String.valueOf(p) + " " + sb.toString();

			context.getCounter(PageCount.sumPageRank).increment(
					(long) (p * 10000));
			context.write(key, new Text(t));
		}

		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void reduce(Text arg0, Iterator<Text> arg1,
				OutputCollector<Text, Text> arg2, Reporter arg3)
				throws IOException {
			StringBuilder sb = new StringBuilder();
			double p = 0;
			while (arg1.hasNext()) {
				Text val = arg1.next();
				try {
					double contri = Double.valueOf(val.toString());
					p += contri;
				} catch (Exception e) {
					sb.append(" " + val.toString());
				}
			}

			p = p * beta + 1 - beta;
			String t = String.valueOf(p) + sb.toString();
			arg2.collect(arg0, new Text(t));
		}
	}

	public static void computePageRank(String inputPath, String outputPath,
			int itId) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		fs.delete(new Path(outputPath), true);

		Job job = new Job(conf, "Page Rank " + itId);

		job.setJarByClass(PageRank.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: <Input_path> <Output_path>");
			return;
		}
		final int ITLIMIT = 20;
		String input = args[0];
		String output = args[1];
		for (int it = 0; it < ITLIMIT; it++) {
			if (it == 0) {
				output = "pagerank_output2/";
			} else if (it % 2 == 0) {
				input = "pagerank_output1/p*";
				output = "pagerank_output2/";
			} else {
				input = "pagerank_output2/p*";
				output = "pagerank_output1/";
			}
			computePageRank(input, output, it);
		}
	}
}