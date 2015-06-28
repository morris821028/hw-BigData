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

public class Minhash {

	/**
	 * Mapper<Input Key, Input Value, Output Key, Output Value>
	 * 
	 * @author morris
	 * 
	 */
	public static final int SIGNUATURE_SIZE = 100;

	public static class MinhashMapper extends Mapper<Object, Text, Text, Text>
			implements
			org.apache.hadoop.mapred.Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] prop = value.toString().split("##");
			Text vkey = new Text(prop[0]);
			Random rand = new Random();
			rand.setSeed(514);
			for (int i = 0; i < SIGNUATURE_SIZE; i++) {
				String h = permutate_hash(rand.nextLong(), prop[1]);
				Text vvalue = new Text(Integer.toString(i) + " " + h);
				context.write(vkey,  vvalue);
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
			String[] prop = arg1.toString().split("##");
			Text key = new Text(prop[0]);
			Random rand = new Random();
			rand.setSeed(514);
			for (int i = 0; i < SIGNUATURE_SIZE; i++) {
				String h = permutate_hash(rand.nextLong(), prop[1]);
				Text value = new Text(Integer.toString(i) + " " + h);
				arg2.collect(key,  value);
			}
//			System.out.println(key.toString());
		}

		static public long custom_hash(long x) {
			return x * 2654435761L;
		}

		static public String permutate_hash(long seed, String text) {
			Random rand = new Random();
			rand.setSeed(seed);
			long value = Long.MAX_VALUE;
			int n = text.length();
			char[] t = text.toCharArray();
			for (int i = 0; i < n; i++) {
				int j = (rand.nextInt()%n + n) % n;
				char c;
				c = t[j];
				t[j] = t[i];
				t[i] = c;
			}
			rand.setSeed(seed);
			for (int i = 0; i < n; i++) {
				if (t[i] == '1')
					value = Math.min(value, custom_hash((i + 2654435761L) * (i + 198712511L) + 1));
			}
			return Integer.toHexString((int) value);
		}
	}

	public static class MinhashReducer extends Reducer<Text, Text, Text, Text>
			implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			String[] sign = new String[SIGNUATURE_SIZE];
			for (Text val : values) {
				String[] prop = val.toString().split(" ");
				try {
					sign[Integer.parseInt(prop[0])] = prop[1];
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			for	(int i = 0; i < SIGNUATURE_SIZE; i++) {
				sb.append(sign[i]);
				if (i != SIGNUATURE_SIZE - 1)
					sb.append(" ");
			}
			context.write(key, new Text(sb.toString()));
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
			String[] sign = new String[SIGNUATURE_SIZE];
			while (arg1.hasNext()) {
				Text val = arg1.next();
				String[] prop = val.toString().split(" ");
				try {
					sign[Integer.parseInt(prop[0])] = prop[1];
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			for	(int i = 0; i < SIGNUATURE_SIZE; i++) {
				sb.append(sign[i]);
				if (i != SIGNUATURE_SIZE - 1)
					sb.append(", ");
			}
			arg2.collect(arg0, new Text(sb.toString()));
		}
	}

	public static void computeMinhash(String inputPath, String outputPath,
			int itId) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		fs.delete(new Path(outputPath), true);

		Job job = new Job(conf, "Minhash " + itId);

		job.setJarByClass(Minhash.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MinhashMapper.class);
		job.setReducerClass(MinhashReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: <Input_path> <Output_path>");
			String input = "testinput/example";
			String output = "testoutput/";
			computeMinhash(input, output, 0);
			return;
		}
		String input = args[0];
		String output = args[1];
		computeMinhash(input, output, 0);
	}
}
