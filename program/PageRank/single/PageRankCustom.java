package main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRankCustom {
	static enum PageCount {
		sumPageRank
	}

	/**
	 * Mapper<Input Key, Input Value, Output Key, Output Value>
	 * 
	 * @author morris
	 * 
	 */
	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {

		public static String getDomainName(String url)
				throws MalformedURLException {
            try {
                URL uri = new URL(url);
                String domain = uri.getHost();
                return domain.startsWith("www.") ? domain.substring(4) : domain;
            } catch(Exception e) {
                return "";
            }
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());

			if (!tokenizer.hasMoreTokens())
				return;

			String site = tokenizer.nextToken();

			context.write(new Text(getDomainName(site)), new Text("1"));

			while (tokenizer.hasMoreTokens()) {
				String html = tokenizer.nextToken();
				for (int pos = 0; pos < html.length(); pos++) {
					if (html.indexOf("href=", pos) >= 0) {
						int p = html.indexOf("href=") + "href=".length();
						String url = "";
						for (int q = p + 1; q < html.length() && html.charAt(q) != html.charAt(p); pos = q++)
							url = url.concat(String.valueOf(html.charAt(q)));
                        if (getDomainName(url).trim().length() > 0)
                            context.write(new Text(getDomainName(site)), new Text(
								getDomainName(url)));
					} else {
						break;
					}
				}
			}
		}

	}

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		public static Double beta = 0.85;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Set<String> S = new HashSet<String>();
			StringBuilder sb = new StringBuilder();
			double pr = 0;
			for (Text val : values) {
				try {
					pr = Double.valueOf(val.toString());
				} catch (Exception e) {
					if (!S.contains(val.toString())) {
						sb = sb.append(" " + val.toString());
						S.add(val.toString());
					}
				}
			}

			String t = String.valueOf(pr) + sb.toString();

			context.write(key, new Text(t));
		}

	}

	public static void computePageRank(String inputPath, String outputPath,
			int itId) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		fs.delete(new Path(outputPath), true);

		Job job = new Job(conf, "Page Rank Custom" + itId);

		job.setJarByClass(PageRank.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setInputFormatClass(BlockInputFormat.class);
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
		final int ITLIMIT = 1;
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