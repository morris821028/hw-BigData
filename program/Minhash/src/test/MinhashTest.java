package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;

import main.Minhash;

public class MinhashTest {

	Minhash.MinhashMapper mapper;
	Minhash.MinhashReducer reducer;
	Context context;
	MapDriver<Object, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver driver;

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMapper() throws IOException, InterruptedException {
		mapper = new Minhash.MinhashMapper();
		mapDriver = new MapDriver<Object, Text, Text, Text>()
				.withMapper(mapper);
		mapDriver.withInput(new LongWritable(1L), new Text("001##1234"))
				.withOutput(new Text("001"), new Text("0 000000009E3779B1"))
				.withOutput(new Text("001"), new Text("1 000000013C6EF362"))
				.withOutput(new Text("001"), new Text("2 000000009E3779B1"))
				.withOutput(new Text("001"), new Text("3 000000013C6EF362"))
				.withOutput(new Text("001"), new Text("4 000000062E2AC0EA"))
				.withOutput(new Text("001"), new Text("5 000000013C6EF362"))
				.withOutput(new Text("001"), new Text("6 7FFFFFFFFFFFFFFF"))
				.withOutput(new Text("001"), new Text("7 000000009E3779B1"))
				.withOutput(new Text("001"), new Text("8 7FFFFFFFFFFFFFFF"))
				.withOutput(new Text("001"), new Text("9 000000062E2AC0EA"))
				.runTest();
	}

	@Test
	public void testReducer() throws IOException, InterruptedException {
		reducer = new Minhash.MinhashReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>()
				.withReducer(reducer);
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("0 000000009E3779B1"));
		values.add(new Text("1 000000013C6EF362"));
		values.add(new Text("2 000000009E3779B1"));
		values.add(new Text("3 000000013C6EF362"));
		values.add(new Text("4 000000062E2AC0EA"));
		values.add(new Text("5 000000013C6EF362"));
		values.add(new Text("6 7FFFFFFFFFFFFFFF"));
		values.add(new Text("7 000000009E3779B1"));
		values.add(new Text("8 7FFFFFFFFFFFFFFF"));
		values.add(new Text("9 000000062E2AC0EA"));
		reduceDriver.withInput(new Text("001"), values)
				.withOutput(new Text("001"), new Text("000000009E3779B1, 000000013C6EF362, 000000009E3779B1, 000000013C6EF362, 000000062E2AC0EA, 000000013C6EF362, 7FFFFFFFFFFFFFFF, 000000009E3779B1, 7FFFFFFFFFFFFFFF, 000000062E2AC0EA")).runTest();
	}

	@Test
	public void testMapReduce() throws IOException, InterruptedException {
		mapper = new Minhash.MinhashMapper();
		reducer = new Minhash.MinhashReducer();
		driver = new MapReduceDriver<Object, Text, Text, Text, Text, Text>(
				mapper, reducer);
		RecordReader reader = getRecordReader();
		int counter = 0;
		while (reader.nextKeyValue()) {
			System.out.println(reader.getCurrentKey());
			System.out.println(reader.getCurrentValue());
//			driver.addInput(reader.getCurrentKey(), reader.getCurrentValue());
			counter++;
		}
//		System.out.printf("counter %d\n", counter);
//		driver.runTest();
	}

	private static RecordReader getRecordReader() throws IOException,
			InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");

		String testFilePath = "testinput/input1.txt";

		File testFile = new File(testFilePath);
		Path path = new Path(testFile.toURI().toString());

		FileSplit split = new FileSplit(path, 0, testFile.length(), null);

		TextInputFormat inputFormat = ReflectionUtils.newInstance(
				TextInputFormat.class, conf);
		TaskAttemptContext context = new TaskAttemptContext(conf,
				new TaskAttemptID());
		LineRecordReader reader = (LineRecordReader) inputFormat
				.createRecordReader(split, context);

		reader.initialize(split, context);
		return reader;
	}
}
