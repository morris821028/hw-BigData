package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.BlockInputFormat;
import main.BlockRecordReader;
import main.PageRankCustom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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

public class PageRankCustomTest {

	PageRankCustom.PageRankMapper mapper;
	PageRankCustom.PageRankReducer reducer;
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
	public void testCustomMapper() throws IOException, InterruptedException {
		mapper = new PageRankCustom.PageRankMapper();
		mapDriver = new MapDriver<Object, Text, Text, Text>()
				.withMapper(mapper);
		String html = "http://A\n<html>\na\nb\n<a href=\"http://B\">\n\n<a href='http://C'>\n<a href=\"http://D\">\n<html>\nhttp://A";
		mapDriver.withInput(new LongWritable(1L), new Text(html))
				.withOutput(new Text("A"), new Text("1"))
				.withOutput(new Text("A"), new Text("B"))
				.withOutput(new Text("A"), new Text("C"))
				.withOutput(new Text("A"), new Text("D")).runTest();
	}

	@Test
	public void testCustomReducer() throws IOException, InterruptedException {
		reducer = new PageRankCustom.PageRankReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>()
				.withReducer(reducer);
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("B"));
		values.add(new Text("1"));
		values.add(new Text("C"));
		values.add(new Text("D"));
		reduceDriver.withInput(new Text("A"), values)
				.withOutput(new Text("A"), new Text("1.0 B C D")).runTest();
	}

	@Test
	public void testCustomMapReduce() throws IOException, InterruptedException {
		mapper = new PageRankCustom.PageRankMapper();
		reducer = new PageRankCustom.PageRankReducer();
		driver = new MapReduceDriver<Object, Text, Text, Text, Text, Text>(
				mapper, reducer);

		String html1 = "http://A\n<html>\na\nb\n<a href=\"http://B\">\n\n<a href='http://C'>\n<a href=\"http://D\">\n<html>\nhttp://A";
		String html2 = "http://B\n<html>p\nq\nr\n<a href=\"http://C\">\n<a href=\"http://D\">\n<html>\nhttp://B";
		String html3 = "http://C\n<html>\nx\n<a href=\"http://D\">\n<html>\nhttp://C\n";
		String html4 = "http://D\n<html>\ny\n<a href=\"http://B\">\n<html>\nhttp://D\n";

		driver.withInput(new LongWritable(1L), new Text(html1))
				.withInput(new LongWritable(2L), new Text(html2))
				.withInput(new LongWritable(3L), new Text(html3))
				.withInput(new LongWritable(4L), new Text(html4))
				.withOutput(new Text("A"), new Text("1.0 B C D"))
				.withOutput(new Text("B"), new Text("1.0 C D"))
				.withOutput(new Text("C"), new Text("1.0 D"))
				.withOutput(new Text("D"), new Text("1.0 B")).runTest();
	}

	@Test
	public void testCustomMapReduceDuplicate() throws IOException,
			InterruptedException {
		mapper = new PageRankCustom.PageRankMapper();
		reducer = new PageRankCustom.PageRankReducer();
		driver = new MapReduceDriver<Object, Text, Text, Text, Text, Text>(
				mapper, reducer);

		String html1 = "http://A/A1/index.html\n<html>\na\nb\n<a href=\"http://B\">\n\n<a href='http://C'>\n<a href=\"http://D\">\n<html>\nhttp://A";
		String html2 = "http://A/A1/B1/index.html/\n<html>p\nq\nr\n<a href=\"http://C\">\n<a href=\"http://D\">\n<html>\nhttp://B";

		driver.withInput(new LongWritable(1L), new Text(html1))
				.withInput(new LongWritable(2L), new Text(html2))
				.withOutput(new Text("A"), new Text("1.0 B C D")).runTest();
	}

	@Test
	public void testBlockRecordReader() throws IOException,
			InterruptedException {
		mapper = new PageRankCustom.PageRankMapper();
		reducer = new PageRankCustom.PageRankReducer();
		driver = new MapReduceDriver<Object, Text, Text, Text, Text, Text>(
				mapper, reducer);

		BlockRecordReader reader = getBlockRecordReader();

		int counter = 0;
		while (reader.nextKeyValue()) {
			// System.out.printf("key %s\nvalue %s\n", reader.getCurrentKey(),
			// reader.getCurrentValue());
			counter++;
		}
		assertEquals(4, counter);
	}

	private static BlockRecordReader getBlockRecordReader() throws IOException,
			InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");

		String testFilePath = "testinput/testLinksHtml(small).txt";

		File testFile = new File(testFilePath);
		Path path = new Path(testFile.toURI().toString());

		FileSplit split = new FileSplit(path, 0, testFile.length() / 100, null);

		BlockInputFormat inputFormat = ReflectionUtils.newInstance(
				BlockInputFormat.class, conf);
		TaskAttemptContext context = new TaskAttemptContext(conf,
				new TaskAttemptID());
		BlockRecordReader reader = (BlockRecordReader) inputFormat
				.createRecordReader(split, context);

		reader.initialize(split, context);
		return reader;
	}

}
