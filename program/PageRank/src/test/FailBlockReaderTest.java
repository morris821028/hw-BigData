package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import main.BlockInputFormat;
import main.BlockRecordReader;
import main.FailBlockInputFormat;
import main.FailBlockRecordReader;
import main.PageRankCustom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FailBlockReaderTest {

	PageRankCustom.PageRankMapper mapper;
	PageRankCustom.PageRankReducer reducer;
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
	public void testLargeBlockRecordReaderDuplicate() throws IOException,
			InterruptedException {
		mapper = new PageRankCustom.PageRankMapper();
		reducer = new PageRankCustom.PageRankReducer();
		driver = new MapReduceDriver<Object, Text, Text, Text, Text, Text>(
				mapper, reducer);

		FailBlockRecordReader reader = getLargeBlockRecordReader("testLinksHtml(small).txt");

		int counter = 0;
		while (reader.nextKeyValue()) {
			driver.withInput(new LongWritable(reader.getCurrentKey().get()), new Text(reader.getCurrentValue().toString()));
			counter++;
		}
		driver.runTest();
		assertEquals(101, counter);
	}
	
	private static FailBlockRecordReader getLargeBlockRecordReader(String fileName)
			throws IOException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");

		String testFilePath = "testinput/" + fileName;

		File testFile = new File(testFilePath);
		Path path = new Path(testFile.toURI().toString());

		FileSplit split = new FileSplit(path, 0, 10, null);

		FailBlockInputFormat inputFormat = ReflectionUtils.newInstance(
				FailBlockInputFormat.class, conf);
		TaskAttemptContext context = new TaskAttemptContext(conf,
				new TaskAttemptID());
		FailBlockRecordReader reader = (FailBlockRecordReader) inputFormat
				.createRecordReader(split, context);

		reader.initialize(split, context);
		return reader;
	}
}
