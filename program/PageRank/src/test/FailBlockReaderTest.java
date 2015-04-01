package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import main.BlockInputFormat;
import main.BlockRecordReader;
import main.FailBlockInputFormat;
import main.FailBlockRecordReader;
import main.PageRankCustom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
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
		String fileName = "testHtmlWith2.txt";
		String testFilePath = "testinput/" + fileName;
		long byteForSplit = 170;
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");

		File testFile = new File(testFilePath);
		Path path = new Path(testFile.toURI().toString());
		long offset = 0;

		FailBlockInputFormat inputFormat = ReflectionUtils.newInstance(
				FailBlockInputFormat.class, conf);
		TaskAttemptContext context = new TaskAttemptContext(conf,
				new TaskAttemptID());

		System.out.printf("file bytes = %d, split %d bytes each\n",
				testFile.length(), byteForSplit);

		int counter = 0;

		mapper = new PageRankCustom.PageRankMapper();
		reducer = new PageRankCustom.PageRankReducer();
		driver = new MapReduceDriver<Object, Text, Text, Text, Text, Text>(
				mapper, reducer);

		for (int i = 0; i < 9999; i++) {
			FileSplit split = new FileSplit(path, offset, byteForSplit, null);
			FailBlockRecordReader reader = (FailBlockRecordReader) inputFormat
					.createRecordReader(split, context);
			reader.initialize(split, context);
			if (reader.nextKeyValue() == false)
				break;
			do {
				System.out.printf("<key, value> %d, %s\n", reader
						.getCurrentKey().get(), reader.getCurrentValue()
						.toString());
				driver.withInput(
						new LongWritable(reader.getCurrentKey().get()),
						new Text(reader.getCurrentValue().toString()));
				counter++;
			} while (reader.nextKeyValue());
			offset = reader.pos;
		}
		driver.withOutput(new Text("tw.yahoo.com"),
				new Text("1.0 w3schools.com google.com"))
				.withOutput(new Text("youtube.com"),
						new Text("1.0 w3schools.com google.com tw.yahoo.com"))
				.runTest();
	}
}
