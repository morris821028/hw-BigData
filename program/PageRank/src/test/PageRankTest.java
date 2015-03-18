package test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.PageRank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PageRankTest {

	PageRank.PageRankMapper mapper;
	PageRank.PageRankReducer reducer;
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
		mapper = new PageRank.PageRankMapper();
		mapDriver = new MapDriver<Object, Text, Text, Text>()
				.withMapper(mapper);
		mapDriver.withInput(new LongWritable(1L), new Text("A 1 B C D"))
				.withOutput(new Text("A"), new Text("B"))
				.withOutput(new Text("B"), new Text("0.3333333333333333"))
				.withOutput(new Text("A"), new Text("C"))
				.withOutput(new Text("C"), new Text("0.3333333333333333"))
				.withOutput(new Text("A"), new Text("D"))
				.withOutput(new Text("D"), new Text("0.3333333333333333"))
				.runTest();
	}

	@Test
	public void testReducer() throws IOException, InterruptedException {
		reducer = new PageRank.PageRankReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>()
				.withReducer(reducer);
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("A"));
		values.add(new Text("1"));
		values.add(new Text("C"));
		values.add(new Text("4")); // 5 * 0.85 + 0.15 = 4.4
		reduceDriver.withInput(new Text("B"), values)
				.withOutput(new Text("B"), new Text("4.4 A C")).runTest();
	}

	@Test
	public void testMapReduce() throws IOException, InterruptedException {
		mapper = new PageRank.PageRankMapper();
		reducer = new PageRank.PageRankReducer();
		driver = new MapReduceDriver<Object, Text, Text, Text, Text, Text>(
				mapper, reducer);

		String line1 = new String("A 1 B C D");
		String line2 = new String("B 1 C D");
		String line3 = new String("C 1 D");
		String line4 = new String("D 1 B");

		driver.withInput(new LongWritable(1L), new Text(line1))
				.withInput(new LongWritable(2L), new Text(line2))
				.withInput(new LongWritable(3L), new Text(line3))
				.withInput(new LongWritable(4L), new Text(line4))
				.withOutput(new Text("A"),
						new Text("0.15000000000000002 B C D"))
				.withOutput(new Text("B"), new Text("1.2833333333333332 C D"))
				.withOutput(new Text("C"), new Text("0.8583333333333333 D"))
				.withOutput(new Text("D"), new Text("1.708333333333333 B"))
				.runTest();
	}
}
