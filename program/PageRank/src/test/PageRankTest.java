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
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class PageRankTest {

	PageRank.PageRankMapper mapper;
	PageRank.PageRankReducer reducer;
	Context context;
	MapDriver<Object, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() throws Exception {
		mapper = new PageRank.PageRankMapper();
		mapDriver = new MapDriver<Object, Text, Text, Text>()
				.withMapper(mapper);

		reducer = new PageRank.PageRankReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>()
				.withReducer(reducer);
		context = mock(Context.class);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMapper() throws IOException, InterruptedException {
		mapDriver.withInput(new LongWritable(1L), new Text("A 1 B C D"));
		mapDriver.withOutput(new Text("A"), new Text("B"));
		mapDriver.withOutput(new Text("B"), new Text("0.3333333333333333"));
		mapDriver.withOutput(new Text("A"), new Text("C"));
		mapDriver.withOutput(new Text("C"), new Text("0.3333333333333333"));
		mapDriver.withOutput(new Text("A"), new Text("D"));
		mapDriver.withOutput(new Text("D"), new Text("0.3333333333333333"));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException, InterruptedException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("A"));
		values.add(new Text("0.3333333333333333"));
		reduceDriver.withInput(new Text("B"), values);
		reduceDriver.withOutput(new Text("B"), new Text("0.43333333333333324 A"));
		reduceDriver.runTest();
	}
}
