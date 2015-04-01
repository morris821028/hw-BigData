package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class FailBlockRecordReader extends RecordReader<LongWritable, Text> {
	private final int NLINESTOPROCESS = 9999;
	private LineReader in;
	private LongWritable key;
	private Text value = new Text();
	public long start = 0;
	public long end = 0;
	public long pos = 0;
	public int maxLineLength;
	FSDataInputStream filein;
	@Override
	public void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		final Path file = split.getPath();
		Configuration conf = context.getConfiguration();
		this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		FileSystem fs = file.getFileSystem(conf);
		start = split.getStart();
		end = start + split.getLength();
		System.out.printf("RecordReader before start %d end %d\n", start, end);
		
		boolean skipFirstLine = false;
		filein = fs.open(split.getPath());

		if (start != 0) {
			skipFirstLine = true;
			--start;
			filein.seek(start);
		}
		in = new LineReader(filein, conf);
		System.out.printf("skipFirstline %b\n", skipFirstLine);
		if (skipFirstLine) {
			start += in.readLine(new Text(), 0,
					(int) Math.min((long) Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
		System.out.printf("RecordReader after pos %d start %d end %d\n", pos, start, end);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}
		
		if (pos > end)
			return false;
		
		System.out.printf("pos %d %d\n", pos, filein.getPos());
		value.clear();
		final Text endline = new Text("\n");
		int newSize = 0;
		String begin;
		Text v = new Text();
		while (true) {
			newSize = in.readLine(v, maxLineLength, Math.max(
					(int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
			begin = v.toString();
			if (newSize == 0) {
				key = null;
				value = null;
				return false;
			}
			pos += newSize;
			if (begin.trim().length() > 0) {
				value.append(v.getBytes(), 0, v.getLength());
				value.append(endline.getBytes(), 0, endline.getLength());
				break;
			}
		}
		System.out.printf(">>>>>>>> pos %d end %d, begin header %s\n", pos, end, begin);
		while (true) { 
//		while (true) {
			newSize = in.readLine(v, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));
			value.append(v.getBytes(), 0, v.getLength());
			value.append(endline.getBytes(), 0, endline.getLength());
			if (newSize == 0)
				break;
			pos += newSize;
			if (begin.equals(v.toString()))
				break;
		}
		return true;
	}

}
