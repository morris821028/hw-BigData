package main;

import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;

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

public class BlockRecordReader extends RecordReader<LongWritable, Text> {
    private final int NLINESTOPROCESS = 9999;
    private LineReader in;
    private LongWritable key;
    private Text value = new Text();
    private long start = 0;
    private long end = 0;
    private long pos = 0;
    private int maxLineLength;
    
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
        
        FSDataInputStream filein = fs.open(split.getPath());
        
        filein.seek(start);
        boolean skipFirstLine = false;
        if (start != 0) {
            skipFirstLine = true;
            --start;
            filein.seek(start);
        }
        in = new LineReader(filein, conf);
        if (skipFirstLine) {
            start += in.readLine(new Text(), 0,
                                 (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
        // System.out.printf("start %d end %d\n", start, end);
        this.pos = start;
    }
    
    public static String getDomainName(String url) throws MalformedURLException {
        try {
            URL uri = new URL(url);
            String domain = uri.getHost();
            return domain.startsWith("www.") ? domain.substring(4) : domain;
        } catch (Exception e) {
            return "";
        }
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
        if (pos >= end)
            return false;
        
        value.clear();
        final Text endline = new Text("\n");
        int newSize = 0;
        String begin = "error";
        Text v = new Text();
        while (pos < end) {
            // find <url begin>
            while (true) {
                newSize = in.readLine(v, maxLineLength, Math.max(
                                                                 (int) Math.min(Integer.MAX_VALUE, end - pos),
                                                                 maxLineLength));
                begin = v.toString();
                if (newSize == 0) {
                    key = null;
                    value = null;
                    return false;
                }
                pos += newSize;
                if (begin.length() > 0 && getDomainName(begin).length() > 0)
                    break;
                if (pos >= end)
                    return false;
            }
            // find next token after <url begin>
            while (true) {
                newSize = in.readLine(v, maxLineLength, Math.max(
                                                                 (int) Math.min(Integer.MAX_VALUE, end - pos),
                                                                 maxLineLength));
                pos += newSize;
                if (newSize == 0)
                    return false;
                
                if (v.toString().trim().length() > 0) {
                    if (getDomainName(v.toString()).length() > 0) {
                        // in next split
                        if (pos - newSize >= end)
                            return false;
                        // replace <url begin>
                        begin = v.toString(); 
                    } else {
                        // find non null string and with <??? html ???>
                        if (v.toString().trim().charAt(0) == '<' && v.toString().indexOf("html") >= 0)
                            break;
                        if (pos >= end)
                            return false;
                    }
                }
            }
            if (v.toString().trim().charAt(0) == '<') {
                System.out.printf("%s\n%s\n", begin, v.toString());
                value.append(begin.getBytes(), 0, begin.length());
                value.append(endline.getBytes(), 0, endline.getLength());
                value.append(v.getBytes(), 0, v.getLength());
                value.append(endline.getBytes(), 0, endline.getLength());
                break;
            } else {
                value.clear();
            }
            if (pos >= end)
                return false;
        }
        
        // System.out.printf("%d begin %s\n", pos, begin);
        while (pos < end) {
            newSize = in.readLine(v, maxLineLength,
                                  Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
                                           maxLineLength));
            value.append(v.getBytes(), 0, v.getLength());
            value.append(endline.getBytes(), 0, endline.getLength());
            pos += newSize;
            if (newSize == 0)
                return false;
            if (begin.equals(v.toString()))
                break;
        }
        // System.out.printf("%d end %s\n", pos, begin);
        
        return true;
    }
    
}
