package ca.uwo.seeu.hadoop.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

public class OneLineInputFormat extends FileInputFormat<LongWritable, Text> 
{ 

    @Override 
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, 
            TaskAttemptContext context) throws IOException, 
InterruptedException { 

        // from here - https://issues.apache.org/jira/secure/attachment/12413533/patch-375.txt
        context.setStatus(split.toString()); 
        return new LineRecordReader(); 
    } 

    public List<InputSplit> getSplits(JobContext job) 
      throws IOException { 
        List<InputSplit> splits = new ArrayList<InputSplit>(); 
        for (FileStatus status : listStatus(job)) { 
          Path fileName = status.getPath(); 
          if (status.isDir()) { 
            throw new IOException("Not a file: " + fileName); 
          } 
          FileSystem  fs = fileName.getFileSystem(job.getConfiguration()); 
          LineReader lr = null; 

          try { 
            FSDataInputStream in  = fs.open(fileName); 
            lr = new LineReader(in, job.getConfiguration()); 
            Text line = new Text(); 
            long begin = 0; 
            long length = 0; 
            int num = -1; 
            while ((num = lr.readLine(line)) > 0) { 
                length += num; 

                if (begin == 0) { 
                    splits.add(new FileSplit(fileName, begin, length - 1, 
                    new String[] {})); 
                } else { 
                    splits.add(new FileSplit(fileName, begin - 1, length, 
                    new String[] {})); 
                } 
                begin += length; 
                length = 0; 
            } 
          } finally { 
              if (lr != null) { 
              lr.close(); 
             } 
          } 
        } 
        return splits; 
      } 
} 