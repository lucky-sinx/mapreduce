package com.wy.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream wyOut;
    private FSDataOutputStream otherOut;

    public LogRecordWriter(TaskAttemptContext taskAttemptContext) {
        try {
            FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
            wyOut = fileSystem.create(new Path("D:\\code\\data\\hadoopinput\\outputformat_output\\wy.log"));
            otherOut = fileSystem.create(new Path("D:\\code\\data\\hadoopinput\\outputformat_output\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String log = text.toString();
        if (log.contains("wy")) {
            wyOut.writeBytes(log + "\n");
        } else {
            otherOut.writeBytes(log + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(wyOut);
        IOUtils.closeStream(otherOut);
    }
}
