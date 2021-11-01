package com.wy.mapreduce.writablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
//    private Text outK = new Text();
//    private FlowBean outV = new FlowBean();
//
//    @Override
//    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
//        //get one line
//        String line = value.toString();
//        //split
//        String[] words = line.split(" ");
//        //get data
//        String phone = words[1];
//        String up = words[words.length - 3];
//        String down = words[words.length - 2];
//        //package
//        outK.set(phone);
//        outV.setUpFlow(Long.parseLong(up));
//        outV.setDownFlow(Long.parseLong(down));
//        outV.setSumFlow();
//        //write
//        context.write(outK, outV);
//    }

    private FlowBean outK = new FlowBean();
    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" |\t");
        outV.set(words[0]);
        outK.setUpFlow(Long.parseLong(words[1]));
        outK.setDownFlow(Long.parseLong(words[2]));
        outK.setSumFlow();
        context.write(outK, outV);
    }
}
