package com.wy.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private String fileName;
    private Text outK = new Text();
    private TableBean outV = new TableBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (fileName.contains("order")) {//订单表
            String[] split = line.split(" ");

            outK.set(split[1]);
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setPname("");
            outV.setFlag("order");
            outV.setAmount(Integer.parseInt(split[2]));
        } else {//pd表
            String[] split = line.split(" ");

            outK.set(split[0]);
            outV.setId("");
            outV.setPid(split[0]);
            outV.setPname(split[1]);
            outV.setFlag("pd");
            outV.setAmount(0);
        }
        context.write(outK, outV);
    }

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        this.fileName = inputSplit.getPath().getName();
    }
}
