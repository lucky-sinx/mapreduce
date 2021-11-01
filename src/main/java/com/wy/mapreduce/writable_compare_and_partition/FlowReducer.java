package com.wy.mapreduce.writable_compare_and_partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
//    private FlowBean outV = new FlowBean();
//
//    @Override
//    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
//        long upSum = 0;
//        long downSum = 0;
//        for (FlowBean value : values) {
//            upSum += value.upFlow;
//            downSum += value.downFlow;
//        }
//        outV.setUpFlow(upSum);
//        outV.setDownFlow(downSum);
//        outV.setSumFlow();
//        context.write(key, outV);
//    }

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
