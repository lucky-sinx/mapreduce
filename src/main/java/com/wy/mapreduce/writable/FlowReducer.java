package com.wy.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        long upSum = 0;
        long downSum = 0;
        for (FlowBean value : values) {
            upSum += value.upFlow;
            downSum += value.downFlow;
        }
        outV.setUpFlow(upSum);
        outV.setDownFlow(downSum);
        outV.setSumFlow();
        context.write(key, outV);
    }
}
