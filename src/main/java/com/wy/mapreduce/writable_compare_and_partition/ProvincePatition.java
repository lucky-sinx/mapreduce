package com.wy.mapreduce.writable_compare_and_partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePatition extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        int partition = -1;
        String phoneID = text.toString().substring(0, 3);
        switch (phoneID) {
            case "136":
                partition = 1;
                break;
            case "138":
                partition = 2;
                break;
            case "184":
                partition = 3;
                break;
            case "134":
                partition = 4;
                break;
            default:
                partition = 0;
                break;
        }
        return partition;
    }

//    @Override
//    public int getPartition(Text text, FlowBean flowBean, int i) {
//
//    }
}
