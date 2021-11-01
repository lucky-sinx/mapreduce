package com.wy.mapreduce.partnion2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePatition extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
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
}
