package com.wy.mapreduce.bigdatahomework;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CountryPartition extends Partitioner<Text, CountryTable> {

    @Override
    public int getPartition(Text text, CountryTable countryTable, int i) {
        int partition = -1;
        String country = text.toString();
        switch (country) {
            case "China":
                partition = 1;
                break;
            case "United States":
                partition = 2;
                break;
            case "Italy":
                partition = 3;
                break;
            case "United Kingdom":
                partition = 4;
                break;
            default:
                partition = 0;
                break;
        }
        return partition;
    }
}
