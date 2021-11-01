package com.wy.mapreduce.bigdatahomework.countryanddate;

import com.wy.mapreduce.bigdatahomework.CountryTable;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partition extends Partitioner<CountryDate, CountryTable> {

    @Override
    public int getPartition(CountryDate countryDate, CountryTable countryTable, int i) {
        int partition = -1;
        String country = countryDate.getCountry();
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
