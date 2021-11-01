package com.wy.mapreduce.bigdatahomework;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TableReducer extends Reducer<Text, CountryTable, CountryTable, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<CountryTable> values, Reducer<Text, CountryTable, CountryTable, NullWritable>.Context context) throws IOException, InterruptedException {
        for (CountryTable value : values) {
            context.write(value, NullWritable.get());
        }
    }
}
