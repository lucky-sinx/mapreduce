package com.wy.mapreduce.bigdatahomework.countryanddate;

import com.wy.mapreduce.bigdatahomework.CountryTable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CountryDateMapper extends Mapper<LongWritable, Text, CountryDate, CountryTable> {
    private String fileName;
    private CountryDate outK = new CountryDate();
    private CountryTable outV = new CountryTable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CountryDate, CountryTable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        if (this.fileName.contains("Country")) {
            if (split[0].matches("202.-..-..")){
                outK.setCountry(split[3]);
                outK.setDate(split[0]);
                outV.setDate(split[0]);
                outV.setQuery(split[1]);
                outV.setImplicitIntent(Boolean.parseBoolean(split[2]));
                outV.setCountry(split[3]);
                outV.setPopularityScore(Integer.parseInt(split[4]));
                context.write(outK, outV);
            }
        }
    }

    @Override
    protected void setup(Mapper<LongWritable, Text, CountryDate, CountryTable>.Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        this.fileName = inputSplit.getPath().getName();
    }
}
