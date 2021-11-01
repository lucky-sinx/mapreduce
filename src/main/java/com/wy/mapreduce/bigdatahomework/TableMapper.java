package com.wy.mapreduce.bigdatahomework;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, CountryTable> {
    private String fileName;
    private Text outK = new Text();
    private CountryTable outV = new CountryTable();
    private String country = "China";

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CountryTable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        if (this.fileName.contains("Country")) {
            if (split[0].matches("2020-..-01")) {
                outK.set(split[3]);
                outV.setDate(split[0]);
                outV.setQuery(split[1]);
                outV.setImplicitIntent(Boolean.parseBoolean(split[2]));
                outV.setCountry(split[3]);
                outV.setPopularityScore(Integer.parseInt(split[4]));
                context.write(outK, outV);
            }
        }
//        if (split[3].equals("United Kingdom") && split[0].equals("2020-01-01")) {
//            outV.set(split[0] + split[3]);
//            outK.setDate(split[0]);
//            outK.setQuery(split[1]);
//            outK.setImplicitIntent(Boolean.parseBoolean(split[2]));
//            outK.setCountry(split[3]);
//            outK.setPopularityScore(Integer.parseInt(split[4]));
//            context.write(outK, outV);
//        }
    }

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, CountryTable>.Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        this.fileName = inputSplit.getPath().getName();
    }
}
