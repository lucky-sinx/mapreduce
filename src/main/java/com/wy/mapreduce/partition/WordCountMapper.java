package com.wy.mapreduce.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN, map输入key类型
 * VALUEIN, map输出value类型
 * KEYOUT, map输出key类型
 * VALUEOUT, map输出value类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割出每一行的单词
        String[] words = line.split(" ");
        //循环写出
        for (String word : words) {
            outK.set(word);
            context.write(outK, outV);
        }
    }
}
