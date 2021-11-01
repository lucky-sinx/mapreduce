package com.wy.mapreduce.partition;


import fileutil.MyFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;


public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, IOException {
//        try {
//            // 设置 HADOOP_HOME 目录
//            System.setProperty("hadoop.home.dir", "D:/code/hadoop/hadoop-3.2.2/");
//            // 加载库文件
//            System.load("D:/code/hadoop/hadoop-3.2.2/bin/hadoop.dll");
//        } catch (UnsatisfiedLinkError e) {
//            System.err.println("Native code library failed to load.\n" + e);
//            System.exit(1);
//        }
        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.设置jar
        job.setJarByClass(WordCountDriver.class);
//        job.setJarByClass(WordCountDriver.class);
//        job.setJar("D:\\code\\java\\MapReduceDemo\\src\\main\\java\\com\\wy\\mapreduce\\wordcount\\WordCountDriver.java");
        //3.关联mapper、reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.设置map的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);

        //6.设置输入路径输出路径
        String inputPath = "D:\\code\\data\\hadoopinput\\wordcount\\";
        String outputPath = "D:\\code\\data\\hadoopinput\\wordcount_output_partition\\";

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
//
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7.提交job
        File file = new File(outputPath);
        if (file.exists()) {
            MyFileUtil f = new MyFileUtil();
            f.deleteDirectory(outputPath);
        }

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : -1);
    }

}

