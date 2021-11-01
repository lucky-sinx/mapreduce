package com.wy.mapreduce.writablecomparable;


import fileutil.MyFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class FLowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.获取job
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.key.field.separator", " ");

        Job job = Job.getInstance(conf);
        //2.设置jar
        job.setJarByClass(FLowDriver.class);
//        job.setJarByClass(WordCountDriver.class);
//        job.setJar("D:\\code\\java\\MapReduceDemo\\src\\main\\java\\com\\wy\\mapreduce\\wordcount\\WordCountDriver.java");
        //3.关联mapper、reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4.设置map的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5.设置最终输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //6.设置输入路径输出路径
        String inputPath = "D:\\code\\data\\hadoopinput\\flowcount_output\\";
        String outputPath = "D:\\code\\data\\hadoopinput\\flowcount_output_writablecomparable\\";

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
