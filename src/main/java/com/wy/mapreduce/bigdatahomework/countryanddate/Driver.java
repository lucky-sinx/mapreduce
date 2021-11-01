package com.wy.mapreduce.bigdatahomework.countryanddate;

import com.wy.mapreduce.bigdatahomework.*;
import fileutil.MyFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TableDriver.class);
        job.setMapperClass(CountryDateMapper.class);
        job.setReducerClass(CountryDateReducer.class);
        job.setMapOutputKeyClass(CountryDate.class);
        job.setMapOutputValueClass(CountryTable.class);
        job.setOutputKeyClass(CountryTable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setPartitionerClass(Partition.class);
        job.setNumReduceTasks(5);
//        job.setPartitionerClass(CountryPartition.class);
//        job.setNumReduceTasks(5);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClas
//        s(TableBean.class);
//        FileInputFormat.setInputPaths(job, new Path("D:\\code\\data\\hadoopinput\\reducejoin_input"));
//        FileOutputFormat.setOutputPath(job, new Path("D:\\code\\data\\hadoopinput\\reducejoin_output"));
//        String inputPath = "D:\\code\\data\\bigdata_homework_datasets\\Dataset2\\data\\2020\\QueriesByCountry_2020-01-01_2020-01-31.tsv";
        String inputPath = "D:\\code\\data\\bigdata_homework_datasets\\Dataset2\\data\\2020\\";
        String outputPath = "D:\\code\\data\\bigdata_homework_datasets\\wordcount_output";

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        File file = new File(outputPath);
        if (file.exists()) {
            MyFileUtil f = new MyFileUtil();
            f.deleteDirectory(outputPath);
        }
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
