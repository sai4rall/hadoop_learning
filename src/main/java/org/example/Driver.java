package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration=new Configuration();
        Job j=new Job(configuration,"WordCount");
        j.setJarByClass(Driver.class);
        j.setMapperClass(WordMap.class);
        j.setReducerClass(WordReducer.class);
        FileInputFormat.addInputPath(j,new Path(args[0]));
        FileOutputFormat.setOutputPath(j,new Path(args[1]));
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
System.out.println("=======================");

    }
}