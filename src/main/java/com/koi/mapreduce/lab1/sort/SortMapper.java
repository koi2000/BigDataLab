package com.koi.mapreduce.lab1.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        IntWritable intWritable = new IntWritable(1);
        IntWritable ints = new IntWritable(Integer.parseInt(value.toString()));
        context.write(intWritable, ints);
    }

}
