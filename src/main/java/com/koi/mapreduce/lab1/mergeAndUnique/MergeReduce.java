package com.koi.mapreduce.lab1.mergeAndUnique;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MergeReduce extends Reducer<Text, Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key,new Text(""));
    }
}
