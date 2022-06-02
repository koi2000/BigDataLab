package com.koi.mapreduce.lab1.mergeAndUnique;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MergeMapper extends Mapper<Object,Text,Text,Text> {
    private static Text text = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        text = value;
        context.write(text,new Text(""));
    }
}
