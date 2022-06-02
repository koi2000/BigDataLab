package com.koi.mapreduce.lab4.Kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

/**
 * @author koi
 * @date 2022/5/19 19:57
 */
public class KmeansReduce extends Reducer<Text, Text, Text, Text> {

    public KmeansReduce() throws IOException, InterruptedException {
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    }

}