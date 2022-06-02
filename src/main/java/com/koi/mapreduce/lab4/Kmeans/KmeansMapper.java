package com.koi.mapreduce.lab4.Kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;

/**
 * @author koi
 * @date 2022/5/19 19:57
 */
public class KmeansMapper extends Mapper<Object, Text, Text, Text> {

    public KmeansMapper() throws IOException, InterruptedException {
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    }
}