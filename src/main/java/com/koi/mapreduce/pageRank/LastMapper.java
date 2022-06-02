package com.koi.mapreduce.pageRank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author koi
 * @date 2022/4/27 10:20
 */
public class LastMapper extends Mapper<Object, Text, Text, Text> {

    IntWritable intWritable = new IntWritable(1);
    Text text = new Text();
    Text kv = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 将当前行转化为string
        String val = value.toString();

        // 以\t作为分隔符
        String[] html = val.split("\t");
        text.set("1");
        kv.set(html[0]+":"+html[2]);
        context.write(text,kv);
    }
}
