package com.koi.mapreduce.pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;

public class PageRankMapper extends Mapper<Object, Text, Text, Text> {
    // 在map阶段获得文件名和文件中单词出现的次数
    // 然后统计词频
    IntWritable intWritable = new IntWritable(1);
    Text text = new Text();
    Text kv = new Text();
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.87.144:9000"), configuration, "root");

    public PageRankMapper() throws IOException, InterruptedException {
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 将当前行转化为string
        String val = value.toString();

        // 以\t作为分隔符
        String[] html = val.split("\t");
        // 以,作为分隔符
        String[] relate = html[1].split(",");
        String tmp = "";
        text.set(html[0]);
        Double res;
        // 获得当前的pageRank值
        if (html.length == 2) {
            kv.set("&"+1);
            res = 1.0;
            context.write(text,kv);
        } else {
            kv.set("&"+html[2]);
            res = Double.parseDouble(html[2]);
            context.write(text,kv);
        }
        // 保存它所链接到的所有词
        kv.set("#" + html[1]);
        context.write(text, kv);

        // 获得该页的权重
        double weight = res / relate.length;

        for (String s : relate) {
            // 该部分存储被指向后被赋予的权值
            tmp = "!" + weight;
            kv.set(tmp);
            text.set(s);
            // k为每个词，v为链接到它的权重
            context.write(text, kv);
        }
    }
}
