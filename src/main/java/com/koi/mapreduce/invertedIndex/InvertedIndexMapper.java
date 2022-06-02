package com.koi.mapreduce.invertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
    // 在map阶段获得文件名和文件中单词出现的次数
    // 然后统计词频
    IntWritable intWritable = new IntWritable(1);
    Text text = new Text();
    Text kv = new Text();
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.87.144:9000"), configuration, "root");
    ;

    public InvertedIndexMapper() throws IOException, InterruptedException {
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 首先获得当前文件的名称
        String name = ((FileSplit) context.getInputSplit()).getPath().getName();
        // 将当前行转化为string
        String val = value.toString();
        kv.set(val);

//        List<Word> words = WordSegmenter.seg(val);
        // 去除符号等
        val = val.replaceAll("[^a-zA-Z0-9]", " ");
        // 将string以换行符和空格进行分割
        String[] split = val.split("[ |\n]");
        // value为文件名+词频，此处均设置为1
        kv.set(name + ":" + 1);
        for (String o : split) {
            // 只有非空字符串才会交给reduce
            if (o.length() >= 1) {
                // 全部转化为小写
                this.text.set(o.toLowerCase());
                context.write(this.text, kv);
            }
        }
//        for (Word o : words) {
//            String txt = o.getText();
//            //System.out.println(txt);
//            this.text.set(o.getText().toLowerCase());
//            context.write(this.text, kv);
//        }
    }
}
