package com.koi.mapreduce.invertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    Text value = new Text();
    StringBuilder stringBuilder = new StringBuilder();

    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.get(URI.create("hdfs://10.102.0.198:9000"), configuration, "bigdata_202000130061");
    //    String remotePath = "/user/root/lab2/stop/stop_words_eng.txt";
    String remotePath = "/stop_words/stop_words_eng.txt";

    Set<String> set;

    public InvertedIndexReducer() throws IOException, InterruptedException {
        // 读取停止词
        FSDataInputStream open = fs.open(new Path(remotePath));

        byte[] cs = new byte[1024];//存储读取到的多个字符
        int len = 0;
        while ((len = open.read(cs)) != -1) {
            stringBuilder.append(new String(cs, 0, len));
        }
        open.close();
        String stop = stringBuilder.toString();
        stop = stop.replaceAll("[^a-zA-Z0-9]", " ");
        String[] splits = stop.split("[ |\n]");

        set = new HashSet<>();
        for (String o : splits) {
            if (o.length() >= 1) {
                set.add(o);
            }
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if (set.contains(key.toString())) {
            return;
        }
        // 创建一个TreeMap
        Map<String, Integer> integerMap = new TreeMap<>();
        // 解析每一个value，此处的values为一个集合，key为一个词，values为k:v格式
        values.forEach(o -> {
            String s = o.toString();
            // 分割词，去除文档和出现次数
            String[] split = s.split(":");
            // 如果能找到文档，则++
            if (integerMap.containsKey(split[0])) {
                Integer integer = integerMap.get(split[0]);
                integer++;
                integerMap.put(split[0], integer);
            } else {
                integerMap.put(split[0], 1);
            }
        });
        StringBuilder fins = new StringBuilder();
        // 最后处理数据
        int sum = 0;
        for (Map.Entry<String, Integer> entry : integerMap.entrySet()) {
            String k = entry.getKey();
            Integer v = entry.getValue();
            String tmp = "<" + k + "," + v.toString() + ">;";
            sum += v;
            fins.append(tmp);
        }
        String tmp = "<total," + Integer.toString(sum) + ">.";

        fins.append(tmp);
        String s = fins.toString();
        value.set(s);
        context.write(key, value);
    }

}
