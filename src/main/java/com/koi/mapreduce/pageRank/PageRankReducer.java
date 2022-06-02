package com.koi.mapreduce.pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    Text value = new Text();
    StringBuilder stringBuilder = new StringBuilder();

    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.get(URI.create("hdfs://10.102.0.198:9000"), configuration, "bigdata_202000130061");
    //    String remotePath = "/user/root/lab2/stop/stop_words_eng.txt";
    String remotePath = "/stop_words/stop_words_eng.txt";

    Set<String> set;

    public PageRankReducer() throws IOException, InterruptedException {
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double prev = 0.0;
        Double now = 0.0;
        String relate = "";
        for (Text text : values) {
            String val = text.toString();
            if (val.charAt(0) == '&') {
                String substring = val.substring(1);
                now = Double.parseDouble(substring);
            }
            if (val.charAt(0) == '!') {
                String substring = val.substring(1);
//                BigDecimal bg = BigDecimal.valueOf(Double.parseDouble(substring));
//                Double d = bg.setScale(10, BigDecimal.ROUND_HALF_UP).doubleValue();
                prev += Double.parseDouble(substring);
            }
            if (val.charAt(0) == '#') {
                relate = val.substring(1);
            }
        }
        prev = 0.15 + prev * 0.85;
//        BigDecimal bg = BigDecimal.valueOf(prev);
//        prev = bg.setScale(10, BigDecimal.).doubleValue();

        String res = relate + "\t" + prev;
        value.set(res);
        context.write(key, value);
    }

}
