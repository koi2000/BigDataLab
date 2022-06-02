package com.koi.mapreduce.invertedIndex;

import com.koi.mapreduce.lab1.mining.MineDriver;
import com.koi.mapreduce.lab1.mining.MineMapper;
import com.koi.mapreduce.lab1.mining.MineReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
/*
diff <(./part-r-00000) <(hdfs dfs -cat /lab2/output/s17/part-r-00000)

hadoop jar MapReduceDemo-1.0-SNAPSHOT-jar-with-dependencies.jar com.koi.mapreduce.invertedIndex.PageRankDriver "/ex3/input" "/user/bigdata_202000130061/outputs/output4"
"/ex3/input" "/lab3/outputs/output7/output41"
*/
public class InvertedIndexDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME","bigdata_202000130061");
        System.setProperty("HADOOP_USER_PASSWORD","PASSWORD");

        //获取job
        Configuration conf = new Configuration();
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String[] otherArgs = args;
        Job job = Job.getInstance();

        //设置jar包路径
        job.setJarByClass(InvertedIndexDriver.class);

        //关联mapper和reduce
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

//        job.setInputFormatClass(WholeFileInputFormat.class);
        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 6 设置输入路径和输出路径

        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // 7 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
