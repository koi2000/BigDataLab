package com.koi.mapreduce.pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
diff <(./part-r-00000) <(hdfs dfs -cat /lab2/output/s17/part-r-00000)

hadoop jar MapReduceDemo-1.0-SNAPSHOT-jar-with-dependencies.jar com.koi.mapreduce.pageRank.PageRankDriver "/ex3/input" "/user/bigdata_202000130061/outputs/output4"
"/ex3/input" "/lab3/outputs/output7/output41"
*/
public class PageRankDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "bigdata_202000130061");
        System.setProperty("HADOOP_USER_PASSWORD", "PASSWORD");

        //获取job
        Configuration conf = new Configuration();
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String[] otherArgs = args;
        String inPath = otherArgs[0];
        String outPath = otherArgs[1];
        boolean result = true;
        for (int i = 0; i < 10; i++) {
            Job job = Job.getInstance();

            //设置jar包路径
            job.setJarByClass(PageRankDriver.class);

            //关联mapper和reduce
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);

            //job.setInputFormatClass(WholeFileInputFormat.class);
            //设置输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //动态更新输入输出路径
            FileInputFormat.setInputPaths(job, new Path(inPath));
            FileOutputFormat.setOutputPath(job, new Path(outPath + i));
            inPath = (outPath+i);
            // 7 提交job
            result &= job.waitForCompletion(true);
        }

        Job job = Job.getInstance();

        //设置jar包路径
        job.setJarByClass(PageRankDriver.class);

        //关联mapper和reduce
        job.setMapperClass(LastMapper.class);
        job.setReducerClass(LastReduce.class);

        //job.setInputFormatClass(WholeFileInputFormat.class);
        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //动态更新输入输出路径
        FileInputFormat.setInputPaths(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath + "fin"));
        // 7 提交job
        result &= job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
