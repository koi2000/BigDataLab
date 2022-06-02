package com.koi.mapreduce.lab4.NaiveBayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class NaiveBayesClassifier {
    private static final String HADOOP_HOME = System.getenv("HADOOP_HOME");
    private static final HashMap<Integer, Integer> mp = new HashMap<>();
    private static int M = 0;

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration config = new Configuration();
        config.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
        Job job = new Job(config, "NaiveBayesTrainer");
        job.setJarByClass(NaiveBayesClassifier.class);
        job.setMapperClass(NaiveBayes_Mapper.class);
        job.setReducerClass(NaiveBayes_Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("test/lab4_data/nb"));
        FileOutputFormat.setOutputPath(job, new Path("output_NB"));
        job.waitForCompletion(true);
    }

    private static class NaiveBayes_Mapper extends Mapper<Object, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int[] data = Arrays.stream(value.toString().split(" ")).mapToInt(Integer::parseInt).toArray();
            int[] x = Arrays.copyOf(data, data.length - 1);
            int y = data[data.length - 1];
            M++;
            mp.put(y, mp.getOrDefault(y, 0) + 1);
            context.write(new Text("#" + y), one);
            for (int i = 0; i < x.length; ++i) {
                context.write(new Text(String.format("%d,%d,%d", i, x[i], y)), one);
            }
        }
    }

    private static class NaiveBayes_Reducer extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) sum += value.get();
            if (key.toString().startsWith("#"))
                context.write(key, new Text(String.format("%.10f", (double) sum / M)));
            else {
                int y = Integer.parseInt(key.toString().split(",")[2]);
                context.write(key, new Text(String.format("%.10f", (double) sum / mp.get(y))));
            }
        }
    }
}