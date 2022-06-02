package com.koi.mapreduce.lab4.Kmeans;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class KMeans {
    private static final String HADOOP_HOME = System.getenv("HADOOP_HOME");

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String input_points = "test/lab4_data/kmeans";
        String base_output = "output/";
        int K = 16;
        int max_iteration = 50;

        KMeans_Cluster.run(input_points, base_output + "it" + 0, K);
        for (int i = 0; i < max_iteration; ++i) {
            String input = base_output + "it" + i + "/part-r-00000";
            String output = base_output + "it" + (i + 1);
            KMeans_Trainer.run(input, input_points, output);
        }
        KMeans_Reviewer.run(base_output + "it" + max_iteration + "/part-r-00000", input_points, base_output + "result");
    }

    private static class KMeans_Cluster {
        public static void run(String input, String output, int K) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration config = new Configuration();
            config.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
            config.setInt("K", K);
            Job job = new Job(config, "Cluster");
            job.setJarByClass(KMeans_Cluster.class);
            job.setMapperClass(Cluster_Mapper.class);
            job.setReducerClass(Cluster_Reducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);
        }

        private static class Cluster_Mapper extends Mapper<Object, Text, IntWritable, Text> {
            private int K;

            @Override
            protected void setup(Context context) throws IOException, InterruptedException {
                K = context.getConfiguration().getInt("K", 0);
            }

            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                if (K > 0) {
                    // 初始化k个聚类中心
                    String[] split = value.toString().split("\t");
                    // 放到reduce阶段
                    context.write(new IntWritable(Integer.parseInt(split[0])), new Text(split[1]));
                    K--;
                }
            }
        }

        private static class Cluster_Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
            @Override
            protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                // 直接将k个聚类中心写到文件中
                for (Text value : values) {
                    context.write(key, value);
                }
            }
        }
    }

    private static class KMeans_Trainer {
        public static void run(String cluster, String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration config = new Configuration();
            config.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
            Job job = new Job(config, "Trainer");
            job.setJarByClass(KMeans_Trainer.class);
            job.setMapperClass(KMeans_Mapper.class);
            job.setReducerClass(KMeans_Reducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            job.addCacheFile(new Path(cluster).toUri());
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);
        }

        private static class KMeans_Mapper extends Mapper<Object, Text, IntWritable, Text> {
            // 聚类中心
            private final List<Point> cluster = new ArrayList<>();

            @Override
            protected void setup(Context context) throws IOException {
                String line;
                // 从文件中读取聚类中心，只放了第二维坐标，此处猜测第一个应该是类别，第二个为坐标，然后坐标可以通过，进行分割
                for (URI file : Job.getInstance(context.getConfiguration()).getCacheFiles()) {
                    Path path = new Path(file.getPath());
                    BufferedReader in = new BufferedReader(new FileReader(path.getName()));
                    while ((line = in.readLine()) != null) {
                        cluster.add(new Point(line.split("\t")[1]));
                    }
                }
            }

            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                // 对于读进来的每一个点，重新计算它所属的聚类中心
                Point p = new Point(value.toString().split("\t")[1]);
                int k = 0;
                for (int i = 1; i < cluster.size(); ++i) {
                    double d1 = p.sub(cluster.get(k)).abs2();
                    double d2 = p.sub(cluster.get(i)).abs2();
                    if (d1 > d2) {
                        k = i;
                    }
                }
                // 将其交给reduce，分别为所属的类和点信息
                context.write(new IntWritable(k), new Text(p.toString()));
            }
        }

        private static class KMeans_Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
            @Override
            protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                int n = 0;
                Point p = new Point(0, 0, 0);
                for (Text value : values) {
                    p = p.add(new Point(value.toString()));
                    n++;
                }
                // 重新计算聚类中心
                context.write(key, new Text(p.divide(n).toString()));
            }
        }
    }

    private static class KMeans_Reviewer {
        public static void run(String cluster, String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration config = new Configuration();
            config.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
            Job job = new Job(config, "Reviewer");
            job.setJarByClass(KMeans_Reviewer.class);
            job.setMapperClass(Reviewer_Mapper.class);
            job.setReducerClass(Reviewer_Reducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            job.addCacheFile(new Path(cluster).toUri());
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);
        }

        private static class Reviewer_Mapper extends Mapper<Object, Text, IntWritable, IntWritable> {
            private final List<Point> cluster = new ArrayList<>();

            @Override
            protected void setup(Context context) throws IOException {
                String line;
                for (URI file : Job.getInstance(context.getConfiguration()).getCacheFiles()) {
                    Path path = new Path(file.getPath());
                    BufferedReader in = new BufferedReader(new FileReader(path.getName()));
                    while ((line = in.readLine()) != null) {
                        cluster.add(new Point(line.split("\t")[1]));
                    }
                }
            }

            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String[] split = value.toString().split("\t");
                int id = Integer.parseInt(split[0]);
                Point p = new Point(split[1]);
                int k = 0;
                for (int i = 1; i < cluster.size(); ++i) {
                    double d1 = p.sub(cluster.get(k)).abs2();
                    double d2 = p.sub(cluster.get(i)).abs2();
                    if (d1 > d2) {
                        k = i;
                    }
                }
                context.write(new IntWritable(k), new IntWritable(id));
            }
        }

        private static class Reviewer_Reducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
            @Override
            protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                List<Integer> points = new ArrayList<>();
                for (IntWritable value : values) {
                    points.add(value.get());
                }
                Collections.sort(points);
                context.write(key, new Text(StringUtils.join(",", points.stream().map(String::valueOf).collect(Collectors.toList()))));
            }
        }
    }

    private static class Point {
        public double x, y, z;

        Point(String s) {
            double[] pos = Arrays.stream(s.toString().split(",")).mapToDouble(Double::parseDouble).toArray();
            x = pos[0];
            y = pos[1];
            z = pos[2];
        }

        Point(double _x, double _y, double _z) {
            x = _x;
            y = _y;
            z = _z;
        }

        public Point add(final Point t) {
            return new Point(x + t.x, y + t.y, z + t.z);
        }

        public Point sub(final Point t) {
            return new Point(x - t.x, y - t.y, z - t.z);
        }

        public Point divide(double k) {
            return new Point(x / k, y / k, z / k);
        }

        public double abs2() {
            return x * x + y * y + z * z;
        }

        public String toString() {
            return String.format("%.10f,%.10f,%.10f", x, y, z);
        }
    }
}
