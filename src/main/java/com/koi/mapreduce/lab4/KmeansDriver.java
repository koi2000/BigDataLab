package com.koi.mapreduce.lab4;


import com.google.common.collect.Ranges;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
import java.util.*;
import java.util.stream.Collectors;

public class KmeansDriver {
    private static final String HADOOP_HOME = System.getenv("HADOOP_HOME");

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("HADOOP_USER_PASSWORD", "PASSWORD");
        String input_points = "/lab4";
        String base_output = "output24/";
        int K = 16;
        int max_iteration = 5;

        KMeans_Cluster.run(input_points, base_output + "it" + 0, K);
        for (int i = 0; i < max_iteration; ++i) {
            String input = base_output + "it" + i + "/part-r-00000";
            String output = base_output + "it" + (i + 1);
            // 迭代，得到聚类中心
            // 第一个input为聚类中心，第二个为点信息，最后一个为输出，即为新的聚类中心
            KMeans_Trainer kMeans_trainer = new KMeans_Trainer(input);
            kMeans_trainer.run(input, input_points, output);
        }
        String in = base_output + "it" + max_iteration + "/part-r-00000";
        new KMeans_Reviewer(in).run(in, input_points, base_output + "result");
    }

    private static class KMeans_Cluster {
        public static void run(String input, String output, int K) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration config = new Configuration();
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
                    context.write(new IntWritable(K),value);
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
        //private static String uri;

        // 聚类中心
        private static List<Point> cluster = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.87.144:9000"), configuration, "root");

        private KMeans_Trainer(String uri) throws IOException, InterruptedException {
            FSDataInputStream open = fs.open(new Path(uri));

            byte[] cs = new byte[1024];//存储读取到的多个字符
            int len = 0;
            while ((len = open.read(cs)) != -1) {
                stringBuilder.append(new String(cs, 0, len));
            }
            open.close();
            cluster = new ArrayList<>();
            String stop = stringBuilder.toString();
            String[] split = stop.split("\n");
            for (String s : split) {
                String[] line = s.split("\t");
                cluster.add(new Point(line[1]));
            }
        }

        public void run(String cluster, String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration config = new Configuration();
            Job job = new Job(config, "Trainer");
            job.setJarByClass(KMeans_Trainer.class);
            job.setMapperClass(KMeans_Mapper.class);
            job.setReducerClass(KMeans_Reducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            //job.addCacheFile(new Path(cluster).toUri());
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);
        }

        private static class KMeans_Mapper extends Mapper<Object, Text, IntWritable, Text> {

            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                // 对于读进来的每一个点，重新计算它所属的聚类中心
                Point p = new Point(value.toString());
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
                Point p = new Point(new ArrayList<>());
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
        // 聚类中心
        private static List<Point> cluster = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.87.144:9000"), configuration, "root");

        public void run(String cluster, String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration config = new Configuration();
            Job job = new Job(config, "Reviewer");
            job.setJarByClass(KMeans_Reviewer.class);
            job.setMapperClass(Reviewer_Mapper.class);
            job.setReducerClass(Reviewer_Reducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.addCacheFile(new Path(cluster).toUri());
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);
        }

        private KMeans_Reviewer(String uri) throws IOException, InterruptedException {
            FSDataInputStream open = fs.open(new Path(uri));

            byte[] cs = new byte[1024];//存储读取到的多个字符
            int len = 0;
            while ((len = open.read(cs)) != -1) {
                stringBuilder.append(new String(cs, 0, len));
            }
            open.close();
            cluster = new ArrayList<>();
            String stop = stringBuilder.toString();
            String[] split = stop.split("\n");
            for (String s : split) {
                String[] line = s.split("\t");
                cluster.add(new Point(line[1]));
            }
        }

        private static class Reviewer_Mapper extends Mapper<Object, Text, Text, Text> {
            private final List<Point> cluster = new ArrayList<>();

//            @Override
//            protected void setup(Context context) throws IOException {
//                String line;
//                for (URI file : Job.getInstance(context.getConfiguration()).getCacheFiles()) {
//                    Path path = new Path(file.getPath());
//                    BufferedReader in = new BufferedReader(new FileReader(path.getName()));
//                    while ((line = in.readLine()) != null) {
//                        cluster.add(new Point(line));
//                    }
//                }
//            }

            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//                Point p = new Point(value.toString());
//                int k = 0;
//                for (int i = 1; i < cluster.size(); ++i) {
//                    double d1 = p.sub(cluster.get(k)).abs2();
//                    double d2 = p.sub(cluster.get(i)).abs2();
//                    if (d1 > d2) {
//                        k = i;
//                    }
//                }
                context.write(new Text("1"), value);
            }
        }

        private static class Reviewer_Reducer extends Reducer<Text, Text, Text, Text> {
            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                cluster.sort(new Comparator<Point>() {
                    @Override
                    public int compare(Point o1, Point o2) {
                        double sum1 = 0.0;
                        double sum2 = 0.0;
                        for (Double aDouble : o1.coordinate) {
                            sum1 += aDouble;
                        }
                        for (Double aDouble : o2.coordinate) {
                            sum2 += aDouble;
                        }
                        return (int) (sum1 - sum2);
                    }
                });
                for (int i = 0; i < cluster.size(); i++) {
                    context.write(new Text(""+i),new Text(cluster.get(i).toString()));
                }
//                List<Integer> points = new ArrayList<>();
//                for (IntWritable value : values) {
//                    points.add(value.get());
//                }
//                Collections.sort(points);
//                context.write(key, new Text(StringUtils.join(",", points.stream().map(String::valueOf).collect(Collectors.toList()))));
            }
        }
    }

    private static class Point {
        public ArrayList<Double> coordinate = new ArrayList<>();

        Point(String s) {
            coordinate = new ArrayList<>();
            double[] doubles = Arrays.stream(s.toString().split(",")).mapToDouble(Double::parseDouble).toArray();
            for (double aDouble : doubles) {
                coordinate.add(aDouble);
            }
        }

        Point(double[] point) {
            coordinate = new ArrayList<>();
            for (double aDouble : point) {
                coordinate.add(aDouble);
            }
        }

        Point(ArrayList<Double> coordinate) {
            this.coordinate = new ArrayList<>();
            this.coordinate.addAll(coordinate);
        }

        public Point add(final Point t) {
            ArrayList<Double> tmp = new ArrayList<>();
            for (int i = 0; i < t.coordinate.size(); i++) {
                if (coordinate.size() <= i) {
                    tmp.add(t.coordinate.get(i));
                    continue;
                }
                tmp.add(t.coordinate.get(i) + coordinate.get(i));
            }
            return new Point(tmp);
        }

        public Point sub(final Point t) {
            ArrayList<Double> tmp = new ArrayList<>();
            for (int i = 0; i < t.coordinate.size(); i++) {
                if (coordinate.size() < i) {
                    tmp.add(t.coordinate.get(i));
                    continue;
                }
                tmp.add(-t.coordinate.get(i) + coordinate.get(i));
            }
            return new Point(tmp);
        }

        public Point divide(double k) {
            ArrayList<Double> tmp = new ArrayList<>();
            for (int i = 0; i < coordinate.size(); i++) {
                tmp.add(coordinate.get(i) / k);
            }
            return new Point(tmp);
        }

        public double abs2() {
            double res = 0;
            for (int i = 0; i < coordinate.size(); i++) {
                res += Math.pow(coordinate.get(i), 2);
            }
            return res;
        }

        public String toString() {
            StringBuilder res = new StringBuilder();
            for (Double aDouble : coordinate) {
                res.append(String.format("%.10f,", aDouble));
            }
            res.deleteCharAt(res.length() - 1);
            return res.toString();
        }
    }
}
