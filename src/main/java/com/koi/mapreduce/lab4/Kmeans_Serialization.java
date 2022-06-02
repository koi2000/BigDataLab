package com.koi.mapreduce.lab4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collector;

/**
 * @author koi
 * @date 2022/5/19 22:58
 */
public class Kmeans_Serialization {
    private static ArrayList<Point> cluster = new ArrayList<>();

    private static String[] allPoints;
    private static int K = 16;

    public static void main(String[] args) throws IOException {

        int max_iteration = 5;
        FileInputStream fis = new FileInputStream("F:\\课程之外\\大数据\\hadoop\\代码\\MapReduceDemo\\src\\main\\java\\com\\koi\\mapreduce\\lab4\\wine.data");
        StringBuilder stringBuilder = new StringBuilder();
        byte[] cs = new byte[1024];//存储读取到的多个字符
        int len = 0;
        while ((len = fis.read(cs)) != -1) {
            stringBuilder.append(new String(cs, 0, len));
        }
        fis.close();
        cluster = new ArrayList<>();
        String allPoint = stringBuilder.toString();
        allPoints = allPoint.split("\n");
        for (int i = 0; i < K; i++) {
            //String[] line = allPoints[i].split("\t");
            //cluster.add(new Point(line[1]));
            cluster.add(new Point(allPoints[i]));
        }

        for (int i = 0; i < max_iteration; ++i) {
            train();
        }
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

        BufferedWriter bw = new BufferedWriter(new FileWriter("./a1.txt"));
        // 遍历集合
        for (int i = 0; i < cluster.size(); i++) {
            String s = i + "\t" + cluster.get(i).toString();
            bw.write(s);
            bw.newLine();
        }
        bw.flush();
        // 释放资源
        bw.close();
    }

    public static void train() {
        ArrayList<Point>[] clu = new ArrayList[K];
        for (int i = 0; i < clu.length; i++) {
            clu[i] = new ArrayList<>();
        }
        for (String allPoint : allPoints) {
            Point p = new Point(allPoint);
            int k = 0;
            for (int i = 1; i < cluster.size(); ++i) {
                double d1 = p.sub(cluster.get(k)).abs2();
                double d2 = p.sub(cluster.get(i)).abs2();
                if (d1 > d2) {
                    k = i;
                }
            }
            clu[k].add(p);
        }
        // 重新计算聚类中心
        for (int i = 0; i < clu.length; i++) {
            Point tmp = new Point(new ArrayList<>());
            for (Point point : clu[i]) {
                tmp = tmp.add(point);
            }
            cluster.set(i, tmp.divide(clu[i].size()));
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
