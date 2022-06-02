package com.koi.spark;

import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

//import java.io.Serializable;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

// ./spark submit class com.koi.spark.PageRank PageRank_Spark.jar
// ./spark-submit --class com.koi.spark.PageRank MapReduceDemo-1.0-SNAPSHOT-jar-with-dependencies.jar
public class PageRank implements Serializable {

    //private static final long serialVersionUID = 5378738997755484868L;
    //private static String path = "hdfs://192.168.87.144:9000/lab3/input/DataSet";
    private static String path = "hdfs://10.102.0.198:9000/ex3/input/DataSet";
    private static String master = "spark://10.102.0.198:7077";

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "bigdata_202000130061");
        System.setProperty("HADOOP_USER_PASSWORD", "PASSWORD");
        SparkConf sc = new SparkConf().setMaster(master)
                .setAppName("PageRank");

        JavaSparkContext jsc = new JavaSparkContext(sc);


        JavaRDD<String> data = jsc.textFile(path);
        // 初始化数据
        JavaPairRDD<String, Iterable<String>> line = data.mapToPair(new PairFunction<String, String, Iterable<String>>() {
            @Override
            public Tuple2<String, Iterable<String>> call(String s) throws Exception {
                String[] split = s.split("\t");
                return new Tuple2<>(split[0], Arrays.asList(split[1].split(",")));
            }
        });

        //初始化rank 设置每一个页面的初始权重为1.0，使用mapValue生成RDD
        JavaPairRDD<String, Double> ranks = line.mapValues(new Function<Iterable<String>, Double>() {
            @Override
            public Double call(Iterable<String> strings) throws Exception {
                return 1.0;
            }
        });

        //迭代计算更新每个页面的rank，此处迭代十次
        for (int i = 0; i < 10; ++i) {
            JavaPairRDD<String, Double> joins = line.join(ranks).values().flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>,Double>, String, Double>() {
                @Override
                public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> iterableDoubleTuple2) throws Exception {
                    int urlCount = Iterables.size(iterableDoubleTuple2._1());
                    List<Tuple2<String, Double>> results = new ArrayList<>();
                    for (String str : iterableDoubleTuple2._1) {
                        results.add(new Tuple2<String, Double>(str, iterableDoubleTuple2._2 / urlCount));
                    }
                    return results.iterator();
                }
            });

            //重新计算网页 的 ranks值
            ranks = joins.reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double aDouble, Double aDouble2) throws Exception {
                    return aDouble + aDouble2;
                }
            }).mapValues(new Function<Double, Double>() {
                @Override
                public Double call(Double aDouble) throws Exception {
                    return 0.15 + aDouble * 0.85;
                }
            });
        }

        //保存最后结果
        JavaRDD<String> ret = ranks.mapToPair(s -> new Tuple2<>(s._2(), s._1()))
                .sortByKey(false)
                .map(s -> String.format("(%s,%.10f)", s._2(), s._1()));
        ret.saveAsTextFile("/user/bigdata_202000130061/outputs/out"+args[0]);
    }
}
