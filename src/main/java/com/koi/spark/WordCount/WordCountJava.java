package com.koi.spark.WordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author koi
 * @date 2022/4/27 16:23
 */
public class WordCountJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("localhost").setAppName("javawc");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lineRDD = jsc.textFile("");
        JavaRDD<String> word = lineRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                String[] split = line.split(" ");
                List<String> strings = Arrays.asList(split);
                return strings.iterator();
            }
        });
        JavaPairRDD<String, Integer> pairRDD = word.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+":"+stringIntegerTuple2._2);
            }
        });
    }
}
