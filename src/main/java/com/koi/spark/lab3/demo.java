package com.koi.spark.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class demo {
    private static String appName = "spark.demo";
    private static String master = "local[*]";
    private static String path = "hdfs://192.168.87.144:9000/lab3/input";
    private static String outPath = "hdfs://192.168.87.144:9000/lab3/output8/";


    public static void main(String[] args) {
        String[] otherArgs = args;
        String inPath = otherArgs[0];
        String outPath = otherArgs[1];
        SparkConf conf = new SparkConf().setMaster("local").setAppName("lab3");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lineRDD = jsc.textFile(path);



    }
}
