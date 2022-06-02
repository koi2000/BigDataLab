package com.koi.spark;
 
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
 
import java.util.Arrays;
import java.util.Iterator;
 
/**
 * Created by caimh on 2019/10/29.
 */
public class JavaWordCount {
 
    public static void main(String[] args) {
 
        //创建配置文件
        SparkConf conf = new SparkConf().setAppName("JavaWordCount");
        //创建SparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定以后从哪里读取数据
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //切分压平(函数式编程)
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        //将单词和1组合在一起
        JavaPairRDD<String, Integer> wordTuple = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
 
                return new Tuple2<>(word, 1);
            }
        });
        //根据key聚合
        JavaPairRDD<String, Integer> wordReduced = wordTuple.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //排序(方法sordBykey是根据key排序，需要把wordReduced的k,v顺序调换)
        //调换顺序
        JavaPairRDD<Integer, String> wordSwaped = wordReduced.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tp) throws Exception {
                return tp.swap();
            }
        });
        //排序
        wordSwaped.sortByKey(false);
        //调换顺序
        JavaPairRDD<String, Integer> result = wordSwaped.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tp) throws Exception {
                return tp.swap();
            }
        });
        //将数据保存到HDFS
        result.saveAsTextFile(args[1]);
        //释放资源
        jsc.close();
    }
}