package com.koi.spark;

import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Function1;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * @author koi
 * @date 2022/4/29 20:54
 */
public class PageRankSql {

    private static String path = "hdfs://10.102.0.198:9000/ex3/input/DataSet";
    private static String master = "spark://192.168.87.144:7077";

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {


        SparkConf sc = new SparkConf().setMaster(master)
                .setAppName("PageRank");
        JavaSparkContext jsc = new JavaSparkContext(sc);

        SQLContext sqlContext = new SQLContext(jsc);

        // 总结一下
        // jdbc数据源
        // 首先，是通过SQLContext的read系列方法，将mysql中的数据加载为DataFrame
        // 然后可以将DataFrame转换为RDD，使用Spark Core提供的各种算子进行操作
        // 最后可以将得到的数据结果，通过foreach()算子，写入mysql、hbase、redis等等db / cache中

        String url = "jdbc:mysql://152.136.26.27:33306/lab";
        //查找的表名
        String table = "pageRank";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","123456");
        connectionProperties.put("driver","com.mysql.cj.jdbc.Driver");
        //SparkJdbc读取Postgresql的products表内容
        System.out.println("读取test数据库中的user_test表内容");
        // 读取表中所有数据
        Dataset<Row> jdbc = sqlContext.read().jdbc(url,table,connectionProperties).select("*");
//        Map<String, String> options = new HashMap<String, String>();
//        options.put("url", "jdbc:mysql://152.136.26.27:33306/lab");
//        options.put("user","root");
//        options.put("password","123456");
//        options.put("dbtable", "pageRank");
//
//
//        Dataset<Row> jdbc = sqlContext.read().jdbc().format("jdbc")
//                .options(options).load();
        jdbc.show();
        jdbc.foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row);
            }
        });
        System.out.println(jdbc.toJSON());
        JavaRDD<Row> data = jdbc.javaRDD();

        JavaPairRDD<Long, Long> pairRDD = data.mapToPair(new PairFunction<Row, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Row row) throws Exception {
                return null;
            }
        });

        JavaPairRDD<Long, Iterable<Long>> groupRDD = pairRDD.groupByKey();

        //初始化rank 设置每一个页面的初始权重为1.0，使用mapValue生成RDD
        JavaPairRDD<Long, Double> ranks = groupRDD.mapValues(new Function<Iterable<Long>, Double>() {
            @Override
            public Double call(Iterable<Long> strings) throws Exception {
                return 1.0;
            }
        });

        //迭代计算更新每个页面的rank，迭代次数可以自由设定，最好是设置结束条件：收敛结束
        for (int i = 0; i < 10; ++i) {
            JavaPairRDD<Long, Double> contribs = groupRDD.join(ranks).values().flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<Long>,Double>, Long, Double>() {
                @Override
                public Iterator<Tuple2<Long, Double>> call(Tuple2<Iterable<Long>, Double> iterableDoubleTuple2) throws Exception {
                    int urlCount = Iterables.size(iterableDoubleTuple2._1());
                    List<Tuple2<Long, Double>> results = new ArrayList<>();
                    for (Long str : iterableDoubleTuple2._1) {
                        results.add(new Tuple2<Long, Double>(str, iterableDoubleTuple2._2 / urlCount));
                    }
                    return results.iterator();
                }
            });

            //重新计算URL 的 ranks值 基于临接网页
            ranks = contribs.reduceByKey(new Function2<Double, Double, Double>() {
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

        //输出所有的页面的pageRank 值
        JavaRDD<String> ret = ranks.mapToPair(s -> new Tuple2<>(s._2(), s._1()))
                .sortByKey(false)
                .map(s -> String.format("(%s,%.10f)", s._2(), s._1()));
        ret.saveAsTextFile("/user/bigdata_202000130061/outputs/out"+args[0]);
    }

    static class lab implements Serializable {
        public Integer id;
        public Long kkey;
        public Long vvalue;
    }
}
