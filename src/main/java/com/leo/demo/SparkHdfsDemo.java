package com.leo.demo;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by xucongjie on 2017/9/14.
 */
public class SparkHdfsDemo extends DemoBase {

    private final static String WORD_COUNT_FILE = "/wordcount/wordcount.txt";

    private final static Logger log = Logger.getLogger(SparkHdfsDemo.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(HDFS_MASTER+WORD_COUNT_FILE);
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile(HDFS_MASTER+"/"+args[0]);
    }

}
