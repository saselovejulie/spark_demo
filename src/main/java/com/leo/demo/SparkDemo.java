package com.leo.demo;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 加载内存的List,提交到Spark集群进行分析
 * Created by xucongjie on 2017/9/13.
 */
public class SparkDemo extends DemoBase {

    private final static Logger log = Logger.getLogger(SparkDemo.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaDemo");
        try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
            //模拟日志
            List<String> data = Arrays.asList("Error: Demo1", "Error: Demo2", "Success: Demo3", "Error: Demo4", "Success: Demo5", "Success: Demo6");
            // jsc是上述代码已经创建的JavaSparkContext实例
            JavaRDD<String> distData = jsc.parallelize(data);
            //只保留Success的日志
            JavaRDD<String> successLines = distData.filter((line) -> line.contains("Success"));
            List<String> successList = successLines.collect();
            successList.forEach(i -> log.info(i));
        }

    }

}
