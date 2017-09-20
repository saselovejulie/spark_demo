package com.leo.demo;

import com.google.gson.Gson;
import com.leo.entity.InspurUserEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by xucongjie on 2017/9/14.
 */
public class SparkHBaseDemo extends DemoBase {

    private final static Logger log = Logger.getLogger(SparkHBaseDemo.class);
    private static Gson gson = new Gson();
    private final static String HBASE_FILE = "/hbase-testdata/";

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseHFile");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //配置需要序列化的类
        Class[] classes = new Class[] {ImmutableBytesWritable.class};
        sparkConf.registerKryoClasses(classes);

        //hbase的表名
        String tableName = "leo_test";
        //生成的HFile的临时保存路径
//        String stagingFolder = HDFS_MASTER+args[0];
        String stagingFolder = args[0];

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //生成HBase的Configuration
        Configuration conf = HBaseConfiguration.create();
        //设置HFile的Table信息
        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        conf.set(FileOutputFormat.OUTDIR, stagingFolder);
        //配置Zookeeper的相关信息
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
//
        JavaRDD<String> hbaseFile = jsc.textFile(HDFS_MASTER+HBASE_FILE);
        JavaPairRDD<ImmutableBytesWritable, Put> putJavaRDD = hbaseFile.mapToPair(line -> convertToPut(line));
        putJavaRDD.sortByKey(true);

        log.info("Start save tmp file into HDFS.");

        //使用TableOutputFormat写入到HBase,不涉及到HFile的生成和使用
        putJavaRDD.saveAsNewAPIHadoopFile(stagingFolder, ImmutableBytesWritable.class, Put.class, TableOutputFormat.class, conf);

    }

    private static Tuple2<ImmutableBytesWritable, Put> convertToPut(String beanString) {
        InspurUserEntity inspurUserEntity = gson.fromJson(beanString, InspurUserEntity.class);
        String rowKey = inspurUserEntity.getDepartment_level1()+"_"+inspurUserEntity.getDepartment_level2()+"_"+inspurUserEntity.getId();

        Put p = new Put(Bytes.toBytes(rowKey));
        p.addColumn(Bytes.toBytes("name"), Bytes.toBytes(""), Bytes.toBytes(inspurUserEntity.getName()));
        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(inspurUserEntity.getAge()));
        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes(inspurUserEntity.getGender()));
        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("seniority"), Bytes.toBytes(inspurUserEntity.getSeniority()));

        p.addColumn(Bytes.toBytes("department"), Bytes.toBytes("level1"), Bytes.toBytes(inspurUserEntity.getDepartment_level1()));
        p.addColumn(Bytes.toBytes("department"), Bytes.toBytes("level2"), Bytes.toBytes(inspurUserEntity.getDepartment_level2()));
        p.addColumn(Bytes.toBytes("department"), Bytes.toBytes("level3"), Bytes.toBytes(inspurUserEntity.getDepartment_level3()));

        p.addColumn(Bytes.toBytes("department"), Bytes.toBytes("leader"), Bytes.toBytes(inspurUserEntity.getDepartment_leader()));

        return new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), p);
    }

}
