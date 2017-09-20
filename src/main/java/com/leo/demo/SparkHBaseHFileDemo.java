package com.leo.demo;

import com.google.gson.Gson;
import com.leo.entity.InspurUserEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Spark读取HDFS文件生成HFile并导入到HBase数据库
 * 目前仅支持单列族导入,不支持多列族相同的rowKey导入数据库
 * TODO是需要修改的配置
 */
public class SparkHBaseHFileDemo  {

    private final static Logger log = Logger.getLogger(SparkHBaseHFileDemo.class);
    private static Gson gson = new Gson();
    //TODO HDFS的数据位置
    private final static String HBASE_FILE = "/hbase-testdata/";
    //TODO ColumnFamily
    private final static byte[] COLUMN_FAMILY = Bytes.toBytes("info");
    //TODO Column
    private final static byte[] COLUMN = Bytes.toBytes("age");
    //TODO 需要保存的hbase的表名
    private final static String TABLE_NAME = "leo_test";
    //TODO 配置HDFS的Master
    private final static String HDFS_MASTER = "hdfs://master:9000";

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseHFile");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //配置需要序列化的类
        Class[] classes = new Class[] {ImmutableBytesWritable.class};
        sparkConf.registerKryoClasses(classes);

        //生成的HFile的临时保存路径
        String stagingFolder = HDFS_MASTER+args[0];

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //生成HBase的Configuration
        Configuration conf = HBaseConfiguration.create();
        //设置HFile的Table信息
        conf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
        conf.set(FileOutputFormat.OUTDIR, stagingFolder);
        //配置Zookeeper的相关信息
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        JavaRDD<String> hbaseFile = jsc.textFile(HDFS_MASTER+HBASE_FILE);
        JavaPairRDD<ImmutableBytesWritable, KeyValue> putJavaRDD = hbaseFile.mapToPair(line -> convertToKV(line));
        // for multi-column upload ->  hbaseFile.flatMap(str -> convertToList(str)).mapToPair(line -> convert(line));
        putJavaRDD.sortByKey(true);

        log.info("Start save tmp file into HDFS.");
        FileSystem fs = FileSystem.get(new URI(HDFS_MASTER), conf);
        if (fs.exists(new Path(args[0]))) {
            log.warn("output Dir is exist, will delete and continue.");
            fs.delete(new Path(args[0]), true);
        }

        //将日志保存到指定目录
        putJavaRDD.saveAsNewAPIHadoopFile(stagingFolder, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, conf);
        //此处运行完成之后,在stagingFolder会有我们生成的Hfile文件
        log.info("Finished save tmp file into HDFS.");

        //开始HFile导入到Hbase,此处都是hbase的api操作
        LoadIncrementalHFiles load = new LoadIncrementalHFiles(conf);
        //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            //根据表名获取表
            Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
            //获取hbase表的region分布
            RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf(TABLE_NAME));

            //创建一个hadoop的job
            Job job = Job.getInstance(conf);
            job.setJobName("SparkDumpFile");
            //需要设置文件输出的key,因为我们要生成HFile,所以out-key要用ImmutableBytesWritable
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            //输出文件的内容
            job.setMapOutputValueClass(KeyValue.class);
            //配置HFileOutputFormat2的信息
            HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

            log.info("Complete init setting, Start Upload.");
            Admin admin = conn.getAdmin();
            load.doBulkLoad(new Path(stagingFolder), admin, table, regionLocator);
        }
    }

    @Deprecated
    /**
     * 多列族的生成方法,目前HFileOutputFormat2不支持.
     * 不可用
     */
    private static Tuple2<ImmutableBytesWritable, KeyValue> convert(String[] lineArray) {
        String rowKey = lineArray[0];
        return new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(rowKey)),
                new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes(lineArray[1]), Bytes.toBytes(lineArray[2]), Bytes.toBytes(lineArray[3])));
    }

    @Deprecated
    /**
     * 多列族的生成方法,目前HFileOutputFormat2不支持.
     * 不可用
     */
    private static Iterator<String[]> convertToList(String beanString) {
        InspurUserEntity inspurUserEntity = gson.fromJson(beanString, InspurUserEntity.class);
        String rowKey = inspurUserEntity.getDepartment_level1()+"_"+inspurUserEntity.getDepartment_level2()+"_"+inspurUserEntity.getId();
        List<String[]> values = new ArrayList<>(10);
        values.add(new String[] {rowKey, "name", "", inspurUserEntity.getName()});
        values.add(new String[] {rowKey, "info", "age", inspurUserEntity.getAge()});
        values.add(new String[] {rowKey, "info", "gender", inspurUserEntity.getGender()});
        values.add(new String[] {rowKey, "info", "seniority", inspurUserEntity.getSeniority()});
        values.add(new String[] {rowKey, "department", "level1", inspurUserEntity.getDepartment_level1()});
        values.add(new String[] {rowKey, "department", "level2", inspurUserEntity.getDepartment_level2()});
        values.add(new String[] {rowKey, "department", "level3", inspurUserEntity.getDepartment_level3()});
        values.add(new String[] {rowKey, "department", "leader", inspurUserEntity.getDepartment_leader()});
        log.info("create Row Key for RDD :"+rowKey);
        return values.iterator();
    }

    /**
     * 需要修改顶部COLUMN_FAMILY和COLUMN, 需要修改return的inspurUserEntity.getAge()为对应的属性值
     * @param beanString
     * @return
     */
    private static Tuple2<ImmutableBytesWritable, KeyValue> convertToKV(String beanString) {
        InspurUserEntity inspurUserEntity = gson.fromJson(beanString, InspurUserEntity.class);
        String rowKey = inspurUserEntity.getDepartment_level1()+"_"+inspurUserEntity.getDepartment_level2()+"_"+inspurUserEntity.getId();
//        values.add(new String[] {rowKey, "name", "", inspurUserEntity.getName()});
//        values.add(new String[] {rowKey, "info", "age", inspurUserEntity.getAge()});
//        values.add(new String[] {rowKey, "info", "gender", inspurUserEntity.getGender()});
//        values.add(new String[] {rowKey, "info", "seniority", inspurUserEntity.getSeniority()});
//        values.add(new String[] {rowKey, "department", "level1", inspurUserEntity.getDepartment_level1()});
//        values.add(new String[] {rowKey, "department", "level2", inspurUserEntity.getDepartment_level2()});
//        values.add(new String[] {rowKey, "department", "level3", inspurUserEntity.getDepartment_level3()});
//        values.add(new String[] {rowKey, "department", "leader", inspurUserEntity.getDepartment_leader()});
        log.info("create Row Key for RDD :"+rowKey);

        return new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(rowKey)),
                new KeyValue(Bytes.toBytes(rowKey), COLUMN_FAMILY, COLUMN, Bytes.toBytes(inspurUserEntity.getAge())));
    }

}
