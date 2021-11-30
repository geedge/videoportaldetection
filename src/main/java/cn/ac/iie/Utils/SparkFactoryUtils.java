package cn.ac.iie.Utils;

import cn.ac.iie.config.Configurations;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkFactoryUtils {
    static SparkSession spark;
    static {
        SparkConf conf = new SparkConf()
                .setAppName("PrefixSpanHosts"+System.currentTimeMillis())
                .setMaster(Configurations.getStringProperty(0, "Spark.master"))
                .set("spark.executor.extraJavaOptions", "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./dumpex")
                .set("spark.driver.extraJavaOptions", "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./dumpdr")
                .set("spark.driver.memory", "10g")
                .set("spark.executor.memory", "10g")
        ;
        spark = SparkSession
                .builder()
                //.master("local[*]")
                .config(conf)
                .getOrCreate();
        SparkContext sc = spark.sparkContext();
        sc.setLogLevel("WARN");
    }

    public static SparkSession getSparkSessionFactory() {
        return spark;
    }
}
