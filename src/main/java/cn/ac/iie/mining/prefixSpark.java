package cn.ac.iie.mining;

import java.util.*;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.fpm.PrefixSpan;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.Test;

public class prefixSpark {
    public static void main(String[] args) {
        long stime = System.currentTimeMillis();
        SparkSession spark = SparkSession
                .builder()
                .appName("PrefixTest")
                .master("local[4]")
                //.master("spark://localhost:7077")
                //.master("spark://hezhengjiedeMacBook-Pro.local:7077")
                .getOrCreate();

        // $example on$
        List<Row> data = Arrays.asList(
                RowFactory.create(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3))),
                RowFactory.create(Arrays.asList(Arrays.asList(1), Arrays.asList(3, 2), Arrays.asList(1,2))),
                RowFactory.create(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(5))),
                RowFactory.create(Arrays.asList(Arrays.asList(6)))
        );
        miningTask(spark, data);

        List<Row> data1 = Arrays.asList(
                RowFactory.create(Arrays.asList(Arrays.asList(1), Arrays.asList(2), Arrays.asList(2))),
                RowFactory.create(Arrays.asList(Arrays.asList(1), Arrays.asList(2), Arrays.asList(2), Arrays.asList(1), Arrays.asList(2))),
                RowFactory.create(Arrays.asList(Arrays.asList(1), Arrays.asList(2), Arrays.asList(5))),
                RowFactory.create(Arrays.asList(Arrays.asList(6)))
        );
        miningTask(spark, data1);

        spark.stop();
        long etime = System.currentTimeMillis();
        System.out.printf("执行时长：%d 秒.", (etime - stime) / 1000);
    }

    private static void miningTask(SparkSession spark, List<Row> data) {
        StructType schema = new StructType(new StructField[]{ new StructField(
                "sequence", new ArrayType(new ArrayType(DataTypes.IntegerType, true), true),
                false, Metadata.empty())
        });
        Dataset<Row> sequenceDF = spark.createDataFrame(data, schema);

        PrefixSpan prefixSpan = new PrefixSpan().setMinSupport(0.5).setMaxPatternLength(5);

        // Finding frequent sequential patterns
        Dataset<Row> sequence = prefixSpan.findFrequentSequentialPatterns(sequenceDF);
        sequence.show();
        // $example off$
        //List<String> listOne = sequence.as(Encoders.STRING()).collectAsList();
        //System.out.println(listOne);
        //
        //List<String> listTwo = sequence.map((MapFunction<Row, String>) Row::mkString, Encoders.STRING()).collectAsList();
        //System.out.println(listTwo);
    }

    @Test
    public void sparkGrammar() {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSample")
                .master("local[*]")
                .getOrCreate();
        SparkContext sc = spark.sparkContext();
        sc.setLogLevel("ERROR");
        //create df
        List<String> myList = Arrays.asList("one", "two", "three", "four", "five");
        Dataset<Row> df = spark.createDataset(myList, Encoders.STRING()).toDF();
        df.show();
        //using df.as
        List<String> listOne = df.as(Encoders.STRING()).collectAsList();
        System.out.println(listOne);
        //using df.map
        List<String> listTwo = df.map((MapFunction<Row, String>) row -> row.mkString(), Encoders.STRING()).collectAsList();
        System.out.println(listTwo);

        List<String> oneItemList = new ArrayList<>();
        for(Iterator<Row> iter = df.toLocalIterator(); iter.hasNext();) {
            //String item = (iter.next()).toString();
            //System.out.println(item.toString());
            Row each = iter.next();
            if (each.size() == 1)
                oneItemList.add(each.getString(0));
        }
        System.out.println(oneItemList);
        System.exit(1);

        List<String> oneItem = new ArrayList<>();
        df.foreach(row -> {
            if (row.size() == 1) {
                oneItem.add(row.getString(0));
                System.out.println(1);
                System.out.println(row.getString(0));
                System.out.println(oneItem.toString());
            }
            else {
                System.out.println(row.size());
                System.out.println(row.getString(0));
                System.out.println(2);
            }
        });
        System.out.println("oneItem: " + oneItem.toString());
    }

    @Test
    public void testGrammer() {
        HashMap<String ,Integer> hashMap = new HashMap<>();
        hashMap.put("a", 11);
        hashMap.put("b", 10);
        hashMap.put("c", 12);
        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");
        list.sort((o1, o2) -> hashMap.get(o2) - hashMap.get(o1));
        System.out.println(list.toString());
    }
}
