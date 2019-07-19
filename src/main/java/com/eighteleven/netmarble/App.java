package com.eighteleven.netmarble;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 *
 * Hello world!
 *
 */
public class App
{
    public static void main( String[] args ) {
        Key mykey = new Key();

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("App").getOrCreate();
        System.out.println("HelloWorld!!!!\n" + "Kafka Source : " + mykey.Kafka_source  + "\nKafka Topic : " + mykey.Kafka_topic);


        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", mykey.Kafka_source)
                .option("subscribe", mykey.Kafka_topic)
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> dg = df.selectExpr("CAST(value AS STRING)");

        Dataset<String> ds = dg
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(":")).iterator(), Encoders.STRING());

        StreamingQuery queryone = ds.writeStream()
//                .format("console")
                // .format("console")
                .format("json")
                .outputMode("append")
                .option("checkpointLocation",mykey.Hadoop_path)
                .option("path",mykey.Hadoop_path)

                .start();

        try {
            queryone.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

//        System.out.println("1!!!");
//
//        Dataset<Row> logs = spark
//                .read()
//                .json(mykey.json_file);
//
//        System.out.println("2!!!");
//
//        logs.printSchema();
//        logs.groupBy("I_LogId").count().show(); // string 형태로 저장되어 I_LogID column이 없다는 error 발생 -> 저장 형식을 DB 형태로 바꾸어야 함
////        logs.groupBy("I_LogId", "I_LogDetailId").count().show();
//        System.out.println("3!!!");

    }
}
