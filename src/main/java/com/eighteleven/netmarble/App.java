package com.eighteleven.netmarble;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

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

        StreamingQuery queryone = dg.writeStream()
//                .format("console")
                .format("json")
                .outputMode("append")
                .option("path","./jsondir")
                .option("checkpointLocation","./jsoncheckdir")
                .start();

        try {
            queryone.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }


        String p = "<p> FINISH </p>";
        System.out.println(StringEscapeUtils.escapeHtml4(p));

    }
}
