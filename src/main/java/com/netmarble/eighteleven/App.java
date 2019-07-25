package com.netmarble.eighteleven;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class App {
    public static void main(String[] args) {
        Key mykey = new Key();
        // Log_Schema log_schema = new Log_Schema();

        SparkSession spark = SparkSession.builder()
            .master("local")
            .appName("App").getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        System.out.println("HelloWorld!!!!\n" + "Kafka Source : " + mykey.Kafka_source + "\nKafka Topic : " + mykey.Kafka_topic);

// ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// [1] Save kafka streaming data into HDFS

        Dataset<Row> df = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", mykey.Kafka_source)
            .option("subscribe", mykey.Kafka_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss",false)
            .load();

        Dataset<Row> dg = df
            .selectExpr("CAST(value AS STRING)");

        dg.printSchema();


        Dataset<Row> dz = dg
//                .flatMap((FlatMapFunction<Row, Row>) x -> Arrays.asList(RowFactory.create(x.mkString().replaceAll("\\\\",""))).iterator(), encoder)
                .select(
                        from_json(dg.col("value"), DataTypes.createStructType(
                        new StructField[] {
                                DataTypes.createStructField("I_LogId", IntegerType,true)
                        })).getField("I_LogId").alias("I_LogId")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_LogDetailId", IntegerType,true)
                                })).getField("I_LogDetailId").alias("I_LogDetailId")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_GameCode", StringType,true)
                                })).getField("I_GameCode").alias("I_GameCode")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_PID", DataTypes.StringType,true)
                                })).getField("I_PID").alias("I_PID")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_CountryCD", DataTypes.StringType,true)
                                })).getField("I_CountryCD").alias("I_CountryCD")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_LanguageCD", DataTypes.StringType,true)
                                })).getField("I_LanguageCD").alias("I_LanguageCD")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_OS", DataTypes.StringType,true)
                                })).getField("I_OS").alias("I_OS")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_DeviceOSVersion", DataTypes.StringType,true)
                                })).getField("I_DeviceOSVersion").alias("I_DeviceOSVersion")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_DeviceModel", DataTypes.StringType,true)
                                })).getField("I_DeviceModel").alias("I_DeviceModel")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_TimeZone", DataTypes.StringType,true)
                                })).getField("I_TimeZone").alias("I_TimeZone")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_SDKVersion", DataTypes.StringType,true)
                                })).getField("I_SDKVersion").alias("I_SDKVersion")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_ChannelType", DataTypes.StringType,true)
                                })).getField("I_ChannelType").alias("I_ChannelType")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_ConnectIP", DataTypes.StringType,true)
                                })).getField("I_ConnectIP").alias("I_ConnectIP")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_TID", DataTypes.StringType,true)
                                })).getField("I_TID").alias("I_TID")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_GameVersion", DataTypes.StringType,true)
                                })).getField("I_GameVersion").alias("I_GameVersion")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_Now", DataTypes.StringType,true)
                        })).getField("I_Now").alias("I_Now")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_NMTimestamp", DataTypes.LongType,true)
                                })).getField("I_NMTimestamp").alias("I_NMTimestamp")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_Region", DataTypes.StringType,true)
                                })).getField("I_Region").alias("I_Region")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_UDID", DataTypes.StringType,true)
                                })).getField("I_UDID").alias("I_UDID")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_PlatformADID", DataTypes.StringType,true)
                                })).getField("I_PlatformADID").alias("I_PlatformADID")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_JoinedCountryCode", DataTypes.StringType,true)
                                })).getField("I_JoinedCountryCode").alias("I_JoinedCountryCode")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_World", DataTypes.StringType,true)
                                })).getField("I_World").alias("I_World")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_OldNMDeviceKey", DataTypes.StringType,true)
                                })).getField("I_OldNMDeviceKey").alias("I_OldNMDeviceKey")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_LogKey", DataTypes.StringType,true)
                                })).getField("I_LogKey").alias("I_LogKey")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_RetryCount", DataTypes.IntegerType,true)
                                })).getField("I_RetryCount").alias("I_RetryCount")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_RetryReason", DataTypes.StringType,true)
                                })).getField("I_RetryReason").alias("I_RetryReason")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_NMMarket", DataTypes.StringType,true)
                                })).getField("I_NMMarket").alias("I_NMMarket")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_NMCharacterID", DataTypes.StringType,true)
                                })).getField("I_NMCharacterID").alias("I_NMCharacterID")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_City", DataTypes.StringType,true)
                                })).getField("I_City").alias("I_City")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_LogDes", DataTypes.StringType,true)
                                })).getField("I_LogDes").alias("I_LogDes")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("IP", DataTypes.StringType,true)
                                })).getField("IP").alias("IP")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_IPHeader", DataTypes.StringType,true)
                                })).getField("I_IPHeader").alias("I_IPHeader")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_RegDateTime", DataTypes.LongType,true)
                                })).getField("I_RegDateTime").alias("I_RegDateTime")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_ConnectorVersion", DataTypes.StringType,true)
                                })).getField("I_ConnectorVersion").alias("I_ConnectorVersion")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_RequestTime", DataTypes.StringType,true)
                                })).getField("I_RequestTime").alias("I_RequestTime")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_NMRequestTime", DataTypes.LongType,true)
                                })).getField("I_NMRequestTime").alias("I_NMRequestTime")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_SessionID", DataTypes.StringType,true)
                                })).getField("I_SessionID").alias("I_SessionID")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_IDFV", DataTypes.StringType,true)
                                })).getField("I_IDFV").alias("I_IDFV")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_NMEventTime", DataTypes.LongType,true)
                                })).getField("I_NMEventTime").alias("I_NMEventTime")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_NMMobileIP", DataTypes.StringType,true)
                                })).getField("I_NMMobileIP").alias("I_NMMobileIP")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_NMManufacturer", DataTypes.StringType,true)
                                })).getField("I_NMManufacturer").alias("I_NMManufacturer")
                        ,from_json(dg.col("value"), DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField("I_NMModel", DataTypes.StringType,true)
                                })).getField("I_NMModel").alias("I_NMModel"));

        dz.printSchema();

        StreamingQuery queryone = dz
                .writeStream()
//                .format("console")
                .format("json")
                .outputMode("append")
                .option("checkpointLocation",mykey.Hadoop_path)
                .option("path",mykey.Hadoop_path)
//                .partitionBy("year","month","day","hour")
                .start();

        try {
                queryone.awaitTermination();
        } catch (StreamingQueryException e) {
                e.printStackTrace();
        }


// ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/*
// ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// [2] Load Hadoop file and query
        Dataset<Row> log = spark.read().json(mykey.Hadoop_file);
        log.select("I_LogId").show(10,false);
        log.groupBy(log.col("I_LogId"), log.col("I_LogDetailId")).count().show();
        log.select(log.col("I_LogDes")).show(false);
        log.show(1);

// ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
*/
    }
}

