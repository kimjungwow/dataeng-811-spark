package com.netmarble.eighteleven;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

public class KafkaToHadoop {

    static String[] LOG_INT = {"I_LogId", "I_LogDetailId", "I_RetryCount"}, LOG_STRING = {"I_GameCode", "I_PID", "I_CountryCD", "I_LanguageCD", "I_OS", "I_DeviceOSVersion", "I_DeviceModel", "I_TimeZone", "I_SDKVersion", "I_ChannelType", "I_ConnectIP", "I_TID", "I_GameVersion", "I_Now", "I_Region", "I_UDID", "I_PlatformADID", "I_JoinedCountryCode", "I_World", "I_OldNMDeviceKey", "I_LogKey", "I_RetryReason", "I_NMMarket", "I_NMCharacterID", "I_City", "I_LogDes", "IP", "I_IPHeader", "I_ConnectorVersion", "I_RequestTime", "I_SessionID", "I_IDFV", "I_NMMobileIP", "I_NMManufacturer", "I_NMModel"}, LOG_LONG = {"I_NMTimestamp", "I_RegDateTime", "I_NMRequestTime", "I_NMEventTime"}, LOGDES_INT = {"GetCash", "GetGameMoney", "GetArousalHeroMaterial", "ArousalMaterialHeroTID", "GetArousalMaterialHero", "GetArousalMaterialAll", "ProvideObjectValue", "RewardType", "RewardSubType", "RewardValue", "P_RewardType"}, LOGDES_INT_ITER = {"GetArousalTemplateID", "GetArousalCount", "MultiRewardType", "MultiRewardValue", "MultiRewardSubType"}, LOGDES_STRING = {"Result", "PlayMode_stage", "PlayMode_cash", "PlayMode", "NOW", "type", "ProvideObjectType"};
    static String HADOOP_PATH = "Hadoop_path", CAST_AS_STRING = "CAST(value AS STRING)", YEAR = "year", MONTH = "month", DAY = "day", HOUR = "hour", I_LOGDES = "I_LogDes", PROPERTIES = "/home/marbleint19_admin/config.properties", KAFKA_SOOURCE = "Kafka_source", KAFKA_TOPIC = "Kafka_topic", I_REGDATETIME = "I_RegDateTime", VALUE = "value", KAFKA = "kafka", KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers", SUBSCRIBE = "subscribe", STARTINGOFFSETS = "startingoffsets", LATEST = "latest", EARLIEST = "earliest", MAXOFFSETSPERTRIGGER = "maxOffsetsPerTrigger", FAILONDATALOSS = "failOnDataLoss", PARQUET = "parquet", PARQUET_BLOCK_SIZE = "parquet.block.size", CHECKPOINTLOCATION = "checkpointLocation", PATH = "path", COMPRESSION = "compression", GZIP = "gzip", LOCAL = "local", ERROR = "error";
    static long PARQUET_BLOCKSIZE = 256 * 1024 * 1024, KAFKA_MAXOFFSETPERTRIGGER = 100000000;
    static double THOUSAND = 1000;

    public static void main(String[] args) {
        // 서버에 저장된 .properties 파일로부터 읽어옴
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(PROPERTIES));
        } catch (IOException e) {
            System.out.println("File IO Error " + e);
        }

        SparkSession spark = SparkSession.builder()
                .master(LOCAL)
                .appName("KafkaToHadoop").getOrCreate();
        spark.sparkContext().setLogLevel(ERROR);
        System.out.println("\n\n*****************************\n*** Kafka -> Hadoop START ***\n" + "Kafka Source : " + prop.getProperty(KAFKA_SOOURCE) + "\nKafka Topic : " + prop.getProperty(KAFKA_TOPIC) + "\n*****************************\n");

        // Kafka로부터 읽어옴
        Dataset<Row> df = spark
                .readStream()
                .format(KAFKA)
                .option(KAFKA_BOOTSTRAP_SERVERS, prop.getProperty(KAFKA_SOOURCE))
                .option(SUBSCRIBE, prop.getProperty(KAFKA_TOPIC))
                .option(STARTINGOFFSETS, LATEST)
                .option(MAXOFFSETSPERTRIGGER, KAFKA_MAXOFFSETPERTRIGGER)
                .option(FAILONDATALOSS, false)
                .load();

        Dataset<Row> dg = df.selectExpr(CAST_AS_STRING);
        Dataset<Row> dz = dg.select(get_columnlist_from_json(dg)); // Kafka의 [value] column에서 필요한 항목들을 꺼냄
        dz = do_withcolumns_LogDes(dz); // I_LogDes 내부의 항목을 새로운 col로 빼냄
        dz = do_withcolumns_TimePartition(dz); // Partition에 필요한 col 추가

        StreamingQuery queryone = dz
                .writeStream()
                .format(PARQUET)
                .option(PARQUET_BLOCK_SIZE, PARQUET_BLOCKSIZE)
                .option(CHECKPOINTLOCATION, prop.getProperty(HADOOP_PATH))
                .option(PATH, prop.getProperty(HADOOP_PATH))
                .partitionBy(YEAR, MONTH, DAY, HOUR) // 날짜, 시간별로 Partition
                .option(COMPRESSION, GZIP) // gzip 형식으로 압축
                .start();
        try {
            queryone.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }

    public static Column get_field_from_json(Dataset<Row> data, DataType field_type, String get) {
        return from_json(data.col(I_LOGDES), DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(get, field_type, true)
        })).getField(get);
    }

    public static Column get_column_from_json(Dataset<Row> data, DataType column_type, String column_name) {
        return from_json(data.col(VALUE), DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(column_name, column_type, true)
        })).getField(column_name).alias(column_name);
    }

    public static Column[] get_columnlist_from_json(Dataset<Row> data) {
        Column[] cols = new Column[LOG_STRING.length + LOG_INT.length + LOG_LONG.length];
        for (int i = 0; i < LOG_STRING.length; i++) {
            cols[i] = get_column_from_json(data, StringType, LOG_STRING[i]);
        }
        for (int i = 0; i < LOG_INT.length; i++) {
            cols[i + LOG_STRING.length] = get_column_from_json(data, IntegerType, LOG_INT[i]);
        }
        for (int i = 0; i < LOG_LONG.length; i++) {
            cols[i + LOG_INT.length + LOG_STRING.length] = get_column_from_json(data, LongType, LOG_LONG[i]);
        }
        return cols;
    }

    public static Dataset<Row> do_withcolumns_TimePartition(Dataset<Row> data) {
        return data.withColumn(DAY, col(I_REGDATETIME).cast(DoubleType).divide(THOUSAND).cast(IntegerType).cast(TimestampType))
                .withColumn(HOUR, hour(col(DAY)))
                .withColumn(YEAR, year(col(DAY)))
                .withColumn(MONTH, month(col(DAY)))
                .withColumn(DAY, dayofmonth(col(DAY)));
    }

    public static Dataset<Row> do_withcolumns_LogDes(Dataset<Row> data) {
        for (int i = 0; i < LOGDES_INT.length; i++) {
            data = data.withColumn(LOGDES_INT[i], get_field_from_json(data, IntegerType, LOGDES_INT[i]));
        }
        for (int i = 0; i < LOGDES_INT_ITER.length; i++) {
            for (int j = 0; j < 5; j++) {
                data = data.withColumn(LOGDES_INT_ITER[i] + j, get_field_from_json(data, IntegerType, LOGDES_INT[i] + j));
            }
        }
        for (int i = 0; i < LOGDES_STRING.length; i++) {
            data = data.withColumn(LOGDES_STRING[i], get_field_from_json(data, StringType, LOGDES_STRING[i]));
        }
        return data;
    }
}

