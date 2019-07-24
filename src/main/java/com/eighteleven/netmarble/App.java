package com.eighteleven.netmarble;

import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.types.DataTypes.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.Encoders.*;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;

public class App implements Serializable {
    public static void main( String[] args ) {
        Key mykey = new Key();
        Log_Schema log_schema = new Log_Schema();

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("App").getOrCreate();
        System.out.println("HelloWorld!!!!\n" + "Kafka Source : " + mykey.Kafka_source  + "\nKafka Topic : " + mykey.Kafka_topic);

        spark.sparkContext().setLogLevel("ERROR");
//
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", mykey.Kafka_source)
                .option("subscribe", mykey.Kafka_topic)
                .option("sep", ",")
//                .schema(schema)
//                .option("startingOffsets", "earliest")
                .load();
//
        Dataset<Row> dg = df
                .selectExpr("CAST(value AS STRING)")
//                .flatMap((FlatMapFunction<Row, Row>) x -> Arrays.asList(RowFactory.create(x.mkString().replaceAll("\\\\", ""))).iterator(), encoder)
//                .select(from_json(df.col("value"), log_schema.log_schema));

        .select(
                from_json(df.col("value")
                        , DataTypes.createStructType(
                                new StructField[] {
                                        DataTypes.createStructField()
                                        }))
                        )
                , from_json
                )
        )

        dg.printSchema();

//                .
//                .as(STRING());
//                .flatMap((FlatMapFunction<String, String>) x ->
//                        Arrays.asList(x.substring(1, x.length() - 1).split(",")).iterator()
//                        , Encoders.
//                                STRING());
//        dg.printSchema();
//        dg.foreachPartition(new MyFuction<Iterator<String>,BoxedUnit>()
//
//        )

//        Dataset<Row> ds = dg.
//                foreach((ForeachFunction<String>))
//        dg.foreach((ForeachFunction<String>) log -> log.getAge)



//        Dataset<String> dq = dg
//                .flatMap((FlatMapFunction<String, String>) x ->
//                        Arrays.asList(x.substring(1, x.length()).split(",")).iterator()
//                        , Encoders.STRING());

//        Dataset<Row> ds = parsing(dg, spark);

//        String a = "{\"I_CountryCD\":\"TH\",\"I_PID\":\"2031CA4C721849488B54698E60BC5978\",\"I_TID\":\"0A7D3C6251DE4DAB9881C0A32A4F4CF6\",\"I_GameVersion\":\"5.5.00\",\"I_UDID\":\"f514b8131adbe48d\",\"I_PlatformADID\":\"6c0b1ff4-e4d7-4fa8-a8be-8908ce89ae39\",\"I_City\":\"10\",\"I_World\":\"\",\"I_Region\":\"ASIA\",\"I_JoinedCountryCode\":\"TH\",\"I_OS\":\"1\",\"I_DeviceOSVersion\":\"9\",\"I_DeviceModel\":\"Vivo 1902\",\"I_TimeZone\":\"+07:00\",\"I_LanguageCD\":\"th_TH\",\"I_SDKVersion\":\"4.4.4.5\",\"I_ChannelType\":\"1\",\"I_ConnectIP\":\"223.24.147.42\",\"I_IPHeader\":\"X-Forwarded-For\",\"IP\":\"223.24.185.174\",\"I_GameCode\":\"sknightsgb\",\"I_NMMarket\":\"google\",\"I_Now\":\"2019-07-23 12:22:28:946\",\"I_NMTimestamp\":1563859348946,\"I_LogKey\":\"5079885C262D4366BA9107F66AAF39B7\",\"I_LogId\":9,\"I_LogDetailId\":106,\"I_PCSeq\":\"117548350\",\"I_LogDes\":{\"PresentRewardType\":0,\"MyArousalMaterialHero\":0,\"ArousalMaterialHeroTID\":0,\"PetUniqueID\":0,\"MySenaPoint\":0,\"type\":\"10\",\"MyLightCrystal\":20,\"MyMissionDungeonPoint\":75735,\"MySPShopPoint\":0,\"SPShopPointType\":0,\"MyManaCount\":0,\"MyStamina\":4629,\"SelectBoxTID\":0,\"GetArousalMaterialHero\":0,\"MyCash\":5,\"GetArousalMaterialAll\":0,\"Score\":0,\"GetTopaz\":0,\"Now\":\"2019-07-23 05:22:28:000\",\"MyLowMana\":7161,\"HeroUniqueID\":0,\"GetHeroCostumeID\":0,\"Region\":\"ASIA\",\"Level\":31,\"MyTicketCount\":0,\"GetCash\":0,\"MyViralPoint\":26,\"MyMateria5\":0,\"MyGamemoney\":1279306,\"MyMateria2\":852987,\"MyMateria1\":427495,\"CountryName\":\"TH\",\"MyMateria4\":29,\"MaterialTemplateID1\":0,\"MyMateria3\":16144,\"GetPetTemplateID\":0,\"GetViralPoint\":0,\"ItemUniqueID\":0,\"GetGamemoney\":0,\"MyFame\":637,\"GetMateria5\":0,\"GetMateria4\":0,\"GetMateria3\":0,\"GetTicketCount\":0,\"GetMateria2\":0,\"GetMateria1\":0,\"GetTicketType\":-1,\"MyGWTokenPoint\":94,\"ADDGWTokenPoint\":0,\"GetStamina\":20,\"GetFame\":0,\"GetManaType\":0,\"GetSPShopPoint\":1060551333,\"MissionDungeonRewardValue\":0,\"MyCrystal\":69492,\"MyArousalMaterialAll\":0,\"MyShadowOre\":0,\"GetItemTemplateID\":0,\"AutoCount\":106420,\"MyTopaz\":101,\"RandomBoxTID\":0,\"GetManaCount\":0,\"MyMiddleMana\":0,\"GetHeroTemplateID\":0},\"I_RequestTime\":\"2019-07-23 12:22:28:947\",\"I_RegDateTime\":1563859350147}";
//        System.out.println(string_to_valuelist(a));
//
//        String b = "{\"I_CountryCD\":\"TH\",\"I_PID\":\"C856A5FCB92348148D43A1986AB9AB85\",\"I_TID\":\"C51824A8382B47F78DDB5B453B79D7CB\",\"I_GameVersion\":\"5.5.00\",\"I_UDID\":\"73153e076c057454\",\"I_PlatformADID\":\"77932d76-6f2d-440f-8872-bb39499ac070\",\"I_City\":\"10\",\"I_World\":\"\",\"I_Region\":\"ASIA\",\"I_JoinedCountryCode\":\"\",\"I_OS\":\"1\",\"I_DeviceOSVersion\":\"5.1\",\"I_DeviceModel\":\"Asus ASUS_Z00VD\",\"I_TimeZone\":\"+07:00\",\"I_LanguageCD\":\"th_TH\",\"I_SDKVersion\":\"4.4.4.5\",\"I_ChannelType\":\"5\",\"I_ConnectIP\":\"223.24.168.11\",\"I_IPHeader\":\"X-Forwarded-For\",\"IP\":\"223.24.62.177\",\"I_GameCode\":\"sknightsgb\",\"I_NMMarket\":\"google\",\"I_Now\":\"2019-07-23 12:22:29:003\",\"I_NMTimestamp\":1563859349003,\"I_LogKey\":\"B4606347DE344DA186C57E69F67850F6\",\"I_LogId\":9,\"I_LogDetailId\":100,\"I_PCSeq\":\"112408174\",\"I_LogDes\":{\"Level\":91,\"RecvUserID\":\"A22101F0B3AC4EFEBD19632BDC0BB3BF\",\"CountryName\":\"TH\",\"AddedPoint\":1,\"Now\":\"2019-07-23 05:22:28:000\",\"Region\":\"ASIA\"},\"I_RequestTime\":\"2019-07-23 12:22:29:004\",\"I_RegDateTime\":1563859350362}";
//        System.out.println(string_to_valuelist(b));
//
//        String c = "{\"I_CountryCD\":\"TH\",\"I_PID\":\"E88145CAE9B64E2FA69F87162EC1608E\",\"I_TID\":\"3EC106BA4FAF4A108F4E0322D8F9E6DA\",\"I_GameVersion\":\"5.5.00\",\"I_UDID\":\"9fd0b608921bbb55\",\"I_PlatformADID\":\"7909c275-af0f-4ba6-825e-6ec89bc017b8\",\"I_City\":\"50\",\"I_World\":\"\",\"I_Region\":\"ASIA\",\"I_JoinedCountryCode\":\"\",\"I_OS\":\"1\",\"I_DeviceOSVersion\":\"7.1.1\",\"I_DeviceModel\":\"OnePlus ONEPLUS A5000\",\"I_TimeZone\":\"+07:00\",\"I_LanguageCD\":\"th_TH\",\"I_SDKVersion\":\"4.4.4.5\",\"I_ChannelType\":\"1,5\",\"I_ConnectIP\":\"171.4.248.85\",\"I_IPHeader\":\"X-Forwarded-For\",\"IP\":\"171.4.233.22\",\"I_GameCode\":\"sknightsgb\",\"I_NMMarket\":\"google\",\"I_Now\":\"2019-07-23 12:22:30:030\",\"I_NMTimestamp\":1563859350030,\"I_LogKey\":\"7F3B5211D6AB4C20ABC5CFC3F9D3A354\",\"I_LogId\":9,\"I_LogDetailId\":101,\"I_PCSeq\":\"112948198\",\"I_LogDes\":{\"HeroTemplateID\":3601,\"Level\":75,\"Duplication\":1,\"CountryName\":\"TH\",\"Now\":\"2019-07-23 05:22:30:000\",\"Region\":\"ASIA\"},\"I_RequestTime\":\"2019-07-23 12:22:30:030\",\"I_RegDateTime\":1563859350498}";
//        System.out.println(string_to_valuelist(c));


//        List<String> data = Arrays.asList("Hello", "World");
//        Dataset<String> dom = spark.createDataset(data, Encoders.STRING());
//        System.out.println("Wow12");
//        System.out.println(dom);
//        dom.show();
//        System.out.println("Wow13");
//        Seq(Row(1, ""))

//        List<String> data = Arrays.asList("abc", "bcd", "def");
//        Dataset<String> ds = createDataset(data, )
//        row_to_log_schema(df);
//        log.log_schema.add("a", DataTypes.)
//                Dataset<Row> dg = df
//                .selectExpr("CAST(value AS STRING)");
//                .selectExpr("I_LogId", "I_LogDetailId");
//        Encoders.STRING();

//        List<List<Object>> valuelist = new ArrayList<>();
//
//                dg
//                .flatMap((FlatMapFunction<String, Void>) x -> valuelist.add(string_to_valuelist(x)), Encoders.STRING());

/*
        List<String[]> stringAsList = new ArrayList<>();
        stringAsList.add(new String[] { "bar1.1", "bar2.1" });
        stringAsList.add(new String[] { "bar1.2", "bar2.2" });

        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<Row> rowRDD = sparkContext.parallelize(valuelist).map((Object[] row) -> RowFactory.create(row));

        // Creates schema
        StructType schema = DataTypes
                .createStructType(new StructField[] { DataTypes.createStructField("foe1", DataTypes.StringType, false),
                        DataTypes.createStructField("foe2", DataTypes.StringType, false) });

        Dataset<Row> dq = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();
*/

/*
        // Core part

        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<Row> rowRDD = sparkContext.parallelize(valuelist).map((List<Object> row) -> RowFactory.create(row));
        Dataset<Row> dq = spark.sqlContext().createDataFrame(rowRDD, log.log_schema).toDF();
        dq.show();

*/

        StreamingQuery queryone = dg.writeStream()
                .format("console")
                .start();
//                .format("json")
//                .outputMode("append")
//                .option("checkpointLocation",mykey.Hadoop_path)
//                .option("path",mykey.Hadoop_path)
//                .start();

//        df.printSchema();
//        dg.printSchema();
//        dq.printSchema();
//        log.log_schema.printTreeString();


//        log_schema.printTreeString();

//        Encoder<Log_Schema> logEncoder = bean(Log_Schema.class);
//        Dataset<Log_Schema> javaBeanDS = spark.createDataset(
//                Collections.singletonList(log_schema),
//                logEncoder
//        );
//        javaBeanDS.printSchema();
//        javaBeanDS.show();

//        System.out.println("Log_Schema");
//        Log_Schema person = new Log_Schema();
//        Encoder<Log_Schema> personEncoder = Encoders.bean(Log_Schema.class);
//        Dataset<Log_Schema> javaBeanDS = spark.createDataset(
//                Collections.singletonList(person),
//                personEncoder
//        );
//        javaBeanDS.show();
//        javaBeanDS.printSchema();

//        try {
//            queryone.awaitTermination();
//        } catch (StreamingQueryException e) {
//            e.printStackTrace();
//        }

//        System.out.println("1!!!");
//
        Dataset<Row> logs = spark
                .read()
                .json(mykey.Hadoop_file);

        logs.printSchema();
        Dataset<Row> logs_2 = parsing(logs, spark);
        logs_2.printSchema();
//
//        System.out.println("2!!!");
//
//        logs.printSchema();
//        logs.groupBy("I_LogId").count().show(); // string 형태로 저장되어 I_LogID column이 없다는 error 발생 -> 저장 형식을 DB 형태로 바꾸어야 함
////        logs.groupBy("I_LogId", "I_LogDetailId").count().show();
//        System.out.println("3!!!");

    }

//    public static void row_to_log_schema(Dataset<Row> df) { // Kafka에서 load된 Dataset<Row> type의 value를 log_schema에 맞게 가공하는 함수
//
//        String[] key_values = df
//                .selectExpr("CAST(value AS STRING)")
//                .as(STRING());
////                .substring(1, )
////                .split(",");
//
//        System.out.println(key_values);
////        Arrays.asList(df,substring(1,))
////        .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.substring(1, x.length()).split(",")).iterator(), STRING());
//    }

    public static List<Object> string_to_valuelist(String log) {
        List<String> keys = Arrays.asList("I_LogId", "I_LogDetailId", "I_GameCode", "I_PID", "I_CountryCD", "I_LanguageCD", "I_OS", "I_DeviceOSVersion", "I_DeviceModel", "I_TimeZone",
                "I_SDKVersion", "I_ChannelType", "I_ConnectIP", "I_TID", "I_GameVersion", "I_Now", "I_NMTimestamp", "I_Region", "I_UDID", "I_PlatformADID",
                "I_JoinedCountryCode", "I_World", "I_OldNMDeviceKey", "I_LogKey", "I_RetryCount", "I_RetryReason", "I_NMMarket", "I_NMCharacterID", "I_City", "I_LogDes",
                "IP", "I_IPHeader", "I_RegDateTime", "I_ConnectorVersion", "I_RequestTime", "I_NMRequestTime", "I_SessionID", "I_IDFV", "I_NMEventTime", "I_NMMobileIP",
                "I_NMManufacturer", "I_NMModel");
        List<String> integer_keys = Arrays.asList("I_LogId", "I_LogDetailId", "I_RetryCount");
        List<String> long_keys = Arrays.asList("I_NMTimestamp", "I_RegDateTime", "I_NMRequestTime", "I_NMEventTime");

        Map<String, String> hashmap = new HashMap<>();
        List<Object> values = new ArrayList<Object>();

        System.out.println(log);
        String[] key_values = log.replaceAll("\"value:\"", "").substring(4, log.length() - 4).split(",\"");
        for (int i = 0; i < key_values.length; i++) {
            if (key_values[i].contains("I_LogDes")) {
                String LogDes = new String();
                while(! key_values[i - 1].contains("}"))
                {
                    System.out.printf("%d %s\n", i, key_values[i]);
                    LogDes = LogDes + key_values[i] + ",";
                    i++;
                }
                LogDes = LogDes.substring(0, LogDes.length() - 1);
                String key = LogDes.split(":\\{")[0].trim().replaceAll("\"", "");
                String value = LogDes.split(":\\{")[1].trim().replaceAll("}", "");
                hashmap.put(key, value);
            }

            System.out.println(key_values[i]);
            String key = key_values[i].split(":", 2)[0].trim().replaceAll("\"", "");
            String value = key_values[i].split(":", 2)[1].trim().replaceAll("\"", "");
            hashmap.put(key, value);
        }

        for (String key : keys) {
            String value = hashmap.get(key);
            if (value == null) values.add(null);
            else if (integer_keys.contains(value)) values.add(Integer.parseInt(value));
            else if (long_keys.contains(value)) values.add(Long.parseLong(value));
            else values.add(value);
        }
        System.out.println("This is hashmap");
        System.out.println(hashmap);
        System.out.println("This is value list");
        System.out.println(values);
        return values;
    }

    public static Dataset<Row> parsing(Dataset<Row> dg, SparkSession spark) {
        List<Object[]> stringAsList = new ArrayList<>();
        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        System.out.println(dg);
        List<String> ds =  dg.select("value").as(STRING()).collectAsList();
        System.out.println(ds);

        for(String log : ds) {
            Object[] tmp_valuelist = string_to_valuelist(log).toArray();
            stringAsList.add(tmp_valuelist);
        }
        JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map(x -> RowFactory.create(x));
        Log_Schema log_schema = new Log_Schema();
        return dg.sparkSession().createDataFrame(rowRDD, log_schema.log_schema).toDF();
    }
}
