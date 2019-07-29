package com.netmarble.eighteleven;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

// 순서를 정리하고 불필요한 library를 삭제해야 함

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;


public class App {
    public static void main(String[] args) throws URISyntaxException, IOException {
        Key mykey = new Key();

        SparkSession spark = SparkSession.builder()
            .master("local")
            .appName("App").getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
// ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


// [2] Load and query the logs

        int DAU = 0, CCU = 0;

        // 오늘 날짜를 계산하여 접근할 디렉토리를 찾음
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String now_str = sdf.format(new Date(timestamp.getTime()));
        Long now_long = timestamp.getTime(); // 하나의 시간만 존재해야 함

        System.out.println(now_str); //for checking execution time

        // DAU 등은 오늘 특정 시간의 디렉토리만을 필요로 하지 않으므로 timestamp -> String 함수를 만들어야 함
        mykey.Hadoop_path += "/year=" + now_str.substring(0, 4);
        mykey.Hadoop_path += "/month=" + (now_str.charAt(4) == '0' ? now_str.substring(5, 6) : now_str.substring(4, 6));
        mykey.Hadoop_path += "/day=" + (now_str.charAt(6) == '0' ? now_str.substring(7, 8) : now_str.substring(6, 8));
        mykey.Hadoop_path += "/hour=" + (now_str.charAt(8) == '0' ? now_str.substring(9, 10) : now_str.substring(8, 10));
        System.out.println(mykey.Hadoop_path);

        mykey.Hadoop_path = "hdfs://localhost:9000/eighteleven/sknightsgb/year=2019/month=7/day=25/hour=15"; // for temporary
        mykey.Hadoop_path = "hdfs://localhost:9000/eighteleven/sknightsgb/year=2019/month=7/day=25"; // for temporary

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(mykey.Hadoop_path), conf);
        List<String> AllFilePath = getAllFilePath(new Path(mykey.Hadoop_path), fs);

        for(String file : AllFilePath) { // 디렉토리 내부의 모든 파일에 대해 쿼리 수행
            mykey.Hadoop_file = file;
            System.out.println(mykey.Hadoop_file);
            Dataset<Row> log = spark.read().json(mykey.Hadoop_file);


            // Example
//            log.groupBy(log.col("I_LogId"), log.col("I_LogDetailId")).count().show();
            log.select(functions.element_at(log.col("I_LogDes"),3)).show(false);
//            log.select(log.col("I_LogDes")).show(false);
/*
            // 동접
            CCU += log
                .filter(log.col("I_RegDateTime").$greater$eq(now_long - 600000))
                .select(log.col("I_PID"))
                .distinct()
                .count();

            // DAU
            DAU += log
                .filter(log.col("I_RegDateTime").between(now_long - now_long % 86400000 - 86400000, now_long - now_long % 86400000))
                .filter(log.col("I_LogId").notEqual(1).notEqual(200))
                .filter (log.col("I_GameCode").equalTo("sknightsgb"))
                .select(log.col("I_PID"))
                .distinct()
                .count();

 */
        }

        System.out.println(CCU);
        System.out.println(DAU);

        timestamp = new Timestamp(System.currentTimeMillis());
        sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        now_str = sdf.format(new Date(timestamp.getTime()));

        System.out.println(now_str);

// ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    }

    public static List<String> getAllFilePath(Path filePath, FileSystem fs) throws FileNotFoundException, IOException {
        List<String> fileList = new ArrayList<String>();
        FileStatus[] fileStatus = fs.listStatus(filePath);
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDirectory()) {
                fileList.addAll(getAllFilePath(fileStat.getPath(), fs));
            } else {
                fileList.add(fileStat.getPath().toString());
            }
        }
        return fileList;
    }
}

