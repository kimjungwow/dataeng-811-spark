package com.eighteleven.netmarble;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Log_Schema {
    StructType log_schema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("I_LogId", DataTypes.IntegerType, true),
            DataTypes.createStructField("I_LogDetailId", DataTypes.IntegerType, true),
            DataTypes.createStructField("I_GameCode", DataTypes.StringType, true),
            DataTypes.createStructField("I_PID", DataTypes.StringType, true),
            DataTypes.createStructField("I_CountryCD", DataTypes.StringType, true),
            DataTypes.createStructField("I_LanguageCD", DataTypes.StringType, true),
            DataTypes.createStructField("I_OS", DataTypes.StringType, true),
            DataTypes.createStructField("I_DeviceOSVersion", DataTypes.StringType, true),
            DataTypes.createStructField("I_DeviceModel", DataTypes.StringType, true),
            DataTypes.createStructField("I_TimeZone", DataTypes.StringType, true),
            DataTypes.createStructField("I_SDKVersion", DataTypes.StringType, true),
            DataTypes.createStructField("I_ChannelType", DataTypes.StringType, true),
            DataTypes.createStructField("I_ConnectIP", DataTypes.StringType, true),
            DataTypes.createStructField("I_TID", DataTypes.StringType, true),
            DataTypes.createStructField("I_GameVersion", DataTypes.StringType, true),
            DataTypes.createStructField("I_Now", DataTypes.StringType, true),
            DataTypes.createStructField("I_NMTimestamp", DataTypes.LongType, true),
            DataTypes.createStructField("I_Region", DataTypes.StringType, true),
            DataTypes.createStructField("I_UDID", DataTypes.StringType, true),
            DataTypes.createStructField("I_I_PlatformADID", DataTypes.StringType, true),
            DataTypes.createStructField("I_JoinedCountryCode", DataTypes.StringType, true),
            DataTypes.createStructField("I_World", DataTypes.StringType, true),
            DataTypes.createStructField("I_OldNMDeviceKey", DataTypes.StringType, true),
            DataTypes.createStructField("I_LogKey", DataTypes.StringType, true),
            DataTypes.createStructField("I_RetryCount", DataTypes.IntegerType, true),
            DataTypes.createStructField("I_RetryReason", DataTypes.StringType, true),
            DataTypes.createStructField("I_NMMarket", DataTypes.StringType, true),
            DataTypes.createStructField("I_NMCharacterID", DataTypes.StringType, true),
            DataTypes.createStructField("I_City", DataTypes.StringType, true),
            DataTypes.createStructField("I_LogDes", DataTypes.StringType, true), // 추후 변경해야 함
            DataTypes.createStructField("IP", DataTypes.StringType, true),
            DataTypes.createStructField("I_IPHeader", DataTypes.StringType, true),
            DataTypes.createStructField("I_RegDateTime", DataTypes.LongType, true),
            DataTypes.createStructField("I_ConnectorVersion", DataTypes.StringType, true),
            DataTypes.createStructField("I_RequestTime", DataTypes.StringType, true),
            DataTypes.createStructField("I_NMRequestTime", DataTypes.LongType, true),
            DataTypes.createStructField("I_SessionID", DataTypes.StringType, true),
            DataTypes.createStructField("I_IDFV", DataTypes.StringType, true),
            DataTypes.createStructField("I_NMEventTime", DataTypes.LongType, true),
            DataTypes.createStructField("I_NMMobileIP", DataTypes.StringType, true),
            DataTypes.createStructField("I_NMManufacturer", DataTypes.StringType, true),
            DataTypes.createStructField("I_NMModel", DataTypes.StringType, true),
    });

    StructType logdes_schema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("logFormat", DataTypes.DateType, false)
    });
}
