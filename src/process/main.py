from __future__ import print_function
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
# from yamlParser import yamlParser
#from getData import getData
from clean_data import *

# import os
# os.environ["PYSPARK_PYTHON"] = "/Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7"

if __name__ == "__main__":
    #myFile = yamlParser("credentials/aws.yaml")
    AWS_ACCESS_KEY_ID = "AKIARNRTM5DQRVYFXH5W" #myFile.read()['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = "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" #myFile.read()['AWS_SECRET_ACCESS_KEY']
    POSTGRESQL_URL = "jdbc:postgresql://ec2-54-184-116-80.us-west-2.compute.amazonaws.com:5432/flight_data"
    POSTGRESQL_TABLE = "public.flight_weather_table_half"
    POSTGRESQL_USER = "postgres"
    POSTGRESQL_PASSWORD = "postgres"

    #filename1 = "s3a://bucketsjustin/BOT_Flight_Data/2019/660007046_T_ONTIME_REPORTING-11.csv"
    #filename1 = "s3a://bucketsjustin//BTS_flight_data/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2019_11.csv"
    filename1 = "s3a://bucketsjustin/BTS_flight_data/*.csv"
    #filename1 = "s3a://bucketsjustin/BTS_flight_data_full/*.csv"
    #filename1 = "s3a://bucketsjustin/all_airports_1.csv"
    filename2 = "s3a://bucketsjustin/weather_data.csv"
    filename3 = "s3a://bucketsjustin/iata.csv"
    filename4 = "s3a://bucketsjustin/timezone.csv"

    # filename1 = "Data/sample_flight_data.csv"
    # filename2 = "Data/sample_weather_data.csv"
    # filename3 = "Data/iata.csv"
    # filename4 = "Data/timezone.csv"
    
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount2")\
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sc.addPyFile("cleanData.py")

    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    

    ### FLIGHT AND WEATHER
    spark_flight_df = spark.read.option("header", "true").csv(filename1)
    flight_csv = getData(filename1, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    flight_df = flight_csv.getFlightColumns(spark_flight_df)
    flight_df = cleanData(flight_df).filterByAirport("ATL")
    flight_df = cleanData(flight_df).departureOrArrival("dep_iata", "arr_iata")
    flight_df1 = cleanData(flight_df).convertToTimeUDF("dep_time", "dep_time")
    flight_df2 = cleanData(flight_df1).convertToTimeUDF("arr_time", "arr_time")
    flight_df3 = cleanData(flight_df2).concatDateTimeUDF("dep_time", "date", "dep_time")
    flight_df4 = cleanData(flight_df3).concatDateTimeUDF("arr_time", "date", "arr_time")
    flight_df6 = cleanData(flight_df4).getAirportTime()
    flight_df7 = cleanData(flight_df6).getDelayedTime()
    flight_df8 = cleanData(flight_df7).roundFiveMinsUDF("date_time")
    
    spark_weather_df = spark.read.option("header", "true").csv(filename2)
    weather_csv = getData(filename2, AWS_SECRET_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
    weather_df = weather_csv.getWeatherColumns(spark_weather_df)
    weather_df1 = cleanData(weather_df).convertGustToWind()
    weather_df2 = cleanData(weather_df1).bucketizeWindSpeed()
    weather_df3 = cleanData(weather_df2).identifyPrecip()
    weather_df4 = cleanData(weather_df3).convertUTCtoTimezoneUDF("time_interval")
    weather_df5 = cleanData(weather_df4).identifyCrosswind()
    weather_df6 = cleanData(weather_df5).exceedCrosswindLimit()
    #weather_df6.printSchema()
    
    join_flight_weather_df = flight_df8.join( weather_df6, on ='time_interval', how='left' )
    join_flight_weather_df = cleanData(join_flight_weather_df).verifyCrosswindDelayDry()
    join_flight_weather_df = cleanData(join_flight_weather_df).verifyCrosswindDelayWet()
    join_flight_weather_df = cleanData(join_flight_weather_df).verifyCrosswindDelayTotal()
    join_flight_weather_df.printSchema()
    
    ### TIMEZONE AND IATA
    spark_timezone_df = spark.read.option("header", "true").csv(filename4)
    timezone_csv = getData(filename4, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    timezone_df = timezone_csv.getTimezoneColumns(spark_timezone_df)
 
    spark_iata_df = spark.read.option("header", "true").csv(filename3)
    iata_csv = getData(filename3, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    iata_df = iata_csv.getIataColumns(spark_iata_df)
    
    join_timezone_iata_df = iata_df.join(timezone_df, on = "iata_code", how="left")
    dep_join_tz_iata_df = join_timezone_iata_df.select(
                                col("iata_code").alias("dep_iata"),
                                col("lat").alias("dep_lat"),
                                col("long").alias("dep_long"),
                                col("timezone").alias("dep_timezone"))

    arr_join_tz_iata_df = join_timezone_iata_df.select(
                                col("iata_code").alias("arr_iata"),
                                col("lat").alias("arr_lat"),
                                col("long").alias("arr_long"),
                                col("timezone").alias("arr_timezone"))
    
    ### JOIN FLIGHT AND TIMEZONE
    join_flight_tz_df = join_flight_weather_df.join(dep_join_tz_iata_df, on= "dep_iata" ,how="left")
    join_flight_tz_df = join_flight_tz_df.join(arr_join_tz_iata_df, on= "arr_iata" ,how="left")
    
    ### CONVERT local timestamp to ATL timestamp
    convert_dep_arr_tz_df = cleanData(join_flight_tz_df).convertToTimezoneUDF("dep_time", "dep_timezone")
    convert_dep_arr_tz_df = cleanData(convert_dep_arr_tz_df).convertToTimezoneUDF("arr_time", "arr_timezone")
    convert_dep_arr_tz_df = cleanData(convert_dep_arr_tz_df).addPrimaryKey()
    convert_dep_arr_tz_df = cleanData(convert_dep_arr_tz_df).dropRedundantCols()    
    convert_dep_arr_tz_df.show(n=50)
    convert_dep_arr_tz_df.printSchema()
    print("Pyspark job finished at: ", datetime.utcnow())
    
    ###.mode("overwrite") or .mode("append")
    convert_dep_arr_tz_df.write \
            .format("jdbc") \
            .option("url", POSTGRESQL_URL) \
            .option("dbtable", POSTGRESQL_TABLE) \
            .option("user", POSTGRESQL_USER) \
            .option("password", POSTGRESQL_PASSWORD) \
            .option("truncate", "true") \
            .mode("append") \
            .save()
    print("Writing to postgres finished at: ", datetime.utcnow())