from __future__ import print_function
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
from process_data import *
#from yamlParser import yamlParser

if __name__ == "__main__":
    myFile = yamlParser("credentials/aws.yaml")
    AWS_ACCESS_KEY_ID = myFile.read()['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = myFile.read()['AWS_SECRET_ACCESS_KEY']
    
    AWS_ACCESS_KEY_ID = ""
    AWS_SECRET_ACCESS_KEY = ""
    POSTGRESQL_URL = ""
    POSTGRESQL_TABLE = ""
    POSTGRESQL_USER = ""
    POSTGRESQL_PASSWORD = ""

    filename1 = ""
    filename2 = ""
    filename3 = ""
    filename4 = ""

    spark = SparkSession\
        .builder\
        .appName("DelayOnTheRunway")\
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sc.addPyFile("process_data.py")

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
    flight_df3 = cleanData(flight_df2).concatDateTimeUDF("dep_time", "date")
    flight_df4 = cleanData(flight_df3).concatDateTimeUDF("arr_time", "date")
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

    join_flight_weather_df = flight_df8.join( weather_df6, on ='time_interval', how='left' )
    # join_flight_weather_df = cleanData(join_flight_weather_df).verifyCrosswindDelayDry()
    # join_flight_weather_df = cleanData(join_flight_weather_df).verifyCrosswindDelayWet()
    # join_flight_weather_df = cleanData(join_flight_weather_df).verifyCrosswindDelayTotal()
    
    # Create timezone dataframe
    spark_timezone_df = spark.read.option("header", "true").csv(filename4)
    timezone_csv = getData(filename4, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    timezone_df = timezone_csv.getTimezoneColumns(spark_timezone_df)
 
    # Create IATA dataframe
    spark_iata_df = spark.read.option("header", "true").csv(filename3)
    iata_csv = getData(filename3, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    iata_df = iata_csv.getIataColumns(spark_iata_df)
    
    # Join timezone and IATA dataframes
    join_timezone_iata_df = iata_df.join(timezone_df, on = "iata_code", how="left")
    
    # Create a IATA dataframe that are departing
    dep_join_tz_iata_df = join_timezone_iata_df.select(
                                col("iata_code").alias("dep_iata"),
                                col("lat").alias("dep_lat"),
                                col("long").alias("dep_long"),
                                col("timezone").alias("dep_timezone"))

    # Create a IATA dataframe that are arriving
    arr_join_tz_iata_df = join_timezone_iata_df.select(
                                col("iata_code").alias("arr_iata"),
                                col("lat").alias("arr_lat"),
                                col("long").alias("arr_long"),
                                col("timezone").alias("arr_timezone"))
    
    # Join flight and timezone dataframes
    join_flight_tz_df = join_flight_weather_df.join(dep_join_tz_iata_df, on= "dep_iata" ,how="left")
    join_flight_tz_df = join_flight_tz_df.join(arr_join_tz_iata_df, on= "arr_iata" ,how="left")

    # Convert local timestamp to ATL timestamp
    convert_dep_arr_tz_df = cleanData(join_flight_tz_df).convertToTimezoneUDF("dep_time", "dep_timezone")
    convert_dep_arr_tz_df = cleanData(convert_dep_arr_tz_df).convertToTimezoneUDF("arr_time", "arr_timezone")
    
    #convert_dep_arr_tz_df = cleanData(convert_dep_arr_tz_df).addPrimaryKey()
    convert_dep_arr_tz_df.show(n=20)
    convert_dep_arr_tz_df = cleanData(convert_dep_arr_tz_df).dropRedundantCols()    
    
    convert_dep_arr_tz_df.printSchema()
    print("Pyspark job finished at: ", datetime.utcnow())
    
    # Write to PostgreSQL database
    convert_dep_arr_tz_df.write \
            .format("jdbc") \
            .option("url", POSTGRESQL_URL) \
            .option("dbtable", POSTGRESQL_TABLE) \
            .option("user", POSTGRESQL_USER) \
            .option("password", POSTGRESQL_PASSWORD) \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()
    print("Writing to postgres finished at: ", datetime.utcnow())

