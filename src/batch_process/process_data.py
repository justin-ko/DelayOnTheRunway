from __future__ import print_function
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.functions import to_timestamp, udf
from pyspark.sql.functions import round
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
from pyspark.sql import functions
import timezonefinder, pytz
from pytz import timezone

#import os
#os.environ["PYSPARK_PYTHON"] = "/Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7"
#os.environ["PYSPARK_DRIVER_PYTHON"] = "/Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7"

class getData():
    def __init__(self, filename, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY):
        self.filename = filename
        self.AWS_ACCESS_KEY_ID = AWS_ACCESS_KEY_ID
        self.AWS_SECRET_ACCESS_KEY = AWS_SECRET_ACCESS_KEY

    def getFlightColumns(self, spark_df):
        self.spark_df1 = spark_df.select(col("FlightDate").alias("date").cast('date'), 
                        col("Reporting_Airline").alias("airline_id"),
                        col("Origin").alias("dep_iata"), 
                        col("DepTime").alias("dep_time"), 
                        col("DepDelay").alias("dep_delay").cast('integer'), 
                        col("DepDel15").alias("dep_del15").cast('integer'), 
                        col("Dest").alias("arr_iata"), 
                        col("ArrTime").alias("arr_time"), 
                        col("ArrDelay").alias("arr_delay").cast('integer'),
                        col("ArrDel15").alias("arr_del15").cast('integer'),
                        col("Distance").alias("distance").cast('integer'),
                        col("CancellationCode").alias("cncl_code"),
                        col("WeatherDelay").alias("wthr_code"),
                        col("LateAircraftDelay").alias("late_arr_code"),
                        col("Div1Airport").alias('diverted')
                        )
        return self.spark_df1

    def getWeatherColumns(self, spark_df):
        #drop the first few descriptor rows:
        self.spark_df2 = spark_df.dropna(how="any",subset="Date_Time")
        self.spark_df2 = self.spark_df2.select(col("Date_Time").alias("time_interval").cast('timestamp'),
                        col("wind_cardinal_direction_set_1d").alias("card_dir"),
                        col("wind_direction_set_1").alias("wind_dir").cast('int'),
                        col("wind_speed_set_1").alias("wind_speed").cast('float'),
                        col("wind_gust_set_1").alias("gust_speed").cast('float'),
                        col("weather_condition_set_1d").alias("precip").cast('string'),                        
                        )
        return self.spark_df2

    def getIataColumns(self, spark_df):
        self.spark_df3 = spark_df.select(col("iata").alias("iata_code"),
                        col("lat").alias("lat"),
                        col("long").alias("long"),                          
                        )
        return self.spark_df3
    

    def getTimezoneColumns(self, spark_df):
        self.spark_df5 = spark_df.select(col("iata_code").alias("iata_code").cast('string'),
                        col("timezone").alias("timezone").cast('string'),                        
                        )
        return self.spark_df5


  
class cleanData():
    def __init__(self, df):
        # The dataframe we want to work with
        self.df = df

    def filterByAirport(self, iata_code):
        self.selectedAirports_df = self.df.filter( (self.df.dep_iata == iata_code) | (self.df.arr_iata == iata_code))
        return self.selectedAirports_df

    def convertToTimeUDF(self, col1, col2):
        def convertToTime(self, timeString):

            # This block of if statements are for corner cases
            if timeString is None or timeString == "2400":
                return "00:00"
            if int(timeString) < 10:
                timeString = "000"+timeString
            elif 100 > int(timeString) >= 10:
                timeString = "00"+timeString
            
            try:
                datetimeObject = datetime.strptime(timeString,"%H%M")
            except ValueError as ve:
                print (timeString, ve)
                return "00:00"
            return datetimeObject.strftime("%H:%M")
        
        convertToTime_udf = udf(lambda z: convertToTime(z, z), StringType())
        self.convertedToTime_df = self.df.withColumn( col1, convertToTime_udf(col(col2)))#.cast('timestamp'))  
        return self.convertedToTime_df

    def convertToTimezoneUDF(self, col1, col2):
        def convertToTimezone(self, timestamp, airport_timezone):
            
            # a timestamp to be converted to other timezone
            my_timestamp = timestamp
        
            old_timezone = pytz.timezone(airport_timezone)
            new_timezone = pytz.timezone("America/New_York")

            new_timezone_timestamp = old_timezone.localize(my_timestamp).astimezone(new_timezone)
            return new_timezone_timestamp.replace(tzinfo=None)

        convertToTimezone_udf = udf(lambda x,y: convertToTimezone(x, x,y), TimestampType())
        self.convertToTimezone_df = self.df.withColumn(col1, convertToTimezone_udf( col(col1), col(col2) ))
        return self.convertToTimezone_df

    def convertUTCtoTimezoneUDF(self, col1):
        def convertUTCtoTimezone(self, timestamp):
            # a timestamp to be converted to other timezone
            my_timestamp = timestamp

            # create new and old timezone objects and convert
            old_timezone = pytz.utc
            new_timezone = pytz.timezone("America/New_York")

            # new_timezone_timestamp = old_timezone.localize(my_timestamp).astimezone(new_timezone)
            new_timezone_timestamp = old_timezone.localize(my_timestamp).astimezone(new_timezone)
            return new_timezone_timestamp.replace(tzinfo=None)

        convertUTCtoTimezone_udf = udf(lambda x: convertUTCtoTimezone(x, x), TimestampType())
        self.convertUTCtoTimezone_df = self.df.withColumn(col1, convertUTCtoTimezone_udf(col(col1)) )
        return self.convertUTCtoTimezone_df


    def concatDateTimeUDF(self, col1, col2):
        def concatDateTime(self, dateRow, timeRow):
            #print(datetime.strptime( str(dateRow) + ' ' + str(timeRow) , '%Y-%m-%d %H:%M'))
            return datetime.strptime( str(dateRow) + ' ' + str(timeRow) , '%Y-%m-%d %H:%M')

        concatDateTime_udf = udf(lambda x,y: concatDateTime(x, x,y), TimestampType())
        self.concatDateTime_df = self.df.withColumn(col1 ,concatDateTime_udf(col(col2), col(col1)))
        return self.concatDateTime_df


    def departureOrArrival(self, col1, col2): 
        try:   
            self.departureOrArrival_df = self.df.withColumn('dep_or_arr',
            when(col(col1) == "ATL" , 'departure'). \
            when(col(col2) == "ATL" , 'arrival'))
        except ValueError as ve:
            print (ve, col(col1), col(col2))
            return False
        return self.departureOrArrival_df


    def getAirportTime(self):
        self.airportTime_df = self.df.withColumn ( 'date_time', \
        when(col('dep_or_arr') == 'departure', col('dep_time')). \
        when(col('dep_or_arr') == 'arrival', col('arr_time')))
        return self.airportTime_df


    def getDelayedTime(self):
        self.delayedTime_df = self.df.withColumn ( 'delay_time', \
        when(col('dep_or_arr') == 'departure', col('dep_delay')). \
        when(col('dep_or_arr') == 'arrival', col('arr_delay')))
        return self.delayedTime_df


    def roundFiveMinsUDF(self, col1):
        def roundFiveMins(self, dt=None):
            round_to=300 #round to every 300 seconds or 5 mins
            if dt == None: 
                dt = datetime.now()
            seconds = (dt - dt.min).seconds
            rounding = (seconds+round_to/2) // round_to * round_to
            rounded = dt + timedelta(0,rounding-seconds,-dt.microsecond)
            return rounded
    
        roundFiveMins_udf = udf(lambda x: roundFiveMins(x,x), TimestampType())
        self.roundFiveMins_df = self.df.withColumn('time_interval' ,roundFiveMins_udf(col(col1)))
        return self.roundFiveMins_df


    def convertGustToWind(self):
        self.convertGustToWind_df = self.df.withColumn ( 'wind_speed', \
            when(col('gust_speed') >= col('wind_speed'), col('gust_speed')). \
            otherwise(col('wind_speed')))
        return self.convertGustToWind_df
    

    def bucketizeWindSpeed(self):
        self.bucketizeWind_df = self.df.withColumn ( 'binned_wind', \
            when((col('wind_speed')>=0.00) & (col('wind_speed') < 2.57), '00-05'). \
            when((col('wind_speed')>=2.57) & (col('wind_speed') < 5.14), '05-10'). \
            when((col('wind_speed')>=5.14) & (col('wind_speed') < 7.72), '10-15'). \
            when((col('wind_speed')>=7.72) & (col('wind_speed') < 10.29), '15-20'). \
            when((col('wind_speed')>=10.29) & (col('wind_speed') < 12.86), '20-25'). \
            when((col('wind_speed')>=12.86) & (col('wind_speed') < 15.43), '25-30'). \
            when((col('wind_speed')>=15.43) & (col('wind_speed') < 18.01), '30-35'). \
            when((col('wind_speed')>=18.01), '>35'))
        return self.bucketizeWind_df


    def identifyPrecip(self):
        self.precip_df = self.df.withColumn ( 'precip', \
            when((col('precip') == "Clear") | \
                 (col('precip') == "Fog") | \
                 (col('precip') == "Mostly Clear") | \
                 (col('precip') == "Mostly Cloudy") | \
                 (col('precip') == "N/A") | \
                 (col('precip') == "Overcast") | \
                 (col('precip') == "Partly Cloudy") | \
                 (col('precip') == "Smoke"), 0). \
                 otherwise( 1 ))
        return self.precip_df


    def identifyCrosswind(self):
        self.crosswind_df = self.df.withColumn ( 'crosswind', \
            when((col('card_dir') == "NW") | \
                 (col('card_dir') == "NNW") | \
                 (col('card_dir') == "N") | \
                 (col('card_dir') == "NNE") | \
                 (col('card_dir') == "NE") | \
                 (col('card_dir') == "SE") | \
                 (col('card_dir') == "SSE") | \
                 (col('card_dir') == "S") | \
                 (col('card_dir') == "SSW") | \
                 (col('card_dir') == "SW"), 1). \
                 otherwise( 0 ))
        return self.crosswind_df


    def exceedCrosswindLimit(self):
        self.crosswindLimit_df = self.df.withColumn ( 'crosswind_lim', \
            when((col('precip')==0) & (col('crosswind')==1) & (col('wind_speed')>=15.43), 1). \
            when((col('precip')==1) & (col('crosswind')==1) & (col('wind_speed')>=7.72), 2). \
            otherwise(0))
        return self.crosswindLimit_df        


    # def verifyCrosswindDelayDry(self):
    #     # 1 = delayed by wind   # 2 = cancelled by wind     # 3 = likely delayed by wind    # 4 = likely delayed by wind
    #     self.crosswindDelay_df = self.df.withColumn ( 'cw_del_dry', \
    #         when((col('crosswind_lim')==1) & (col('wthr_code')>0) , 1). \
    #         when((col('crosswind_lim')==1) & (col('cncl_code')=="B") , 2). \
    #         when((col('crosswind_lim')==1) & (col('dep_or_arr')=="departure") & (col('dep_del15')>0), 3). \
    #         when((col('crosswind_lim')==1) & (col('dep_or_arr')=="arrival") & (col('arr_del15')>0), 4). \
    #         when((col('crosswind_lim')==1) & (col('dep_or_arr')=="arrival") & (col('diverted') != None), 5))
    #         #when((col('crosswind_lim')>0) & (col('wthr_code')!=0) & (col('dep_or_arr')=="departure") & (col('dep_del15')>0), "probably delayed by wind"). \
    #         #when((col('crosswind_lim')>0) & (col('wthr_code')!=0) & (col('dep_or_arr')=="arrival") & (col('arr_del15')>0), "probably delayed by wind"))
    #     return self.crosswindDelay_df

    # def verifyCrosswindDelayWet(self):
    #     # 1 = delayed by wind   # 2 = cancelled by wind     # 3 = likely delayed by wind    # 4 = likely delayed by wind
    #     self.crosswindDelay_df = self.df.withColumn ( 'cw_del_wet', \
    #         when((col('crosswind_lim')==2) & (col('wthr_code')>0) , 1). \
    #         when((col('crosswind_lim')==2) & (col('cncl_code')=="B") , 2). \
    #         when((col('crosswind_lim')==2) & (col('dep_or_arr')=="departure") & (col('dep_del15')>0), 3). \
    #         when((col('crosswind_lim')==2) & (col('dep_or_arr')=="arrival") & (col('arr_del15')>0), 4). \
    #         when((col('crosswind_lim')==2) & (col('dep_or_arr')=="arrival") & (col('diverted') != None), 5))
    #         #when((col('crosswind_lim')>0) & (col('wthr_code')!=0) & (col('dep_or_arr')=="departure") & (col('dep_del15')>0), "probably delayed by wind"). \
    #         #when((col('crosswind_lim')>0) & (col('wthr_code')!=0) & (col('dep_or_arr')=="arrival") & (col('arr_del15')>0), "probably delayed by wind"))
    #     return self.crosswindDelay_df

    # def verifyCrosswindDelayTotal(self):
    #     # 1 = delayed by wind   # 2 = cancelled by wind     # 3 = likely delayed by wind    # 4 = likely delayed by wind
    #     self.crosswindDelay_df = self.df.withColumn ( 'cw_del_total', \
    #         when((col('crosswind_lim')>0) & (col('wthr_code')>0) , 1). \
    #         when((col('crosswind_lim')>0) & (col('cncl_code')=="B") , 2). \
    #         when((col('crosswind_lim')>0) & (col('dep_or_arr')=="departure") & (col('dep_del15')>0), 3). \
    #         when((col('crosswind_lim')>0) & (col('dep_or_arr')=="arrival") & (col('arr_del15')>0), 4). \
    #         when((col('crosswind_lim')>0) & (col('dep_or_arr')=="arrival") & (col('diverted') != None), 5))
    #         #when((col('crosswind_lim')>0) & (col('wthr_code')!=0) & (col('dep_or_arr')=="departure") & (col('dep_del15')>0), "probably delayed by wind"). \
    #         #when((col('crosswind_lim')>0) & (col('wthr_code')!=0) & (col('dep_or_arr')=="arrival") & (col('arr_del15')>0), "probably delayed by wind"))
    #     return self.crosswindDelay_df

    def addPrimaryKey(self):
        self.addPrimaryKey_df = self.df.withColumn("unique_id", monotonically_increasing_id())
        #self.rearrangeColumns_df = self.addPrimaryKey_df.select("unique_id", "date_time", "delay_time", "departure_or_arrival", "airline_id", "cncl_code", "wthr_code")
        return self.addPrimaryKey_df #rearrangeColumns_df


    def dropRedundantCols(self):
        self.dropRedundantCols_df = self.df.select(
            #col("date_time").cast('timestamp'), 
            #col("time_interval").cast('timestamp'),
            #col("delay_time").cast('integer'),
            #col("airline_id").cast('string'),
            #col("tail_num").cast('string'),                        
            
            #col("unique_id").cast('integer'),
            col("dep_or_arr").cast('string'),
            
            col("dep_iata").cast("string"),
            #col("dep_timezone").cast("string"),
            col("dep_long").cast("float"),
            col("dep_lat").cast("float"),
            col("dep_time").cast("timestamp"),
            col("dep_delay").cast("integer"),
            col("dep_del15").cast('integer'),

            col("arr_iata").cast("string"),
            #col("arr_timezone").cast("string"),
            col("arr_long").cast("float"),
            col("arr_lat").cast("float"),
            col("arr_time").cast("timestamp"),
            col("arr_delay").cast("integer"),
            col("arr_del15").cast('integer'),                       
            
            col("distance").cast('integer'),
            col("diverted").cast('integer'),

            col("cncl_code").cast('string'),
            col("late_arr_code").cast('string'),
            col("wthr_code").cast('string'),
            
            col("card_dir").cast("string"),
            col("wind_dir").cast("integer"),
            col("wind_speed").cast("float"),
            col("binned_wind").cast("string"),
            
            col("crosswind").cast("integer"),
            col("crosswind_lim").cast("integer"),
            # col("cw_del_dry").cast("integer"),
            # col("cw_del_wet").cast("integer"),
            # col("cw_del_total").cast("integer")
            )
            #col("diverted").cast("integer"))       
        return self.dropRedundantCols_df   
