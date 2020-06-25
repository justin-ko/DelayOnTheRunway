import psycopg2
import flask
from flask import request, jsonify
import pandas as pd
import datetime
from process_query import processQuery
from datetime import datetime

# Postgress settings
POSTGRESQL_DB = "flight_data"
POSTGRESQL_USER = "postgres"
POSTGRESQL_PASSWORD = "postgres"
POSTGRESQL_HOST = "10.0.0.10"
POSTGRESQL_PORT = "5432"

app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
    return "Hello World"

@app.route('/windDirection', methods=['GET'])
def getAirlineWindDirection():
    # API call to retrieve the number of wind directions given an airline
    parameters = request.args
    dep_or_arr = parameters.get('dep_or_arr')
    datetime_lower = parameters.get('datetime_lower')
    datetime_upper = parameters.get('datetime_upper')

    # Specify "dep" or "arr" related columns
    if dep_or_arr == "departure":
        time = "dep_time"
        del15 = "dep_del15"
    if dep_or_arr == "arrival":
        time = "arr_time"
        del15 = "arr_del15"

    # Query statement
    query = "SELECT " + time + ", card_dir, wind_speed, binned_wind," + del15 + " "
    query = query + "FROM flight_weather_table_timeit "
    query = query + "WHERE "
    query = query + time + " >= '" + datetime_lower + "' AND "
    query = query + time +  " < '" + datetime_upper + "' AND "
    query = query + del15 + " > '0' AND "  
    query = query + time + " IS NOT NULL AND "
    query = query + "card_dir IS NOT NULL"
    
    # Initiate cursor to select rows 
    cur.execute(query)
    # Fetch all the list of tuple
    rows = cur.fetchall()
    
    updatedQuery = processQuery(rows).convertToDataframe(time, del15)
    return updatedQuery.to_json()


@app.route('/airportDelayCount', methods=['GET'])
def getDelayCount():
    parameters = request.args
    dep_or_arr = parameters.get('dep_or_arr')
    datetime_lower = parameters.get('datetime_lower')
    datetime_upper = parameters.get('datetime_upper')

    # Specify "dep" or "arr" related columns
    if dep_or_arr == "departure":
        iata = "dep_iata"
        time = "dep_time"
        longitude = "dep_long"
        latitude = "dep_lat"
        del15 = "dep_del15"
    if dep_or_arr == "arrival":
        iata = "arr_iata"
        time = "arr_time"
        longitude = "arr_long"
        latitude = "arr_lat"
        del15 = "arr_del15"

    # Query statement
    query = "SELECT " + iata + ", " + longitude + ", " + latitude + ",COUNT(" + del15 + ") " 
    query = query + "FROM flight_weather_table_timeit WHERE "
    query = query + time + ">='" + datetime_lower + "' AND "
    query = query + time +  "<'" + datetime_upper + "' AND " 
    query = query + time + " IS NOT NULL AND "
    query = query + del15 + " > '0' " 
    query = query + "GROUP BY " + iata + ", " + longitude + ", " + latitude + " "
    query = query + "ORDER BY COUNT(" + del15 + ") DESC"

    # Initiate cursor to select rows 
    cur.execute(query)
    # Fetch all the list of tuple
    rows = cur.fetchall()

    updatedQuery = pd.DataFrame(rows, columns = [iata, longitude, latitude, del15]) 
    return updatedQuery.to_json()


@app.route('/countCrosswindDelay', methods=['GET'])
def getCrosswindDelay():
    parameters = request.args
    dep_or_arr = parameters.get('dep_or_arr')
    datetime_lower = parameters.get('datetime_lower')
    datetime_upper = parameters.get('datetime_upper')

    # Specify "dep" or "arr" related columns
    if dep_or_arr == "departure":
        time = "dep_time"
        del15 = "dep_del15"
    
    if dep_or_arr == "arrival":
        time = "arr_time"
        del15 = "arr_del15"
    
    # Query statement for dry runway condition (crosswind_lim = 1)
    query1 =          "SELECT count(*) FROM flight_weather_table_timeit WHERE crosswind_lim = '1' AND wthr_code > '0' AND " + time + ">='" + datetime_lower + "' AND " + time + "<'" + datetime_upper + "' UNION ALL "
    query1 = query1 + "SELECT count(*) FROM flight_weather_table_timeit WHERE crosswind_lim = '1' AND cncl_code = 'B' AND " + time + ">='" + datetime_lower + "' AND " + time + "<'" + datetime_upper + "' "
    
    # Query statement for wet runway condition (crosswind_lim = 2)
    query2 =          "SELECT count(*) FROM flight_weather_table_timeit WHERE crosswind_lim = '2' AND wthr_code > '0' AND " + time + ">='" + datetime_lower + "' AND " + time + "<'" + datetime_upper + "' UNION ALL "
    query2 = query2 + "SELECT count(*) FROM flight_weather_table_timeit WHERE crosswind_lim = '2' AND cncl_code = 'B' AND " + time + ">='" + datetime_lower + "' AND " + time + "<'" + datetime_upper + "' "
    
    # Initiate cursor to select rows 
    cur.execute(query1)
    # Fetch all the list of tuple
    rows1 = cur.fetchall()
    # convert the tuples into a list
    rows1 = [item for tup in rows1 for item in tup]
    
    # Initiate cursor to select rows 
    cur.execute(query2)
    # Fetch all the list of tuple
    rows2 = cur.fetchall()
    # convert the tuples into a list
    rows2 = [item for tup in rows2 for item in tup]
    
    delay_condition = ['weather delay by CW','weather cancellation by CW']
    updatedQuery = pd.DataFrame({ 'cond':delay_condition,'dry':rows1, 'precip':rows2})

    return updatedQuery.to_json()


if __name__ == "__main__":
    # Connect to the database 
    con = psycopg2.connect(database=POSTGRESQL_DB,
                            user=POSTGRESQL_USER, 
                            password=POSTGRESQL_PASSWORD, 
                            host=POSTGRESQL_HOST, 
                            port=POSTGRESQL_PORT)
    
    # Initiate cursor to select rows 
    cur = con.cursor()
    app.run(host='0.0.0.0', debug=True)
