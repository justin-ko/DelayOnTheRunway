import psycopg2
import flask
from flask import request, jsonify
import pandas as pd

class processQuery():
    def __init__(self, raw_query):
        self.raw_query = raw_query

    def convertToDataframe(self, time, del15):
        raw_query_df = pd.DataFrame(self.raw_query, columns = [ time, "card_dir", "wind_speed", "binned_wind", del15 ]) 
        raw_query_df["count"] = 1
        raw_query_df = raw_query_df.groupby(["card_dir", "binned_wind"])["count"].count().reset_index()    
        dir_list = ['N','NNE', 'NE','ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW','NW', 'NNW']
        raw_query_df["index_dir"] = 0

        wind_dir = list(raw_query_df['card_dir'].unique())

        for idx, nsew in enumerate(dir_list):
            if nsew in wind_dir:
                # if that direction exists
                raw_query_df.loc[raw_query_df["card_dir"] == nsew, "index_dir"] = idx
            else:
                # if that direction does not exists, create dummy row for that wind direction
                missing_dir = pd.DataFrame({"card_dir": [nsew], "binned_wind":["00-05"], "count": [0], 'index_dir':[idx]}) 
                raw_query_df = raw_query_df.append(missing_dir, ignore_index=True)

        raw_query_df = raw_query_df.sort_values(["index_dir","binned_wind"])
        return  raw_query_df

    def convertDepDelayCountDF(self):
        raw_query_df = pd.DataFrame(self.raw_query, columns = ["dep_iata", "dep_long", "dep_lat", "delay_count"])
        return  raw_query_df