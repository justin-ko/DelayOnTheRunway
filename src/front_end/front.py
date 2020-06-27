# http://ec2-52-43-242-60.us-west-2.compute.amazonaws.com:8050
# http://datapipeline.club:8050

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
from dash.dependencies import Input, Output
import json 
import requests
import plotly.express as px
import pandas as pd
from datetime import datetime
import re

flask_IP = "http://10.0.0.4:5000"

dep_or_arr_dropdown = [{'label': 'Departure from ATL', 'value': 'departure'},
                        {'label': 'Arrival to ATL', 'value': 'arrival'}]

def updateHeatmap(dep_or_arr, start_date, end_date):
    call_API = flask_IP + "/airportDelayCount"
    call_API = call_API + "?dep_or_arr=" + dep_or_arr
    call_API = call_API + "&datetime_lower=" + str(start_date) + " 00:00"
    call_API = call_API + "&datetime_upper=" + str(end_date) + " 23:59"
    response = requests.get(call_API).json()    
    df_delay_count = pd.DataFrame(response)

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

    df_delay_count = df_delay_count[df_delay_count[iata] != 'ATL'] 
    fig = px.scatter_geo(df_delay_count, lon=longitude, lat=latitude, #color=del15,
                    size=del15, locationmode = 'USA-states', center={"lat": 39.8283, "lon": -98.5795}, scope='usa', hover_name=iata, height=400)
    fig.update_layout(
        legend_title_text='Trend',
    )
    return fig


def updateDelayTable(dep_or_arr, start_date, end_date):
    call_API = flask_IP + "/countCrosswindDelay"
    call_API = call_API + "?dep_or_arr=" + dep_or_arr
    call_API = call_API + "&datetime_lower=" + str(start_date) + " 00:00"
    call_API = call_API + "&datetime_upper=" + str(end_date) + " 23:59"
    response = requests.get(call_API).json()    
    df_delay_count = pd.DataFrame(response, columns = ['cond','dry','precip'])

    if dep_or_arr == "departure":
        time = "dep_time"
    if dep_or_arr == "arrival":
        time = "arr_time"

    fig = px.bar(df_delay_count, x='cond', y=['dry','precip'], barmode='group',height=400)
    fig.update_layout(
        yaxis_title="Count",
        xaxis_title="Cause for Interruption",
        legend=dict(
            title = "Runway Condition",

        )
    )
    return fig


def updateWindRose(dep_or_arr, datetime_lower, datetime_upper):
    # Call API with the parameter
    call_API = flask_IP + "/windDirection?"
    call_API = call_API + "dep_or_arr=" + dep_or_arr
    call_API = call_API + "&datetime_lower=" + str(datetime_lower) + " 00:00"
    call_API = call_API + "&datetime_upper=" + str(datetime_upper) + " 23:59"
    response = requests.get(call_API).json()

    df = pd.DataFrame(response)
    fig = px.bar_polar(df, r="count", theta="card_dir",
                color="binned_wind", template="plotly_white",
                color_discrete_sequence= px.colors.sequential.Plasma_r)
    fig.update_layout(
        legend=dict(
            title = "Wind Speed (knots)",
            traceorder="grouped"
        )
    )
    return fig


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1(children='Delay On The Runway'),
    html.H3('Tracking Flight Delays from Takeoff and Landing', style={'color': 'black', 'fontSize': 24}),
    html.H4('By: Justin Ko (20B DE.BOS)', style={'color': 'black', 'fontSize': 18}),
    
    # Drop down and range date range picker
    html.Div([
        html.Div('Select', style={'color': 'black', 'fontSize': 24}),
        dcc.Dropdown(
            id='dep_or_arr_dropdown',
            options=dep_or_arr_dropdown,
            value='departure'
        ),   

        dcc.DatePickerRange(
            id='wind_rose_date_picker',
            min_date_allowed=datetime(2003, 1, 1),
            max_date_allowed=datetime(2020, 2, 29),
            initial_visible_month=datetime(2019, 11, 5),
            end_date=datetime(2019, 11, 30).date()
        ),
    ], style={'marginBottom': 25, 'marginTop': 10}),
    
    # Heatmap
    html.Div([
        html.Div('Delay Count', style={'color': 'black', 'fontSize': 24}),
        html.Div('Represents the delay counts from departure to ATL or destination from ATL.', style={'color': 'black', 'fontSize': 14}),
        dcc.Graph(id='heatmap'),
    ]),

    # Windrose & Barchart
    html.Div([
        html.Div([
            html.Div('Wind-Rose Diagram', style={'color': 'black', 'fontSize': 24}),
            html.Div('Represents the number of flights that departed/arrived under a given wind speed and direction.', style={'color': 'black', 'fontSize': 14}),
            dcc.Graph(id = 'wind-rose',),
        ], className ='six columns'),

        html.Div([
            html.Div('Interruptions from Crosswind (Dry vs. Precipitation)', style={'color': 'black', 'fontSize': 24}),
            html.Div('Shows the number of "weather related" interruptions specifically caused by the crosswinds (CW). The maximum allowable limit for CW is much lower with precipitation on the runway.', style={'color': 'black', 'fontSize': 14}),
            dcc.Graph(id = 'bar-chart'),
        ], className ='six columns'),
    ], className = 'row')  
])

@app.callback(
    [dash.dependencies.Output(component_id='heatmap', component_property='figure'),
    dash.dependencies.Output(component_id='wind-rose', component_property='figure'),
    dash.dependencies.Output(component_id='bar-chart', component_property='figure')],
    [dash.dependencies.Input(component_id = 'dep_or_arr_dropdown', component_property='value'),
    dash.dependencies.Input('wind_rose_date_picker', 'start_date'),
    dash.dependencies.Input('wind_rose_date_picker', 'end_date')]
)

def updateGraphs(dep_or_arr, datetime_lower, datetime_upper):
    delay_graph = updateHeatmap(dep_or_arr, datetime_lower, datetime_upper)
    wind_rose_graph = updateWindRose(dep_or_arr, datetime_lower, datetime_upper)
    bar_chart = updateDelayTable(dep_or_arr, datetime_lower, datetime_upper)
    return delay_graph, wind_rose_graph, bar_chart

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', debug=True)

