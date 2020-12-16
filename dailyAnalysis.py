#!/usr/bin/env python
# coding=utf-8
'''
Author: Jin X
Date: 2020-12-15 15:57:07
LastEditTime: 2020-12-16 12:17:15
'''

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import csv
from itertools import islice
from dateutil.parser import parse
from datetime import timedelta

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go

with open("./input/daily_FB.csv", newline="") as csvfile:
    lines = csv.reader(csvfile, delimiter=',')
    lines = islice(lines, 1, None)
    stocks = [(row[0], float(row[4])) for row in lines]
conf = SparkConf()

conf.setAppName('dailyAnalysis')
conf.setMaster('local[*]')
conf.set('spark.executor.memory', '1g')
conf.set('spark.executor.cores', '8')

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
s = sc.parallelize(stocks)


def floorAndCeil(day):
    dt = parse(day)
    if(dt.isoweekday() != 5):
        ceilTime = timedelta(days=-1, seconds=dt.second, microseconds=dt.microsecond,
                             milliseconds=0, minutes=dt.minute, hours=dt.hour, weeks=0)
    else:
        ceilTime = timedelta(days=-3, seconds=dt.second, microseconds=dt.microsecond,
                             milliseconds=0, minutes=dt.minute, hours=dt.hour, weeks=0)
    newCeilTime = dt - ceilTime
    return dt.strftime("%Y-%m-%d"), newCeilTime.strftime("%Y-%m-%d")




def mapFunc(x):
    floor, ceil = floorAndCeil(x[0])
    return [(floor, +x[1]), (ceil, -x[1])]


def reduceFunc(x, y):
    if (x > 0):
        return -x/y - 1
    else:
        return -y/x - 1


rdd = s.flatMap(mapFunc)


rdd = rdd.reduceByKey(reduceFunc) \
     .filter(lambda x: abs(x[1]) < 1)

pdDF = rdd.toDF(["time", "growth"]).toPandas()

closeInfo = pd.read_csv('./input/daily_FB.csv', usecols=['time','close'])

pdDF = pd.merge(pdDF, closeInfo, how="right", on="time", copy=False)



pdDF["color"] = pdDF.apply(lambda x: 'rgba(23,170,23,0.7)' if x.growth > 0 else 'rgba(170,23,23,0.7)', axis=1)

pdDF.to_csv("./output/daily_fb.csv", index=0)

# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

# app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# go_scatter = go.Scatter(x=pdDF['time'], y=pdDF['close'])

# go_bar = go.Bar(x=pdDF['time'], y=pdDF['growth'], marker_color=pdDF['color'],xaxis='x2',yaxis='y2')

# fig = go.Figure(
#     data=[go_scatter, go_bar],
#     layout={
#         "xaxis": {"title": "time", "showgrid": False, "zeroline": False},
#         "yaxis": {"title": "close", "showgrid": False},
#         "xaxis2": {"title": "time", "side": "top", "overlaying": "x"},
#         "yaxis2": {"title": "growth", "side": "right", "overlaying": "y"},
#     }
# )

# fig = px.line(pdDF, x="time", y="close")
# fig.add_bar(x=pdDF['time'], y=pdDF["growth"])
# fig = px.bar(pdDF, x="time", y="growth", color='color')
# fig.add_line(x=pdDF['time'], y=pdDF['close'])
# fig.add_trace(
#     go.Bar(x=pdDF['time'], y=pdDF['growth'], marker_color=pdDF['color'])
# )
# fig.add_trace(
#     go.Scatter(x=pdDF['time'], y=pdDF['close'])
# )
# print(pdDF)


# fig.update_layout(
    
# )


# app.layout = html.Div(children=[
#     html.H1(children='History Stock Analysis'),
#     html.H2(children='GOOG'),
#     dcc.Graph(
#         id='example-graph',
#         figure=fig
#     )
# ])

# app.run_server(debug=False, port=8085)