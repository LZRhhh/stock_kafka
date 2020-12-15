#!/usr/bin/env python
# coding=utf-8
'''
Author: Jin X
Date: 2020-12-15 15:57:07
LastEditTime: 2020-12-15 19:31:34
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


with open("./input/daily_GOOG.csv", newline="") as csvfile:
    lines = csv.reader(csvfile, delimiter=',')
    lines = islice(lines, 1, None)
    stocks = [(row[0], float(row[4])) for row in lines]
# print(stocks)
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
    return str(dt), str(newCeilTime)


# k = s.reduce(lambda x, y: (x[1] + y[1]))


def mapFunc(x):
    floor, ceil = floorAndCeil(x[0])
    return [(floor, +x[1]), (ceil, -x[1])]


def reduceFunc(x, y):
    if (x > 0):
        return -x/y - 1
    else:
        return -y/x - 1


rdd = s.flatMap(mapFunc)

# k.toDF(['time', 'growth']).show()

rdd = rdd.reduceByKey(reduceFunc) \
     .filter(lambda x: abs(x[1]) < 1)
# k.collect()
# print(s.glom().collect())
# print(k.toDF(["Time", "Relative"]).toPandas())
# k.toDF(['time', 'growth']).show()

pdDF = rdd.toDF(["time", "growth"]).toPandas()

pdDF["color"] = pdDF.apply(lambda x: 'raise' if x.growth > 0 else 'drop', axis=1)


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


fig = px.bar(pdDF, x="time", y="growth", color="color")


app.layout = html.Div(children=[
    html.H1(children='History Stock Analysis'),
    html.H2(children='GOOG'),
    dcc.Graph(
        id='example-graph',
        figure=fig
    )
])

app.run_server(debug=False, port=8085)