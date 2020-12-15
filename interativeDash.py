#!/usr/bin/env python
# coding=utf-8
'''
Author: Jin X
Date: 2020-12-15 21:22:19
LastEditTime: 2020-12-15 21:45:50
'''
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px

import pandas as pd

from cassandra.cluster import Cluster

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.H1(children='Stock Analysis on Spark, Kafka'),
    html.H2(children='History'),
    html.H3(children='GOOG'),
    ############################################################################
    dcc.Graph(id='graph-with-slider'),

    html.H2(children='Real-Time'),
    html.H3(children='GOOG'),
    dcc.Graph(id='real-time-goog'),
    ############################################################################


    dcc.Interval(
        id='real-time-update',
        interval=1*10000,  # 1s
        n_intervals=0,
    )
])


@app.callback(
    Output('real-time-goog', 'figure'),
    Input('real-time-update', 'n_intervals'))
def update_figure(n):
    # 改这里读
    # Cassandra
    # df_goog = ...
    statement = "select * from %s where symbol = '%s'" % (quote_table, symbol)
    res = session.execute(statement)
    # for row in res:
    #     print(row)
    df = pd.DataFrame(list(res))
    df = df.round(4)
    print(df)
    fig = px.line(df, x='time', y='price')
    return fig


if __name__ == '__main__':
    # app.run_server(debug=True)
    contact_points = ['localhost']
    cassandra_cluster = Cluster(
        contact_points=contact_points  # many servers, using ',' to split them
    )

    key_space = 'stock'
    quote_table = 'quotes'

    symbol = "GOOG"

    session = cassandra_cluster.connect()
    session.set_keyspace(key_space)

    app.run_server(debug=False)
