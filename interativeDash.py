#!/usr/bin/env python
# coding=utf-8
'''
Author: Jin X
Date: 2020-12-15 21:22:19
LastEditTime: 2020-12-16 12:20:20
'''
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go

import pandas as pd

from cassandra.cluster import Cluster

def readFig(symbol):
    df = pd.read_csv('./output/daily_{}.csv'.format(symbol))
    go_scatter = go.Scatter(x=df['time'], y=df['close'], name="close")
    go_bar = go.Bar(x=df['time'], y=df['growth'],
                    marker_color=df['color'], xaxis='x2', yaxis='y2', name='growth')
    fig = go.Figure(
        data=[go_scatter, go_bar],
        layout={
            "xaxis": {"title": "time", "showgrid": False, "zeroline": False},
            "yaxis": {"title": "close", "showgrid": False},
            "xaxis2": {"title": "time", "side": "top", "overlaying": "x"},
            "yaxis2": {"title": "growth", "side": "right", "overlaying": "y"},
        }
    )
    return html.Div([
        html.H3(children=symbol),
        dcc.Graph(
            id="daily-" + symbol,
            figure=fig
        )
    ])


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.H1(children='Stock Analysis on Spark, Kafka'),
    html.H2(children='Static-Daily'),
    readFig("GOOG"),
    readFig("AMZN"),
    readFig("FB"),
    readFig("AAPL"),
    readFig("MSFT"),
    ############################################################################

    html.H2(children='Real-Time'),
    html.H3(children='GOOG'),
    dcc.Graph(id='real-time-goog'),
    dcc.Graph(id='real-time-goog-mean'),
    ############################################################################

    dcc.Interval(
        id='real-time-update',
        interval=1 * 10000,  # 1s
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
    # print(df)

    layout = dict(title='Trace',
                  xaxis=dict(title='Time'),  # 横轴坐标
                  yaxis=dict(title='Price'),  # 总轴坐标
                  legend=dict(x=1, y=1)  # 图例位置
                  )

    fig = go.Figure(layout=layout)
    # 画第一个图
    fig.add_trace(
        go.Line(
            x=df['time'],
            y=df['close'],
            name='price'
        ))
    # 画第二个图
    fig.add_trace(
        go.Line(
            x=df['time'],
            y=df['mean'],
            name='avg',
            line=dict(dash='dash')
        ))
    fig.add_trace(
        go.Line(
            x=df['time'],
            y=df['min'],
            name='low',
            line=dict(dash='dash')
        ))
    fig.add_trace(
        go.Line(
            x=df['time'],
            y=df['max'],
            name='high',
            line=dict(dash='dash')
        ))

    return fig


@app.callback(
    Output('real-time-goog-mean', 'figure'),
    Input('real-time-update', 'n_intervals'))
def update_figure(n):
    # 改这里读
    # Cassandra
    # df_goog = ...
    statement = "select time, per from %s where symbol = '%s'" % (quote_table, symbol)
    res = session.execute(statement)
    # for row in res:
    #     print(row)
    df = pd.DataFrame(list(res))
    df['color'] = df.apply(lambda x: 'rgba(23,170,23,0.7)' if x.per > 0 else 'rgba(170,23,23,0.7)', axis=1)
    # print(df)
    df = df.round(4)

    layout = dict(title='Per',
                  xaxis=dict(title='Time'),  # 横轴坐标
                  yaxis=dict(title='Per (%)'),  # 总轴坐标
                  legend=dict(x=1.1, y=1)  # 图例位置
                  )

    fig = go.Figure(layout=layout)
    # 画第一个图
    fig.add_trace(
        go.Bar(
            x=df['time'],
            y=df['per'],
            name='per',
            marker_color=df['color']
        ))

    # fig = px.bar(df, x='time', y='per', name='per', color=df['color'], labels=dict(x="time", y="per %"))

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
