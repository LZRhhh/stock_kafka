#!/usr/bin/env python
# coding=utf-8
'''
Author: Jin X
Date: 2020-12-15 18:19:07
LastEditTime: 2020-12-16 12:05:10
'''
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go


def readFig(file):
    df = pd.read_csv(file)
    go_scatter = go.Scatter(x=df['time'], y=df['close'],name="close")
    go_bar = go.Bar(x=df['time'], y=df['growth'], marker_color=df['color'],xaxis='x2',yaxis='y2',name='growth')
    fig = go.Figure(
        data=[go_scatter, go_bar],
        layout={
            "xaxis": {"title": "time", "showgrid": False, "zeroline": False},
            "yaxis": {"title": "close", "showgrid": False},
            "xaxis2": {"title": "time", "side": "top", "overlaying": "x"},
            "yaxis2": {"title": "growth", "side": "right", "overlaying": "y"},
        }
    )
    return fig
