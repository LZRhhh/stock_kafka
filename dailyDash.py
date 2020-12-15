#!/usr/bin/env python
# coding=utf-8
'''
Author: Jin X
Date: 2020-12-15 18:19:07
LastEditTime: 2020-12-15 20:40:19
'''
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd

stocks = pd.read_csv('./input/daily_GOOG.csv', usecols=['time', 'close'])

print(stocks)

# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

# app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# # assume you have a "long-form" data frame
# # see https://plotly.com/python/px-arguments/ for more options
# df = pd.DataFrame({
#     "Fruit": ["Apples", "Oranges", "Bananas", "Apples", "Oranges", "Bananas"],
#     "Amount": [4, 1, 2, 2, 4, 5],
#     "City": ["SF", "SF", "SF", "Montreal", "Montreal", "Montreal"]
# })

# fig = px.line(df, x="Fruit", y="Amount", color="City")


# app.layout = html.Div(children=[
#     html.H1(children='Hello Dash'),

#     html.Div(children='''
#         Dash: A web application framework for Python.
#     '''),

#     dcc.Graph(
#         id='example-graph',
#         figure=fig
#     )
# ])

# if __name__ == '__main__':
#     app.run_server(debug=True)
