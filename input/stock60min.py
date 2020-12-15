#!/usr/bin/env python
# coding=utf-8
'''
Author: Jin X
Date: 2020-12-15 13:47:49
LastEditTime: 2020-12-15 16:31:08
'''
# from pyspark import SparkContex

import csv
from itertools import islice


with open("./intraday_1min_GOOG.csv", newline="") as csvfile:
    lines = csv.reader(csvfile, delimiter=',')
    lines = islice(lines, 1, 10)
    lines = [(r[0],float(r[1])) for r in lines]

print(lines)
