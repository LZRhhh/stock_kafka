#!/usr/bin/env python
# coding=utf-8
'''
Author: Jin X
Date: 2020-12-13 21:57:15
LastEditTime: 2020-12-15 15:19:23
'''

from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="name")
brokers = 'localhost:9092'
offset = 0
streamRdd = KafkaUtils.createRDD(sc, {"metadata.broker.list": brokers}, [offset, offset+1])
print(streamRdd.collect())


for offset in range(10):

    streamRdd = KafkaUtils.createRDD(sc, {"metadata.broker.list": brokers}, [offset, offset+1])

    streamRdd.map(lambda x:x[1]).join()

time: t0<t1<t2

map:
    (t1, val1) --> ((t1_t2, val1), (t0_t1, val1))
    (t2, val2) --> ((t2_t3, val2), (t1_t2, val2))

flatmap

reduceBykey:
    (t1_t2) --> val2/val1

map:
    minInterval