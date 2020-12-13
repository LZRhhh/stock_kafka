from __future__ import print_function

import json
import sys
import findspark
from operator import add

findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def split_count(data):
    symbol, price = data.split('\t')
    return symbol, (float(price), 1)


def split(data):
    symbol, price = data.split('\t')
    return symbol, float(price)


if __name__ == "__main__":
    sc = SparkContext(appName="streamingkafka")
    sc.setLogLevel("WARN")  # 减少shell打印日志
    ssc = StreamingContext(sc, 10)  # 5秒的计算窗口
    brokers = 'localhost:9092'
    topic = 'test'
    # 使用streaming使用直连模式消费kafka

    kafka_streaming_rdd = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    lines_rdd = kafka_streaming_rdd.map(lambda x: x[1])

    mean_rdd = lines_rdd.map(split_count).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda x: (x[0], x[1][0] / x[1][1]))
    mean_rdd.pprint()
    pair = lines_rdd.map(split)
    min_rdd = pair.reduceByKey(lambda x, y: min(x, y))
    max_rdd = pair.reduceByKey(lambda x, y: max(x, y))

    ssc.start()
    ssc.awaitTermination()
