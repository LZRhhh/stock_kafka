from __future__ import print_function

import json
import sys
import findspark
from operator import add

findspark.init()

from pyspark import SparkContext


def split_count(data):
    symbol, price = data.split('\t')
    return symbol, (float(price), 1)


def split(data):
    symbol, price = data.split('\t')
    return symbol, float(price)


def join(rdd, queue):
    print(queue.join(rdd).collect())


if __name__ == "__main__":
    sc = SparkContext(appName="streamingkafka")
    sc.setLogLevel("WARN")  # 减少shell打印日志
    # 使用streaming使用直连模式消费kafka
    var1 = [('GOOG', 1), ('AAPL', 1)]

    var2 = []
    for i in range(100):
        var2.append(('GOOG', i))
        var2.append(('AAPL', i))
    queue = sc.parallelize(var1)
    new = sc.parallelize(var2)
    rdd = queue.cogroup(new)
    print(rdd.collect())


        # .reduceByKey(lambda x, y: x.extend(y))


