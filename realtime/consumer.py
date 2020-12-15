from datetime import datetime
import datetime as dt
from db import key_space, stat_table

import findspark

findspark.init()

from cassandra.cluster import Cluster
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from stock import get_quote

symbols = ['GOOG', 'AAPL', 'FB', 'AMZN', 'MSFT']


def get_session():
    contact_points = ['localhost']
    cassandra_cluster = Cluster(
        contact_points=contact_points  # many servers, using ',' to split them
    )
    session = cassandra_cluster.connect()
    session.set_keyspace(key_space)
    return session


def split(data):
    symbol, price, time = data.split('\t')
    return symbol, (float(price), time)


# filter out quotes 20min earlier
def is_valid(tuple):
    # print(tuple)
    delta = dt.timedelta(hours=13)
    now = datetime.today() - delta
    today = str(now.date())
    start = datetime.strptime(today + ' 09:30', '%Y-%m-%d %H:%M')
    end = datetime.strptime(today + ' 16:00', '%Y-%m-%d %H:%M')
    if start <= now <= end:
        time = datetime.strptime(tuple[1][1], '%Y-%m-%d %H:%M:%s')
        delta = now - time
        if delta.seconds <= 1200:
            return True
        else:
            return False
    return True


# handle new quotes, renew tmp and compute
def process_stream(rdd, tmp):
    def _time(tuple):
        return tuple[1][1]

    def to_map(rdd):
        arr = rdd.collect()
        map = {}
        for tuple in arr:
            map[tuple[0]] = tuple[1]
        return map

    def get_per(opens, closes):
        per = {}
        for symbol in symbols:
            per[symbol] = (closes[symbol][0] / opens[symbol][0] - 1) * 100
            if per[symbol] >= 0:
                per[symbol] = str(per[symbol]) + '% ↑'
            else:
                per[symbol] = str(per[symbol]) + '% ↓'
        return per

    print('======================', datetime.now().replace(microsecond=0), '=======================')
    queue = tmp[0]
    queue = queue.union(rdd).filter(is_valid)

    # print('quotes:', queue.collect())
    prices = queue.map(lambda x: (x[0], x[1][0]))
    # print(prices.collect())

    # ==========================================================
    # min, max, mean
    # ==========================================================
    min_rdd = prices.reduceByKey(min)
    max_rdd = prices.reduceByKey(max)
    mean_rdd = prices.map(lambda x: (x[0], (x[1], 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda x: (x[0], x[1][0] / x[1][1]))
    mins = to_map(min_rdd)
    maxs = to_map(max_rdd)
    means = to_map(mean_rdd)
    # ==========================================================
    # open, close, incr percentage
    # ==========================================================
    close = queue.reduceByKey(lambda x, y: max(x, y, key=_time))
    open = queue.reduceByKey(lambda x, y: min(x, y, key=_time))
    closes = to_map(close)
    opens = to_map(open)
    pers = get_per(opens, closes)

    # ===========================================================
    print('%-10s%-10s%-10s%-10s%-10s%-10s%-10s' % ('SYMBOL', 'OPEN', 'LOW', 'HIGH', 'CLOSE', 'AVERAGE', 'PER'))
    for symbol in symbols:
        print('%-10s%-10s%-10s%-10s%-10s%-10s%-10s' % (symbol, opens[symbol][0], mins[symbol], maxs[symbol],
                                                       closes[symbol][0], round(means[symbol], 2), pers[symbol]))

        persist_stat(symbol, opens[symbol][0], mins[symbol], maxs[symbol], closes[symbol][0], round(means[symbol], 2),
                     pers[symbol])

    tmp[0] = queue


def persist_stat(symbol, open, min, max, close, mean, per):
    statement = "INSERT INTO %s (symbol, open, min, max, close, mean, per) VALUES ('%s', %f, %f, %f, %f, %f, %f)" \
                % (stat_table, symbol, open, min, max, close, mean, float(per.split('%')[0]))
    session.execute(statement)
    return


# init current quotes
def init():
    print('================ init ================')

    var = []
    for symbol in symbols:
        symbol, price, time = get_quote(symbol)
        var = var + [(symbol, (float(price), time))]
    queue = sc.parallelize(var)
    for tuple in queue.collect():
        print(tuple)
    # print(queue.collect())
    tmp = [queue]
    return tmp


if __name__ == "__main__":
    session = get_session()

    sc = SparkContext(appName="streamingkafka")
    sc_conf = SparkConf()
    sc_conf.set('spark.executor.memory', '2g')  # executor memory是每个节点上占用的内存。每一个节点可使用内存
    sc_conf.set("spark.executor.cores",
                '4')  # spark.executor.cores：顾名思义这个参数是用来指定executor的cpu内核个数，分配更多的内核意味着executor并发能力越强，能够同时执行更多的task
    sc_conf.set('spark.cores.max',
                40)  # spark.cores.max：为一个application分配的最大cpu核心数，如果没有设置这个值默认为spark.deploy.defaultCores
    sc_conf.set('spark.logConf', True)  # 当SparkContext启动时，将有效的SparkConf记录为INFO。
    print(sc_conf.getAll())
    sc.setLogLevel("WARN")  # reduce logs from shell
    window = 60
    ssc = StreamingContext(sc, window)  # get messages of 1 min
    brokers = 'localhost:9092'
    topic = 'test'

    tmp = init()
    #
    kafka_streaming_rdd = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    # kafka_streaming_rdd.pprint()
    lines_rdd = kafka_streaming_rdd.map(lambda x: x[1]).map(split) \
        .foreachRDD(lambda rdd: process_stream(rdd, tmp))

    ssc.start()
    ssc.awaitTermination()
