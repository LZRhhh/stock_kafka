from datetime import datetime
import datetime as dt

import findspark

findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from stock_kafka.realtime.stock import get_quote

symbols = ['GOOG', 'AAPL']


def split(data):
    symbol, price, time = data.split('\t')
    return symbol, (float(price), time)


def nowtime():
    now = datetime.now()
    hour = (now.hour + 24 - 13) % 24
    nowtime = str(hour) + ':' + str(now.minute)
    return nowtime


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


def join(rdd, tmp):
    print('========', datetime.today(), '=========')
    queue = tmp[0]
    queue = queue.union(rdd).filter(is_valid)
    # print('quotes:', queue.collect())
    prices = queue.map(lambda x: (x[0], x[1][0]))
    # print(prices.collect())
    min_rdd = prices.reduceByKey(min)
    max_rdd = prices.reduceByKey(max)
    mean_rdd = prices.map(lambda x: (x[0], (x[1], 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda x: (x[0], x[1][0] / x[1][1]))
    mins = min_rdd.collect()
    maxs = max_rdd.collect()
    means = mean_rdd.collect()
    print('min:', mins)
    print('max', maxs)
    print('mean', means)
    for i in range(len(symbols)):
        persist_stat(symbols[i], mins[i][1], maxs[i][1], means[i][1])
    tmp[0] = queue


def persist_stat(symbol, min, max, mean):
    print('SAVED:', symbol, min, max, mean)
    return


def init():
    print('======== init =========')

    var = []
    for symbol in symbols:
        symbol, price, time = get_quote(symbol)
        var = var + [(symbol, (float(price), time))] * 10
    queue = sc.parallelize(var)
    print(queue.collect())
    tmp = [queue]
    return tmp


if __name__ == "__main__":
    sc = SparkContext(appName="streamingkafka")
    sc.setLogLevel("WARN")  # 减少shell打印日志
    window = 60
    ssc = StreamingContext(sc, window)  # 60秒的计算窗口
    brokers = 'localhost:9092'
    topic = 'test'
    # 使用streaming使用直连模式消费kafka

    tmp = init()
    # queue = sc.parallelize(var)
    # tmp = [queue]
    #
    kafka_streaming_rdd = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines_rdd = kafka_streaming_rdd.map(lambda x: x[1]).map(split) \
        .foreachRDD(lambda rdd: join(rdd, tmp))

    ssc.start()
    ssc.awaitTermination()
