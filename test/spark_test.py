import findspark

findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def split(data):
    symbol, price = data.split('\t')
    return symbol, float(price)


def join(rdd, tmp):
    queue = tmp[0]
    queue = queue.join(rdd).map(lambda x: (x[0], x[1][0] + x[1][1])).map(lambda x: (x[0], x[1][-60:]))
    print(queue.collect())
    min_rdd = queue.reduceByKey(lambda x, y: min(x, y))
    print(min_rdd.collect())
    max_rdd = queue.reduceByKey(lambda x, y: max(x, y))
    print(max_rdd.collect())
    tmp[0] = queue


if __name__ == "__main__":
    sc = SparkContext(appName="streamingkafka")
    sc.setLogLevel("WARN")  # 减少shell打印日志
    window = 5
    ssc = StreamingContext(sc, window)  # 5秒的计算窗口
    brokers = 'localhost:9092'
    topic = 'test'
    # 使用streaming使用直连模式消费kafka
    init = [0] * 60
    var1 = [('GOOG', init), ('AAPL', init)]
    queue = sc.parallelize(var1)
    tmp = [queue]
    kafka_streaming_rdd = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines_rdd = kafka_streaming_rdd.map(lambda x: x[1]).map(split) \
        .map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).foreachRDD(lambda rdd: join(rdd, tmp))
    # kafka_streaming_rdd.pprint()
    # kafka_streaming_rdd.foreachRDD(lambda rdd: join(rdd, queue))

    ssc.start()
    ssc.awaitTermination()
