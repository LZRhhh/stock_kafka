from pyspark import SparkContext

sc = SparkContext(appName="streamingkafka")
sc.setLogLevel("WARN")  # 减少shell打印日志
rdd = sc.textFile('daily_IBM.csv')
rdd = rdd.flatMap(lambda x: x.split(','))
print(rdd.collect())

