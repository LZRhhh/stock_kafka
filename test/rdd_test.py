from pyspark import SparkContext

from stock import get_quote

sc = SparkContext(appName="streamingkafka")

symbols = ['GOOG', 'AAPL', 'FB', 'AMZN', 'MSFT']
var = []
for symbol in symbols:
    symbol, price, time = get_quote(symbol)
    for i in range(5):
        var = var + [(symbol, (float(price)+i, time))]


def get_per(opens, closes):
    per = {}
    for symbol in symbols:
        per[symbol] = (closes[symbol][0] / opens[symbol][0] - 1) * 100
        if per[symbol] >= 0:
            per[symbol] = str(per[symbol]) + '↑'
        else:
            per[symbol] = str(per[symbol]) + '↓'
    return per


def _time(tuple):
    return tuple[1][1]


def to_map(rdd):
    arr = rdd.collect()
    map = {}
    for tuple in arr:
        map[tuple[0]] = tuple[1]
    return map

queue = sc.parallelize(var)
# for tuple in queue.collect():
#     print(tuple)
close = queue.reduceByKey(lambda x, y: max(x, y, key=_time))
open = queue.reduceByKey(lambda x, y: min(x, y, key=_time))
closes = to_map(close)
opens = to_map(open)
pers = get_per(opens, closes)

print(pers)
