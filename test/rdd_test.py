from pyspark import SparkContext

from stock import get_quote

sc = SparkContext(appName="streamingkafka")

symbols = ['GOOG', 'AAPL', 'FB', 'AMZN', 'MSFT']
var = []
for symbol in symbols:
    symbol, price, time = get_quote(symbol)
    for i in range(5):
        var = var + [(symbol, (float(price) + i, time))]


def get_per(opens, closes):
    per = {}
    for symbol in symbols:
        per[symbol] = (closes[symbol][0] / opens[symbol][0] - 1) * 100
        if per[symbol] >= 0:
            per[symbol] = str(per[symbol]) + '↑'
        else:
            per[symbol] = str(per[symbol]) + '↓'
    return per


def _time(self):
    return self[1][1]


def to_map(rdd):
    arr = rdd.collect()
    map = {}
    for tuple in arr:
        map[tuple[0]] = tuple[1]
    return map
#
#
def max_time(tuple1, tuple2):
    if tuple1[1][1] < tuple2[1][1]:
        return tuple2
    else:
        return tuple1


#
#
def min_time(tuple1, tuple2):
    if tuple1[1][1] <= tuple2[1][1]:
        return tuple1
    else:
        return tuple2


var = [('AMZN', (3152.48, '2020-12-15 10:43:00')),
       ('AAPL', (125.505, '2020-12-15 10:43:00')),
       ('FB', (271.69, '2020-12-15 10:43:00')),
       ('MSFT', (213.145, '2020-12-15 10:43:00')),
       ('GOOG', (1758.0648, '2020-12-15 10:43:00')),
       ('GOOG', (1758.0648, '2020-12-15 10:43:00')),
       ('AAPL', (125.4393, '2020-12-15 10:43:00')),
       ('AAPL', (125.53, '2020-12-15 10:44:00')),
       ('AMZN', (3152.56, '2020-12-15 10:44:00')),
       ('FB', (271.7281, '2020-12-15 10:44:00')),
       ('MSFT', (213.1532, '2020-12-15 10:44:00')),
       ('GOOG', (1758.3344, '2020-12-15 10:43:00')),
       ('GOOG', (1757.8333, '2020-12-15 10:44:00')),
       ('FB', (271.97, '2020-12-15 10:44:00')),
       ('MSFT', (213.29, '2020-12-15 10:44:00')),
       ('AAPL', (125.58, '2020-12-15 10:44:00')),
       ('AMZN', (3154.9999, '2020-12-15 10:44:00')),
       ('FB', (271.7, '2020-12-15 10:44:00')),
       ('AMZN', (3151.93, '2020-12-15 10:44:00')),
       ('MSFT', (213.155, '2020-12-15 10:44:00'))]
# for tuple in queue.collect():
#     print(tuple)
queue = sc.parallelize(var)
close = queue.reduceByKey(lambda x, y: max([x, y], key=lambda s: s[1]))
open = queue.reduceByKey(lambda x, y: min([x, y], key=lambda s: s[1]))

print(close.collect())
print(open.collect())
closes = to_map(close)
opens = to_map(open)
pers = get_per(opens, closes)

print(pers)
# #
# var = [('AMZN', (3152.48, '2020-12-15 10:43:00')),
#        ('AMZN', (3152.56, '2020-12-15 10:44:00'))]
# tuple = max(var, key=lambda x: x[1][1])
# print(tuple)
# tuple = min(var, key=lambda x: x[1][1])
# print(tuple)
# print('2020-12-15 10:44:00' < '2020-12-15 10:43:00')
