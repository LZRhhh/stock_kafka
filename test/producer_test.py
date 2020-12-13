import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from stock_kafka.stock import get_quote, get_quotes

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
# Assign a topic
topic = 'test'
symbol = 'GOOG'
symbols = ['GOOG', 'AAPL']


def test1():
    print('begin')
    try:
        for i in range(1000):
            price = get_quote(symbol)
            line = symbol + '\t' + price
            producer.send(topic, line.encode())
            print("send " + line)
            time.sleep(1)
    except KafkaError as e:
        print(e)
    finally:
        producer.close()
        print('done')


def test2():
    print('begin')
    try:
        for i in range(1000):
            quotes = get_quotes(symbols)
            for line in quotes:
                producer.send(topic, line.encode())
                print("send " + line)
            time.sleep(1)
    except KafkaError as e:
        print(e)
    finally:
        producer.close()
        print('done')


if __name__ == '__main__':
    test2()
