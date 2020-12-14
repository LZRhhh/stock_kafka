import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from stock_kafka.realtime.stock import get_quote

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
# Assign a topic
topic = 'test'
symbol = 'GOOG'


def test():
    print('begin')
    try:
        while True:
            price = get_quote(symbol)
            line = symbol + '\t' + price
            producer.send(topic, line.encode())
            print("send " + line)
            time.sleep(60)
    except KafkaError as e:
        print(e)
    finally:
        producer.close()
        print('done')


if __name__ == '__main__':
    test()
