from kafka import KafkaProducer
from stock_kafka.realtime.stock import get_quote, get_quotes
from cassandra.cluster import Cluster
from apscheduler.schedulers.background import BlockingScheduler  # for catch data every second
from stock_kafka.realtime.db import key_space, quote_table, init_db


def get_session():
    contact_points = ['localhost']
    cassandra_cluster = Cluster(
        contact_points=contact_points  # many servers, using ',' to split them
    )
    session = cassandra_cluster.connect()
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':'3'} AND "
        "durable_writes = 'true'" % key_space)
    # # - text, timestamp, float are types in CQL
    session.set_keyspace(key_space)
    return session


def persist_stock(symbol, price, time):
    statement = "INSERT INTO %s (symbol, price, time) VALUES ('%s', %f, '%s')" % (table, symbol, price, time)
    session.execute(statement)


def fetch_stock(symbol):
    symbol, price, time = get_quote(symbol)
    msg = '\t'.join([symbol, price, time])
    producer.send(topic, msg.encode())
    # print("send", msg)
    persist_stock(symbol, float(price), time)
    print('saved', msg)


def test2(symbols):
    quotes = get_quotes(symbols)
    for line in quotes:
        producer.send(topic, line.encode())
        print("send " + line)


if __name__ == '__main__':
    init_db()
    table = quote_table
    key_space = key_space
    session = get_session()

    schedule = BlockingScheduler()
    schedule.add_executor('threadpool')
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    # Assign a topic
    topic = 'test'
    symbols = ['GOOG', 'AAPL']
    for symbol in symbols:
        schedule.add_job(fetch_stock, 'interval', args=[symbol], seconds=5, id=symbol)  # every second run fetch_price
    schedule.start()
    # test2(symbols)
