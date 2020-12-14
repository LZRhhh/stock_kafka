from cassandra.cluster import Cluster

key_space = 'stock'
quote_table = 'quotes'
stat_table = 'stats'
session = None


def init_db():
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
    session.execute("drop table if exists %s" % quote_table)
    session.execute(
        "CREATE TABLE %s (symbol text, time timestamp, price float, PRIMARY KEY ("
        "symbol, time))" % quote_table)
    session.execute("drop table if exists %s" % stat_table)
    session.execute(
        "CREATE TABLE %s (symbol text, min float, max float, mean float, PRIMARY KEY ("
        "symbol))" % stat_table)


if __name__ == '__main__':
    init_db()
    # contact_points = ['localhost']
    # cassandra_cluster = Cluster(
    #     contact_points=contact_points  # many servers, using ',' to split them
    # )
    #
    # key_space = 'test'
    # data_table = 'test'
    # session = cassandra_cluster.connect()
    # # session.default_timeout = 30
    # # - CQL
    # # - %s : input keyspace name
    # # - SimpleStrategy : once get first node, clockwise next two as replica
    # # - durable_writes = 'true' : all write in replication done then return
    #
    # session.execute(
    #     "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':'3'} AND "
    #     "durable_writes = 'true'" % key_space)
    # # # - text, timestamp, float are types in CQL
    # session.set_keyspace(key_space)
    # session.execute("drop table if exists %s" % data_table)
    # session.execute(
    #     "CREATE TABLE %s (stock_symbol text, current_price float, PRIMARY KEY ("
    #     "stock_symbol))" % data_table)
    #
    # symbol = 'GOOG'
    # price = 19.80
    # for i in range(10):
    #     statement = "INSERT INTO %s (stock_symbol, current_price) VALUES ('%s', %f)" % (data_table, symbol, price+i)
    #     session.execute(statement)
    #
    # statement = "select * from %s where stock_symbol = '%s'" % (data_table, symbol)
    # res = session.execute(statement)
    # for row in res:
    #     print(row)


