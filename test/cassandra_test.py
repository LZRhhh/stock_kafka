from cassandra.cluster import Cluster


def init_session():
    contact_points = ['localhost']
    cassandra_cluster = Cluster(
        contact_points=contact_points  # many servers, using ',' to split them
    )
    session = cassandra_cluster.connect()
    return session


if __name__ == '__main__':
    contact_points = ['localhost']
    cassandra_cluster = Cluster(
        contact_points=contact_points  # many servers, using ',' to split them
    )

    key_space = 'stock'
    quote_table = 'quotes'
    stat_table = 'stats'
    session = cassandra_cluster.connect()
    session.set_keyspace(key_space)
    statement = "select * from %s" % quote_table
    res = session.execute(statement)
    for row in res:
        print(row)

    statement = "select * from %s" % stat_table
    res = session.execute(statement)
    print('test')
    for row in res:
        print(row)


