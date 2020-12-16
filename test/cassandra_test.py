from cassandra.cluster import Cluster
import pandas as pd
import dash
import dash_table


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

    df = pd.DataFrame()
    statement = "select * from %s where symbol = 'GOOG'" % quote_table
    res = session.execute(statement)
    # for row in res:
    #     print(row)
    df = pd.DataFrame(list(res))
    df = df.round(2)
    print(df)
    # app = dash.Dash(__name__)
    #
    # app.layout = dash_table.DataTable(
    #     id='table',
    #     columns=[{"name": i, "id": i} for i in df.columns],
    #     data=df.to_dict('records'),
    #     style_table={'minWidth': '50%'}
    # )
    # app.run_server(debug=True)
    # sc = SparkContext()  # 连接spark
    #
    # sqlContest = SQLContext(sc)  # 连接sparksql
    #
    # spark_df = sqlContest.createDataFrame(df)  # pandas dataframe转为sparksql dataframe
    #
    # print(spark_df.rdd.collect())
    # statement = "select * from %s" % stat_table
    # res = session.execute(statement)
    # print('test')
    # for row in res:
    #     print(row)
