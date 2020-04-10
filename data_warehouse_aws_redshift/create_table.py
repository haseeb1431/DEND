import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def dropTables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def createTables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    dwhconfig = configparser.ConfigParser()
    dwhconfig.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *dwhconfig['CLUSTER'].values()))
    cur = conn.cursor()

    dropTables(cur, conn)
    createTables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()