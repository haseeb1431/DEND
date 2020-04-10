import configparser
import psycopg2
import infrastructure_as_code as iac
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print('Loading staging tables')
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    print('insert_tables: ')
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    dwhConfig = configparser.ConfigParser()
    dwhConfig.read('dwh.cfg')

    conn_string = "host={host} dbname={db_name} user={db_user} password={db_password} port={db_port}"
    
    conn = psycopg2.connect(conn_string.format(**dwhConfig['CLUSTER']))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

    
if __name__ == "__main__":
    main()