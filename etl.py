import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''Copies data from S3 bucket and loads it into staging tables on Redshift'''
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''Inserts data from staging tables to dimension tables'''
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    '''Reads credentials from dwh.cfg, connects to a Redshift database, extracts data from S3 and loads it into staging tables, transfers data from staging tables and loads it into dimension tables'''
    config = configparser.ConfigParser(interpolation=None)
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Loading staging tables.')
    load_staging_tables(cur, conn)
    print('Staging tables complete.')
    
    print('Inserting data into tables.')
    insert_tables(cur, conn)
    print('Data insert complete.')

    conn.close()


if __name__ == "__main__":
    main()