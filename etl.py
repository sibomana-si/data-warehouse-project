import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads data from files into the staging tables in the sparkify database.
    
    Args:
        cur (:obj:`cursor`): cursor object from the sparkify database connection.
        conn (:obj:`connection`): connection object to the sparkify database.      
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """inserts data from the staging tables into the analytics tables in the sparkify database.
    
    Args:
        cur (:obj:`cursor`): cursor object from the sparkify database connection.
        conn (:obj:`connection`): connection object to the sparkify database.      
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()