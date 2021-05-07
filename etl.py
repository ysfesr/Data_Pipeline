import psycopg2
import configparser
from sql_queries import copy_tables_queries, insert_tables_queries

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ENDPOINT = config.get("CLUSTER", "ENDPOINT")
DBNAME = config.get("CLUSTER", "DBNAME")
USER = config.get("CLUSTER", "USER")
PASSWORD = config.get("CLUSTER", "PASSWORD")
PORT = config.get("CLUSTER", "PORT")


def load_staging_tables(cur, conn):
    for query in copy_tables_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    for query in insert_tables_queries:
        cur.execute(query)
        conn.commit() 

if __name__ == "__main__":

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(ENDPOINT, DBNAME, USER, PASSWORD, PORT))
    cur = conn.cursor()

    load_staging_tables(cur, conn)

    insert_tables(cur, conn)
    
    conn.close()