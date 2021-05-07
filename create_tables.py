import psycopg2
import configparser
from sql_queries import stagin_tables_queries, drop_tables_queries, dwh_tables_queries

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ENDPOINT = config.get("CLUSTER", "ENDPOINT")
DBNAME = config.get("CLUSTER", "DBNAME")
USER = config.get("CLUSTER", "USER")
PASSWORD = config.get("CLUSTER", "PASSWORD")
PORT = config.get("CLUSTER", "PORT")

def drop_tables(conn, cur):
    for query in drop_tables_queries:
        cur.execute(query)
        conn.commit()

def create_staging_tables(conn, cur):
    for query in stagin_tables_queries:
        cur.execute(query)
        conn.commit()

def create_dwh_tables(conn, cur):
    for query in dwh_tables_queries:
        cur.execute(query)
        conn.commit()

if __name__ == "__main__":
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(ENDPOINT, DBNAME, USER, PASSWORD, PORT
))
    cur = conn.cursor()
    
    drop_tables(conn, cur)
    create_staging_tables(conn, cur)
    create_dwh_tables(conn, cur)

    conn.close()