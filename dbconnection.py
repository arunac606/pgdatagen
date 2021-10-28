import psycopg2
from config import config
import pandas as pd

def connectdb():
    conn = None
    try:
        params = config()
        print('Connecting to the Postgresql database...')
        conn = psycopg2.connect(**params)
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def closedb(conn):
    if conn is not None:
        conn.close()
    print('Database connection closed.')

def schemacheck(conn,schema):
    isvalid = True
    query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{}'".format(schema)
    df = pd.read_sql(query, conn)
    if (len(df) > 0):
        return isvalid
    else:
        print('Invalid schema')
        return False
