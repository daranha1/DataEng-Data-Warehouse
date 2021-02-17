import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table (if it exists) using the queries in drop_table_queries list in sql_queries.py
    """
    for currQuery in drop_table_queries:
        print (f'Drop info by {currQuery}')
        cur.execute(currQuery)
        conn.commit()


def create_tables(cur, conn):
    """
    This creates the staging and fact and dimensional tables
    """
    for currQuery in create_table_queries:
        print (f'Created : {currQuery}')
        cur.execute(currQuery)
        conn.commit()
        

def main():
    """
     1. Reads config file to obtain database parameters
     2. Connects to the Redshift cluster
     3. Drops existing tables
     4. Creates tables
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # get data to create connection to Redshift Cluster
    db_host     = config.get('CLUSTER','HOST')
    db_name     = config.get('CLUSTER','DB_NAME')
    db_username = config.get('CLUSTER','DB_USER')
    db_password = config.get('CLUSTER', 'DB_PASSWORD')
    db_port     = config.getint('CLUSTER','DB_PORT')

    print ('db_host : ', db_host, ' db_name : ', db_name,  'db_username : ', db_username, 'db_password : ', db_password,  'db-port : ', db_port)
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    
    """ conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(db_host, db_name, db_username, db_password, db_port)) """
    cur = conn.cursor()

    print ('Create_Tables :Connected to Database ')
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    print ('Create_Tables : Tables Created')
    
    conn.close()
    print ('Create_Tables.py - Connection Closed')

if __name__ == "__main__":
    main()