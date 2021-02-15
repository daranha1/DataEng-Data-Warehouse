import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads tables from S3 buckets to Redshift using queries in 
    copy_table_queries in sql_queries.py
    """
    for currQuery in copy_table_queries:
        print (f'Copy : {currQuery}')
        cur.execute(currQuery)
        conn.commit()


def insert_tables(cur, conn):
    for currQuery in insert_table_queries:
        print (f'Transform data by {currQuery}')
        cur.execute(currQuery)
        conn.commit()

def main():
    """
    The main function
    1. Connects to Redshift
    2. Loads tables from Amazon S3 to amazon redshift
    3. Inserts from staging tables to the fact and dimension tables
    4. Closes the connection to Redshift
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print ("Connect To Redshift ...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    """    
      print ("Load Staging Tables ...")
      load_staging_tables(cur, conn)  
    """

    print ("Transform Staging Tables")  
    insert_tables(cur, conn)

    conn.close()
    print ("Ending the ETL process")

if __name__ == "__main__":
    main()