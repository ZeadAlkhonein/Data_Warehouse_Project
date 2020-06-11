import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries,time_converter


def load_staging_tables(cur, conn):
    """ 
    Takes crusor of the Redshift and connection. 
    copy s3 objects to insert into the staging tables
    
    Parameters: 
    cur: crusor of the DB
    filepath: path of file
    Returns: 
    nothing 
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
        """ 
    Takes crusor of the Redshift and connection. 
    insert data from the staging table into the tables of the star schema
    Parameters: 
    cur: crusor of the DB
    filepath: path of file
    Returns: 
    nothing 
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        
def convert_time(cur, conn):
    """ 
    Takes crusor of the Redshift and connection. 
    convert timestamp of the staging table 
    
    Parameters: 
    cur: crusor of the DB
    filepath: path of file
    Returns: 
    nothing 
    """
    
    cur.execute(time_converter)
    conn.commit()
    
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("LOADING STAGING\n")
    load_staging_tables(cur, conn)
    print("finish STAGING\n")
    print("convert time")
    convert_time(cur, conn)
    print("finish convert time\n")
    print("inserting tables\n")
    insert_tables(cur, conn)
    print("finish inserting tables\n")


    conn.close()



if __name__ == "__main__":
    main()