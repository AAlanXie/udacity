import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    drop all pre-existing tables
    
    cur: cursor
    conn: Database connection
    
    return none
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    create all tables which will be used
    
    cur: cursor
    conn: Database connection
    
    return none
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Dropping and creating table
    
    return none
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()