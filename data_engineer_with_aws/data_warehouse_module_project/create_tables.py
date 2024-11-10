import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('/Users/andrekrasinski/Documents/GitHub/DataEngineerAWS/dwh.cfg')

    conn = psycopg2.connect(
        f'host={config.get("DWH","DWH_ENDPOINT")} ' 
        f'dbname={config.get("DWH","DWH_DB")} '
        f'user={config.get("DWH","DWH_DB_USER")} ' 
        f'password={config.get("DWH","DWH_DB_PASSWORD")} ' 
        f'port={config.get("DWH","DWH_PORT")}'
    )

    
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()