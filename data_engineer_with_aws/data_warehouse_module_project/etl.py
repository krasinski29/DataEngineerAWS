import configparser
import psycopg2
import logging
from sql_queries import copy_table_queries, insert_table_queries

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_staging_tables(cur, conn):
    for i, query in enumerate(copy_table_queries, 1):
        try:
            logging.info(f"Loading staging table {i} of {len(copy_table_queries)}")
            logging.debug(f"Executing query: {query}")
            cur.execute(query)
            conn.commit()
            logging.info(f"Successfully loaded staging table {i}")
        except Exception as e:
            logging.error(f"Error loading staging table {i}")
            logging.error(f"Query: {query}")
            logging.error(f"Error: {str(e)}")
            conn.rollback()
            raise e


def insert_tables(cur, conn):
    for i, query in enumerate(insert_table_queries, 1):
        try:
            logging.info(f"Inserting into table {i} of {len(insert_table_queries)}")
            logging.debug(f"Executing query: {query}")
            cur.execute(query)
            conn.commit()
            logging.info(f"Successfully inserted into table {i}")
        except Exception as e:
            logging.error(f"Error inserting into table {i}")
            logging.error(f"Query: {query}")
            logging.error(f"Error: {str(e)}")
            conn.rollback()
            raise e


def main():
    try:
        logging.info("Starting ETL process")
        
        logging.info("Reading configuration")
        config = configparser.ConfigParser()
        config.read('/Users/andrekrasinski/Documents/GitHub/DataEngineerAWS/dwh.cfg')
        
        logging.info("Connecting to database")
        conn = psycopg2.connect(
            f'host={config.get("DWH","DWH_ENDPOINT")} ' 
            f'dbname={config.get("DWH","DWH_DB")} '
            f'user={config.get("DWH","DWH_DB_USER")} ' 
            f'password={config.get("DWH","DWH_DB_PASSWORD")} ' 
            f'port={config.get("DWH","DWH_PORT")}'
        )
        
        cur = conn.cursor()
        logging.info("Successfully connected to database")
        
        logging.info("Starting to load staging tables")
        load_staging_tables(cur, conn)
        logging.info("Starting to insert into final tables")
        insert_tables(cur, conn)
        

        conn.close()
        logging.info("ETL process completed successfully")

    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        raise e
    
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error("Failed to complete ETL process")
        exit(1)