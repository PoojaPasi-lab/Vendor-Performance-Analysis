import pandas as pd
import os
import time
from sqlalchemy import create_engine
import logging

# Setup logging
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Create database engine
engine = create_engine('sqlite:///inventory.db')

# Function to ingest data into DB
def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info(f"Table '{table_name}' ingested successfully.")

# Load CSV files from 'data' folder
def load_raw_data():
    start = time.time()
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            file_path = os.path.join('data', file)
            try:
                df = pd.read_csv(file_path)
                logging.info(f'Ingesting {file} into DB...')
                ingest_db(df, file[:-4], engine)
            except Exception as e:
                logging.error(f"Error ingesting {file}: {e}")
    end = time.time()
    total_time = (end - start) / 60
    logging.info('----------Ingestion Complete----------')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')

# Entry point
if __name__ == '__main__':
    load_raw_data()

