import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm

# Function to generate chunks of the DataFrame
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

# Insert the DataFrame in chunks
def progress(engine, df, table_name):
    # Set the chunk size
    chunk_size = 10000  # Adjust this value based on your requirements

    # Get the total number of chunks
    total_chunks = len(df) // chunk_size + (len(df) % chunk_size > 0)

    # Create a progress bar
    progress_bar = tqdm(total=total_chunks, desc='Inserting data', unit='chunk')

    for i, chunk in enumerate(chunker(df, chunk_size)):
        replace = 'replace' if i == 0 else 'append'

        # Check if the chunk contains any new data
        #if i == 0 or chunk.isin(existing_data).sum().sum() < len(chunk):
        # Insert the chunk into the SQL Server table
        chunk.to_sql(name=table_name, con=engine, if_exists=replace, index=False)

        # Update the progress bar
        progress_bar.update()

    # Close the progress bar
    progress_bar.close()

    # Print a message when the insertion is complete
    print('Data insertion complete.')

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_file = 'output.parquet'
    os.system(f'wget {url} -O {parquet_file}')

    df = pd.read_parquet(parquet_file)
    print("Parquet file read.")

    # Create the SQLAlchemy engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    progress(engine, df, table_name)

    ### This timeout when run with docker
    #print(f"Creating {table_name} in the engine.")
    #df.to_sql(name=table_name, con=engine, if_exists='replace')
    #print(f"Table {table_name} created in the engine.")
    os.system(f'rm {parquet_file}')

if __name__ == '__main__':

    parser = argparse.ArgumentParser (description='Ingest PARQUET data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()

    print(args)

    main(args)