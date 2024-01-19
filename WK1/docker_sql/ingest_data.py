"""DE ZOOMCAMP INGEST DATA MODULE"""
import os
import sys
import gzip
import argparse
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm


def chunker(seq, size):
    """Function to generate chunks of the DataFrame"""
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def progress(engine, df, table_name):
    """Insert the DataFrame in chunks"""
    # Set the chunk size
    chunk_size = 10000  # Adjust this value based on your requirements

    # Get the total number of chunks
    total_chunks = len(df) // chunk_size + (len(df) % chunk_size > 0)

    # Create a progress bar
    progress_bar = tqdm(total=total_chunks, desc="Inserting data", unit="chunk")

    for i, chunk in enumerate(chunker(df, chunk_size)):
        replace = "replace" if i == 0 else "append"

        # Check if the chunk contains any new data
        # if i == 0 or chunk.isin(existing_data).sum().sum() < len(chunk):
        # Insert the chunk into the SQL Server table
        chunk.to_sql(name=table_name, con=engine, if_exists=replace, index=False)

        # Update the progress bar
        progress_bar.update()

    # Close the progress bar
    progress_bar.close()

    # Print a message when the insertion is complete
    print("Data insertion complete.")


def main(params):
    """Main function"""
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    file_extension = os.path.splitext(url)[1]

    output_file = None
    if file_extension == ".gz":
        output_file = "output.csv.gz"

        os.system(f"wget {url} -O {output_file}")

        with gzip.open(output_file) as file_in:
            df = pd.read_csv(file_in)
        print(df.dtypes)
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
        print(df.dtypes)
        print("CSV.GZ file read.")
    elif file_extension == ".csv":
        output_file = "output.csv"

        os.system(f"wget {url} -O {output_file}")
        df = pd.read_csv(output_file)
        print(df.head())

        print("CSV file read.")
    elif file_extension == ".parquet":
        output_file = "output.parquet"
        os.system(f"wget {url} -O {output_file}")
        df = pd.read_parquet(output_file)
        print("Parquet file read.")
    else:
        print("Unknown file type.")
        sys.exit()

    # Create the SQLAlchemy engine
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    progress(engine, df, table_name)

    os.system(f"rm {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest PARQUET data to Postgres")

    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument("--table_name", help="name of the table")
    parser.add_argument("--url", help="url of the parquet file")

    args = parser.parse_args()

    print(args)

    main(args)
