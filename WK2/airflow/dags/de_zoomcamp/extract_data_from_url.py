"""Extract data from URL"""
import pandas as pd
from pandas import DataFrame


def extract_green_taxi_2020_last_quarter_data_from_api() -> DataFrame:
    """Extract NY green taxi data for 2020 last quarter from their DE-Zoomcamp API"""
    pre_fix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
    url_green_taxi_2020_10 = f"{pre_fix}green/green_tripdata_2020-10.csv.gz"
    url_green_taxi_2020_11 = f"{pre_fix}green/green_tripdata_2020-11.csv.gz"
    url_green_taxi_2020_12 = f"{pre_fix}green/green_tripdata_2020-12.csv.gz"

    green_taxi_2020_final_quarter_urls = [
        url_green_taxi_2020_10,
        url_green_taxi_2020_11,
        url_green_taxi_2020_12,
    ]

    taxi_dtypes = {
        "VendorID": "Int64",
        "store_and_fwd_flag": "str",
        "RatecodeID": "Int64",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "ehail_fee": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "payment_type": "float64",
        "trip_type": "float64",
        "congestion_surcharge": "float64",
    }

    parse_dates_green_taxi = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]

    # Initialize an empty list to store the chunked dataframes
    dfs = []

    for file in green_taxi_2020_final_quarter_urls:
        # Read the CSV file in chunks
        reader = pd.read_csv(
            file,
            chunksize=100000,
            sep=",",
            compression="gzip",
            dtype=taxi_dtypes,
            parse_dates=parse_dates_green_taxi,
        )  # Adjust the chunksize as per your memory capacity

        # Iterate through each chunk and append it to the list
        for chunk in reader:
            dfs.append(chunk)

    # Concatenate all the chunks into a single dataframe
    main_df = pd.concat(dfs, ignore_index=True)

    return main_df
