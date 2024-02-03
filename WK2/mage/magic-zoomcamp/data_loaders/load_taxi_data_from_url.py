import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url_green_taxi_2020_10 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz'
    url_green_taxi_2020_11 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz'
    url_green_taxi_2020_12 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz'

    green_taxi_2020_final_quarter_urls = [url_green_taxi_2020_10, url_green_taxi_2020_11, url_green_taxi_2020_12]

    taxi_dtypes = {
        'VendorID': 'Int64',
        'store_and_fwd_flag': 'str',
        'RatecodeID': 'Int64',
        'PULocationID': 'Int64',
        'DOLocationID': 'Int64',
        'passenger_count': 'Int64',
        'trip_distance': 'float64',
        'fare_amount': 'float64',
        'extra': 'float64',
        'mta_tax': 'float64',
        'tip_amount': 'float64',
        'tolls_amount': 'float64',
        'ehail_fee': 'float64',
        'improvement_surcharge': 'float64',
        'total_amount': 'float64',
        'payment_type': 'float64',
        'trip_type': 'float64',
        'congestion_surcharge': 'float64'
    }

    parse_dates_green_taxi = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    # Initialize an empty list to store the chunked dataframes
    dfs = []

    for file in green_taxi_2020_final_quarter_urls:
        # Read the CSV file in chunks
        reader = pd.read_csv(file, chunksize=100000, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates_green_taxi)  # Adjust the chunksize as per your memory capacity
    
        # Iterate through each chunk and append it to the list
        for chunk in reader:
            dfs.append(chunk)

    # Concatenate all the chunks into a single dataframe
    main_df = pd.concat(dfs, ignore_index=True)

    return main_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'