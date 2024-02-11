"""Extract data from URL"""
import pandas as pd
from pandas import DataFrame


def extract_green_taxi_2022_data_from_api() -> DataFrame:
    """
    Extract NY green taxi data for 2022
    https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page API
    """
    green_taxi_2022_urls = []
    pre_fix = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-"
    for month in range(1, 13):
        green_taxi_2022_urls.append(f"{pre_fix}{month:02d}.parquet")

    main_df = pd.concat(
        (pd.read_parquet(f, engine="pyarrow") for f in green_taxi_2022_urls)
    )

    return main_df
