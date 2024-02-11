""" de_zoomcamp DAG WK3"""
from datetime import datetime
import os
from pandas import DataFrame
from de_zoomcamp.extract_data_from_url import extract_green_taxi_2022_data_from_api
from de_zoomcamp.transform_data import transform_taxi_data
from de_zoomcamp.load_data_to_gcp import load_data_from_df_to_gcs
from airflow.decorators import dag, task

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "/opt/airflow/dags/keys/cr-de-zoomcamp-411206-336e73249c10.json"
BUCKET_NAME = "cr_demo-bucket"
OBJECT_KEY = "ny_taxi_data.parquet"
TABLE_NAME = "ny_green_taxi_data_uncleansed"
ROOT_PATH = f"{BUCKET_NAME}/{TABLE_NAME}"


@task()
def extract_data_from_url() -> DataFrame:
    """Function to extract data from url"""
    return extract_green_taxi_2022_data_from_api()


@task()
def transform_data(df: DataFrame) -> DataFrame:
    """Function to transform the data"""
    return transform_taxi_data(df)


@task()
def load_data_to_gcs(df: DataFrame) -> None:
    """Function to upload dataframe to Google Cloud Storage"""
    load_data_from_df_to_gcs(df, ROOT_PATH)


# Define your DAG
@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Set your desired schedule interval or None for manual triggering
    catchup=False,  # Set to True if you want historical DAG runs upon creation
    tags=["upload_files_to_gcs_dag"],
)
def upload_files_to_gcs_dag():
    data = extract_data_from_url()
    transformed_data = transform_data(data)
    load_data_to_gcs(transformed_data)


upload_files_to_gcs_dag()
