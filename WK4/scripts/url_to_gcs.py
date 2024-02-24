"""
With thanks and credits to https://github.com/toufeeqt
since this script is based on his help and work located here:
https://github.com/toufeeqt/de-zoomcamp/blob/main/4_ae/help/web_to_gcs.py
"""
import io
import os
import requests
import pandas as pd
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ="/Users/crowley/Documents/secrets/personal-gcp.json/cr-de-zoomcamp-411206-336e73249c10.json"

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "cr_demo-bucket")

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def url_to_gcs(service: str, year: str):
    """
    Extract NY taxi data for the given colour or fhv and year 
    https://d37ci6vzurychx.cloudfront.net/trip-data/
    """
    taxis_urls = []
    pre_fix = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-"

    for month in range(1, 13):
        taxis_urls.append(f"{pre_fix}{month:02d}.parquet")

    for file in taxis_urls:
        print(f"Parquet: {file}")
        # r = requests.get(file)
        df = pd.read_parquet(file, engine="pyarrow")
        file_name = file.replace("https://d37ci6vzurychx.cloudfront.net/trip-data/","")
        df.to_parquet(file_name, engine='pyarrow')
    
        # upload it to gcs 
        upload_to_gcs(BUCKET, f"ny_taxi_data/{service}/{file_name}", file_name)
        print(f"GCS: ny_taxi_data/{service}/{file_name}")

if __name__ == '__main__':

    print(os.getcwd())
    os.chdir('data')

    url_to_gcs("green", "2019")
    url_to_gcs("green", "2020")

    url_to_gcs("fhv", "2019")

    url_to_gcs("yellow", "2019")
    url_to_gcs("yellow", "2020")