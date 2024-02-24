
## Set up

NB: NEED TO MOVE TO AIRFLOW BUT RAN OUT OF TIME

### URL to GPS
Tried with Airflow but kept getting zombie tasks due to the time taking when loading the data from the API to GPS.

In the end ran a python script to do so.

### Loading GPS to BigQuery
``` sql
CREATE EXTERNAL TABLE `cr-de-zoomcamp-411206.ny_taxi.green_tripdata`
OPTIONS (
  uris=['gs://cr_demo-bucket/ny_taxi_data/green/green_tripdata_20*.parquet'],
  format ='PARQUET'
);

CREATE EXTERNAL TABLE `cr-de-zoomcamp-411206.ny_taxi.yellow_tripdata`
OPTIONS (
  uris=['gs://cr_demo-bucket/ny_taxi_data/yellow/yellow_tripdata_20*.parquet'],
  format ='PARQUET'
);

CREATE EXTERNAL TABLE `cr-de-zoomcamp-411206.ny_taxi.fhv_tripdata`
OPTIONS (
  uris=['gs://cr_demo-bucket/ny_taxi_data/fhv/fhv_tripdata_20*.parquet'],
  format ='PARQUET'
);
```

### Models
Trying to generate models - no luck
```
{% set models_to_generate = codegen.get_models(directory='staging', prefix='stg') %}
{{ codegen.generate_model_yaml(model_names = models_to_generate) 
}}

```

===

## Actions taken
- After setting up my dbt account I followed the [dbt <> BigQuery setup](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/dbt_cloud_setup.md)
- I had a few issues with GitHub but once I created in GitHub my branch to use in DBT and disallowed Allow merge commits, it started working
