"""Load data to GCP with partition columns"""
import pyarrow as pa
import pyarrow.parquet as pq
from pandas import DataFrame


def load_data_to_gcs(df: DataFrame, root_path: str) -> None:
    """Load data to Google Cloud Storage"""
    table = pa.Table.from_pandas(df)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table, root_path=root_path, partition_cols=["lpep_pickup_date"], filesystem=gcs
    )
