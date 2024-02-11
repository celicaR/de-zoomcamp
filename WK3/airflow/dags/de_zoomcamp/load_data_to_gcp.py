"""Load data to GCP"""
import pyarrow as pa
import pyarrow.parquet as pq
from pandas import DataFrame


def load_data_from_df_to_gcs(df: DataFrame, root_path: str) -> None:
    """Load dataframe to Google Cloud Storage"""
    table = pa.Table.from_pandas(df)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(table, root_path=root_path, filesystem=gcs)
