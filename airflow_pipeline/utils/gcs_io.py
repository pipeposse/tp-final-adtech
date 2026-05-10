"""Helpers de I/O para GCS — leer/escribir CSVs como DataFrames."""
import io

import pandas as pd
from google.cloud import storage


def read_csv(bucket: str, path: str) -> pd.DataFrame:
    client = storage.Client()
    blob = client.bucket(bucket).blob(path)
    return pd.read_csv(io.BytesIO(blob.download_as_bytes()))


def write_csv(df: pd.DataFrame, bucket: str, path: str) -> None:
    client = storage.Client()
    blob = client.bucket(bucket).blob(path)
    blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")
