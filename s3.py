


import os
import pyarrow.parquet as parquet
import s3fs
import pandas as pd


if __name__ == "__main__":

    s3 = s3fs.S3FileSystem()
    s3URL = "https://s3.console.aws.amazon.com/s3/buckets/euppparquet-analysis"
    bucket = "s3://euppparquet-analysis"
    filters = [("time", "=", 12), ("time", "=", "6"), ("day", "=", 1)]
    filters = [("time", "=", 12), ("time", "=", "6"), ("day", "=", 1), ("param", "=", "mn2t")]
    data = parquet.ParquetDataset(bucket, filesystem = s3,
            use_legacy_dataset = True, filters = filters).read_pandas().to_pandas()

    print(data)
