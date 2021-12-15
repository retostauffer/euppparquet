


import os
import pyarrow.parquet as parquet
import numpy as np
import pandas as pd


if __name__ == "__main__":
    ## Query from dataset; forwarded to pyarrow.parquet.read_table
    #dataset = "analysis"
    #filters = [("month", "=", 1),
    #           ("day", "<", 10),
    #           ("param", "=", "2t")]
    #filters = [("month", "=", 1),
    #           ("day", "<", 10)]
    #filters = [("month", "=", 1),
    #           ("day", "<", 10),
    #           ("hour", "=", 1200),
    #           ("param", "=", "2t")]
    #filters = [("year", "=", 2017),
    #           ("hour", "=", 12),
    #           ("param", "=", "2t")]
    #x = pd.read_parquet(dataset, engine = "pyarrow", filters = filters)
    #print(x.loc[:, ["hour", "param", "year", "month", "day"]])

    #print(x.iloc[1, ])
    #print(x.param)

    #xall = pd.read_parquet(dataset, engine = "pyarrow")
    #print(xall)
    filters = [("year", "=", 2017),
               ("month", "=", 1),
               ("month", "=", 1),
               ("step",  "=", 96),
               ("param", "=", "t700")]
    filters = [("year", ">=", 2017), ("month", ">=", 6), ("day", ">=", 1),
               ("year", "<=", 2017), ("month", "<=", 6), ("day", "<=", 30),
               ("param", "=", "2t")]


    #p = pd.read_parquet("forecast", engine = "pyarrow", filters = filters)
    p = pd.read_parquet("analysis.parquet", engine = "pyarrow", filters = filters)
    for n in ["year", "month", "day", "hour", "step"]:
        if n in p.columns: p[n] = p[n].astype("int")

    for n in p.columns:
        nt = p[n].dtype
        print(f" Column {n}         {nt}")
        #print(p[n].unique())

    p = p.sort_values(["year", "month", "day", "hour"])

    print(p.head())
    print(p.tail())
    #print(p.iloc[1, ])
    print(p.param.unique())
    print(p.shape)



