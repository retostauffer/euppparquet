


import os
from django.conf import settings
from django.shortcuts import HttpResponse

import logging
logger = logging.getLogger("euppparquet.api")

def get_parquet_data(type, year, month, day, param, limit = 1000):
    """


    Return
    ======
    Returns a dictionary if the request exceeds the limit, or a 
    pandas DataFrame with the data.
    """

    if not isinstance(type, str):
        raise TypeError("Input 'type' must be string.")
    def tolist(x, n):
        if isinstance(x, int) or isinstance(x, float):
            return [x]
        elif isinstance(x, list):
            if not all([isinstance(x, int) or isinstance(x, float) for x in x]):
                raise TypeError("{n} must contain integers or floats only.")
        return x
    year  = tolist(year, "year")
    month = tolist(month, "month")
    day   = tolist(day, "day")

    if isinstance(param, str):
        param = [param]
    elif not all([isinstance(x, str) for x in param]):
        raise ValueError("param must contain strings only.")


    import pyarrow.dataset as ds

    dataset = ds.dataset(os.path.join(settings.PARQUET_DIR, f"{type}.parquet"),
                         format = "parquet", partitioning = "hive")

    exp =       (ds.field("year").isin(year))
    exp = exp & (ds.field("month").isin(month))
    exp = exp & (ds.field("day").isin(day))
    exp = exp & (ds.field("param").isin(param))

    scan = dataset.scanner(filter = exp)

    nmsg = scan.count_rows()
    if nmsg > limit:
        result = dict(warning = "Request results in too many messages ({nmsg}).", nmsg = nmsg)
    else:
        result = scan.to_reader().read_pandas()

    return result



def show_parquet_data(request):
    """

    Returns json for bootgrid
    """

    import json
    from .api import get_parquet_data

    data = get_parquet_data("analysis", 2017, 1, [1, 2], "stl1")

    try:
        current = 1 if not request.GET.get("current") else int(request.GET.get("current"))
    except:
        current = 1
    try:
        rowCount = 20 if not request.GET.get("rowCount") else int(request.GET.get("rowCount"))
    except:
        rowCount = 20

    # Generate dictionary.
    res = dict(current  = current,
               rowCount = rowCount,
               rows     = data.to_dict(orient = "records"),
               total    = data.shape[0],
               columns  = [x for x in data.columns])

    return HttpResponse(json.dumps(res), content_type = "application/json") if request else res






