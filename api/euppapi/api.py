



from django.conf import settings
from django.shortcuts import render
from django.utils.translation import gettext
from django.shortcuts import HttpResponse

import os
import json
import pandas as pd
import pyarrow.dataset as ds
from datetime import datetime as dt

import logging
logger = logging.getLogger("euppapi.api")
logger.info(" inside {:s}.py".format(__name__))


def _get_message_parse_args_(request, daterange, analysis = False):

    import re

    # Dictionary to be returned
    note = "EUPP API request issued {:s}".format(dt.now().strftime("%Y-%m-%d %H:%M:%S"))
    res = dict(note = note, error = False)

    # Steps or time (for analysis)
    what = "time" if analysis else "step"
    req_steps = request.GET.get(what)
    if req_steps is None:
        res[what] = None # None? -> all
    elif not re.match("^[0-9:]+$", req_steps):
        res["note"] += "argument ?step wrong format"
        res["error"] = True
    else:
        res[what] = [int(x) for x in re.findall("[0-9]+", req_steps)]

    # Params
    req_param = request.GET.get("param")
    if req_param is None:
        res["param"] = None # None? -> all
    elif not re.match("^[\w:]+$", req_param):
        res["note"] += "argument ?param wrong format"
        res["error"] = True
    else:
        res["param"] = re.findall("[\w]+", req_param)

    # Number
    req_number = request.GET.get("number")
    if req_number is None:
        res["number"] = None # None? -> all
    elif not re.match("^[0-9:]+$", req_number):
        res["note"] += "argument ?number wrong format"
        res["error"] = True
    else:
        res["number"] = [int(x) for x in re.findall("[0-9]+", req_number)]

    # Processing daterange and steprange
    mtch = re.match(r"([0-9]{4}-[0-9]{2}-[0-9]{2})(/[0-9]{4}-[0-9]{2}-[0-9]{2})?", daterange)
    if mtch.group(2) is None:
        res["daterange"] = [mtch.group(1)] * 2
    else:
        res["daterange"] = [x.replace("/", "")  for x in mtch.groups()]

    # Keep as date to be JSON serializable for now
    tmp = []
    for i in range(len(res["daterange"])): tmp.append(res["daterange"][i])
    res["daterange"] = tmp

    return res


# -----------------------------------------------------------
# -----------------------------------------------------------
# -----------------------------------------------------------
# -----------------------------------------------------------
def get_parquet_data(type, daterange, param, limit = 10000, **kwargs):
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

    # Convert input 'daterange' (list of strings) to datetime.date
    print(daterange)
    daterange =  [dt.strptime(x, "%Y-%m-%d").date() for x in daterange]

    # Building up filter expression
    if len(daterange) == 1:
        d0 = daterange[0]
        exp =       ((ds.field("year")  == d0.year) &
                     (ds.field("month") == d0.month) &
                     (ds.field("day")   == d0.day))
    else:
        d0 = daterange[0]; d1 = daterange[1]
        exp =       ((ds.field("year")  >= d0.year) &
                     (ds.field("month") >= d0.month) &
                     (ds.field("day")   >= d0.day))
        exp = exp & ((ds.field("year")  <= d1.year) &
                     (ds.field("month") <= d1.month) &
                     (ds.field("day")   <= d1.day))

    # subsetting steps/time (hours)?
    if "step" in kwargs.keys() and not kwargs["step"] is None:
        exp = exp & (ds.field("step").isin(kwargs["step"]))
    if "number" in kwargs.keys() and not kwargs["number"] is None:
        exp = exp & (ds.field("number").isin(kwargs["number"]))

    # Parameter: if string -> list
    if isinstance(param, str):
        param = [param]
    # Parameter: if list, check that all entries are string
    elif isinstance(param, list) and not all([isinstance(x, str) for x in param]):
        raise ValueError("param must contain strings only.")
    # Parameter: if we have list, it is a list of strings. Append to filter.
    if isinstance(param, list): exp = exp & (ds.field("param").isin(param))


    # Opening parquet data set
    dataset = ds.dataset(os.path.join(settings.PARQUET_DIR, f"{type}.parquet"),
                         format = "parquet", partitioning = "hive")

    # Dataset scanner to check number of messages this request will result in
    scan = dataset.scanner(filter = exp)
    nmsg = scan.count_rows()
    if nmsg > limit:
        result = dict(error = f"Request results in too many messages ({nmsg}).", nmsg = nmsg)
    else:
        result = scan.to_reader().read_pandas()

    if "time" in kwargs.keys() and not kwargs["time"] is None:
        result = result[result.time.isin([100 * x for x in kwargs["time"]])]

    return result

# -----------------------------------------------------------
# -----------------------------------------------------------
# -----------------------------------------------------------
# -----------------------------------------------------------
def get_messages_analysis(request, daterange):
    """get_messages_analysis()

    GET parameters allowed are '?time', '?number', and '?param'.  Single values
    or colon separated lists.  E.g., 'time=0:6:12' will return analysis for time (hour)
    '0', '6', and '12' only. 'param=2t:cp' will subset '2t' and 'cp' only.
    'number' works like 'step'. If not set, all steps/parameters will be
    returned.  In case the format is wrong a JSON array is returned containing
    logical True on 'error'.

    If ?list is set the correspoinding json return will be list-style (handy
    to create a pandas.DataFrame or data.frame in R).

    Parameters
    ==========
    request : django request
    daterange : str
        Format must be '2017-01-01' or '2017-01-01:2017-01-31'.

    Return
    ======
    JSON string.
    """

    # Parsing inputs
    res = _get_message_parse_args_(request, daterange, analysis = True)
    res.update(dict(type = "analysis"))

    # Initializing resulting dictionary
    data = get_parquet_data("analysis", res["daterange"], res["param"],
                            time = res["time"])
    if not isinstance(data, pd.DataFrame):
        res.update(data)
    else:
        orient = "list" if request.GET.get("list") else "records"
        res.update(dict(nmsg = data.shape[0], data = data.to_dict(orient = orient)))

    # Prepare return
    return HttpResponse(json.dumps(res), content_type = "application/json") if request else res


# -----------------------------------------------------------
# -----------------------------------------------------------
# -----------------------------------------------------------
# -----------------------------------------------------------
def get_messages_forecast(request, product, daterange):
    """test()

    GET parameters allowed are '?step', '?number', and '?param'.  Single values
    or colon separated lists.  E.g., 'step=0:6:12' will return forecast steps
    '0', '6', and '12' only. 'param=2t:cp' will subset '2t' and 'cp' only.
    'number' works like 'step'. If not set, all steps/parameters will be
    returned.  In case the format is wrong a JSON array is returned containing
    logical True on 'error'.

    Parameters
    ==========
    request : django request
    product : None (analysis) or str
        E.g., ens, hr
    daterange : str
        Format must be '2017-01-01' or '2017-01-01:2017-01-31'.

    Return
    ======
    JSON string.
    """

    # Parsing inputs
    res = _get_message_parse_args_(request, daterange, analysis = False)
    res.update(dict(type = "forecast"))

    print(res)

    # Initializing resulting dictionary
    data = get_parquet_data("forecast", res["daterange"], res["param"],
                            step = res["step"], number = res["number"])
    if not isinstance(data, pd.DataFrame):
        res.update(data)
    else:
        orient = "list" if request.GET.get("list") else "records"
        res.update(dict(data = data.to_dict(orient = orient), nmsg = data.shape[0]))

    # Prepare return
    return HttpResponse(json.dumps(res), content_type = "application/json") if request else res





#import os
#from django.conf import settings
#from django.shortcuts import HttpResponse
#
#import logging
#logger = logging.getLogger("euppparquet.api")
#
#
#
#
#def show_parquet_data(request):
#    """
#
#    Returns json for bootgrid
#    """
#
#    import json
#    import pandas as pd
#    from .api import get_parquet_data
#
#    #data = get_parquet_data("analysis", 2017, 1, [1, 2], "stl1")
#    #data = get_parquet_data("analysis", 2017, range(13), range(32), "stl1")
#    data = get_parquet_data("analysis", 2017, 1, range(32), "stl1")
#
#    # Generate dictionary.
#    if isinstance(data, pd.DataFrame):
#        res = dict(total    = data.shape[0],
#                   data     = data.to_dict(orient = "records"),
#                   columns  = [x for x in data.columns],
#                   DATA_BASE_URL = settings.DATA_BASE_URL)
#    else:
#        res = data
#
#    return HttpResponse(json.dumps(res), content_type = "application/json") if request else res



















