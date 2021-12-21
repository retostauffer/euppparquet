
from django.conf import settings
from django.shortcuts import render
from django.utils.translation import gettext
from django.shortcuts import HttpResponse

from django.views.defaults import page_not_found

import logging
logger = logging.getLogger("euppparquet.views")
logger.info(" inside {:s}.py".format(__name__))
logger.info("    STATIC_URL:       " + settings.STATIC_URL)
logger.info("    STATIC_ROOT:      " + settings.STATIC_ROOT)
logger.info("    STATICFILES_DIR:  " + settings.STATICFILES_DIRS[0])
logger.info("    BASE_DIR:         " + str(settings.BASE_DIR))


# Main page (index)
def home(request):

    ###context = {"page_title": "Meteo API"}
    context = {"page_title": None}
    return render(request, "home.html", context)


# Main page (index)
def test(request):

    import os
    from glob import glob


    ###context = {"page_title": "Meteo API"}
    context = {"page_title": None}

    context["files"] = glob(os.path.join(settings.PARQUET_DIR, "**", "*"), recursive = True)
    # Getting PARQUET_DIR from settings

    from .api import get_parquet_data

    # Get data as pandas.DataFrame, extract columns and then convert the data to dict
    context["data"]    = get_parquet_data("analysis", 2017, 1, [1, 2], "stl1")
    context["columns"] = [x for x in context["data"].columns]
    #context["columns"] = [dict(title = x) for x in context["data"].columns]
    context["data"]    = context["data"].to_dict(orient = "records")

    return render(request, "test.html", context)
