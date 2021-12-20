



# --------------------------------------------------------------
# Helper function processing files
# --------------------------------------------------------------
def process_files(urls, dir, verbose = True, nrows = None):

    from .IndexParser import IndexParser
    from .prepare_zipfile import prepare_zipfile

    parser = IndexParser()
    for url in urls:
        zipfile = prepare_zipfile(url, dir, verbose = verbose)
        parser.process_file(zipfile, verbose = verbose, nrows = nrows)

# --------------------------------------------------------------
# Helper function; sanity check for prepare_* functions.
# --------------------------------------------------------------
def _check_years_months_(years, months):
    """_check_years_months_(years, months)

    Sanit check for prepare_* functions. Checks that input
    years and months are of the correct form and within the
    correct range (for years 1950-2050 is somewhat random but
    should be OK for a quick check for getting some reasonable inputs).

    Parameters
    ==========
    years : int or list of int
    months : int or list of int

    Returns
    =======
    two lists : Returns two lists of integers for 'years' and 'months'.

    Raises
    ======
    ValueError: If years or month are neither integer nor lists of integers.
    ValueError: If months is out of range 1-12, years out of range 1950-2050.
    """
    if isinstance(years, int): years = [years]
    if not all([isinstance(x, int) for x in years]):
        raise ValueError("Input 'years' must be integers/list of integers.")
    if not all([1949 < x < 2051 for x in years]):
        raise ValueError("Years must be within 1950 - 2050.")
    if isinstance(months, int): months = [months]
    if not all([isinstance(x, int) for x in months]):
        raise ValueError("Input 'months' must be integers/list of integers.")
    if not all([0 < x < 13 for x in months]):
        raise ValueError("Months must be 1, 2, ..., 12.")
    return years, months

# --------------------------------------------------------------
# Setting up analysis files/urls and process them
# --------------------------------------------------------------
def prepare_analysis(baseurl, dir, years, months, nrows = None):
    """prepare_analysis(baseurl, dir, years, months, nrows = None)

    Prepares URLs to the GRIB index files on the server. These
    URLs are then handed over to 'process_files' which itself
    makes use of 'prepare_zipfile'. The latter downloads the
    GRIB index file and stores a local copy in a zip archive
    in the directory 'dir'. This is mainly done to not download
    the same file multiple times during development.

    Todo
    ====
    I may need an option to directly parse the return of
    the request (no need to zip and store in an operational setting).

    Parameters
    ==========
    baseurl : str
        Base URL to the server where the data are stored. The rest
        of the URL is created below.
    dir : str
        Name of a directory on your local machine where the zip
        files should be stored (see prepare_zipfile).
    years : int or list of int
        Which years should be processed?
    month : int or list of int
        Which months should be processed?
    nrows : None or positive int
        Just for development purposes, defaults to None.
        If None the entire GRIB index is porcessed. If set to
        a positive integer, only 'nrows' lines/entries will be
        processed.

    Returns
    =======
    Absolutely nothing.
    """
    from .prepare_parquet import _check_years_months_
    years, months = _check_years_months_(years, months)

    # Processing 'analysis'
    #for year in range(1997, 1998):
    for year in years:
        urls = []
        for month in months:
            urls.append(f"{baseurl}/data/ana/pressure/EU_analysis_pressure_params_{year:04d}-{month:02d}.grb.index")
            urls.append(f"{baseurl}/data/ana/surf/EU_analysis_surf_params_{year:04d}-{month:02d}.grb.index")

        process_files(urls, dir, nrows = nrows, verbose = True)


# --------------------------------------------------------------
# Forecasts
# --------------------------------------------------------------
def prepare_forecast(baseurl, dir, years, months, version = 0, nrows = None):
    """prepare_forecasts(baseurl, dir, years, months, version = 0, nrows = None)

    Processes all forecasts. This includes control run, ensemble, efi, and hr.
    See 'prepare_analysis' for details.

    Parameters
    ==========
    baseurl : str
    dir : str
    years : int or list of int
    month : int or list of int
    version : int
        Defaults to 0, version of the GRIB index files/GRIB files.
    nrows : None or positive int

    Returns
    =======
    Absolutely nothing.
    """
    from .prepare_parquet import _check_years_months_
    years, months = _check_years_months_(years, months)

    import datetime as dt

    for year in years:
        urls = []
        for month in months:
            ## Control run
            urls.append(f"{baseurl}/data/fcs/pressure/EU_forecast_ctr_pressure_params_{year:04d}-{month:02d}_{version}.grb.index")
            urls.append(f"{baseurl}/data/fcs/surf/EU_forecast_ctr_surf_params_{year:04d}-{month:02d}_{version}.grb.index")
            
            # high resolution run
            urls.append(f"{baseurl}/data/fcs/surf/EU_forecast_hr_surf_params_{year:04d}-{month:02d}_{version}.grb.index")

            # EFI
            urls.append(f"{baseurl}/data/fcs/efi/EU_forecast_efi_params_{year:04d}-{month:02d}_{version}.grb.index")
            # High resolution run

            # Ensemble members
            curr = dt.date(year, month, 1)
            end  = (dt.date(year, month + 1, 1) if month < 12 else dt.date(year + 1, 1, 1)) - dt.timedelta(1)
            while curr <= end:
                date = curr.strftime("%Y-%m-%d")
                # Enemble
                urls.append(f"{baseurl}/data/fcs/pressure/EU_forecast_ens_pressure_params_{date}_{version}.grb.index")
                urls.append(f"{baseurl}/data/fcs/surf/EU_forecast_ens_surf_params_{date}_{version}.grb.index")
                curr += dt.timedelta(1)

        process_files(urls, dir, nrows = nrows, verbose = True)


# --------------------------------------------------------------
# Reforecasts
# --------------------------------------------------------------
def prepare_reforecast(baseurl, dir, years, months, version = 0, nrows = None):
    """prepare_forecasts(baseurl, dir, years, months, version = 0, nrows = None)

    Processes all reforecasts (or hindcasts). This includes control run and
    ensemble. See 'prepare_analysis' for details.

    Parameters
    ==========
    baseurl : str
    dir : str
    years : int or list of int
    month : int or list of int
    version : int
        Defaults to 0, version of the GRIB index files/GRIB files.
    nrows : None or positive int

    Returns
    =======
    Absolutely nothing.
    """
    from .prepare_parquet import _check_years_months_
    years, months = _check_years_months_(years, months)

    import datetime as dt

    for year in years:
        urls = []
        for month in months:
            ## Control run
            urls.append(f"{baseurl}/data/rfcs/surf/EU_reforecast_ctr_surf_params_{year:04d}-{month:02d}_{version}.grb.index")
            urls.append(f"{baseurl}/data/rfcs/pressure/EU_reforecast_ctr_pressure_params_{year:04d}-{month:02d}_{version}.grb.index")

            # Ensemble members
            curr = dt.date(year, month, 1)
            end  = (dt.date(year, month + 1, 1) if month < 12 else dt.date(year + 1, 1, 1)) - dt.timedelta(1)
            while curr <= end:
                # Skip if not Monday (0) or Thursday (3)
                if not  curr.timetuple().tm_wday in [0, 3]:
                    curr += dt.timedelta(1)
                    continue
                date = curr.strftime("%Y-%m-%d")
                # Enemble
                urls.append(f"{baseurl}/data/rfcs/surf/EU_reforecast_ens_surf_params_{date}_{version}.grb.index")
                urls.append(f"{baseurl}/data/rfcs/pressure/EU_reforecast_ens_pressure_params_{date}_{version}.grb.index")
                curr += dt.timedelta(1)

        process_files(urls, dir, nrows = nrows, verbose = True)







