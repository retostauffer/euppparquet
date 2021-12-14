




# --------------------------------------------------------------
# Helper function to get ziped index files
# --------------------------------------------------------------
def prepare_zipfile(url, DATADIR, verbose = True):

    import os
    import requests
    import zipfile
    import tempfile

    local_index = os.path.join(DATADIR, os.path.basename(url))
    local_zip   = os.path.join(DATADIR, os.path.basename(url) + ".zip")
    if not os.path.isfile(local_zip):
        if verbose: print(f"Downloading {url}\nCreating {local_zip}")
        req = requests.get(url)
        if not req.status_code == 200:
            raise Exception("Got 404 for {url}")
        with open(local_index, "w") as fid: fid.write(req.text)

        # Create zip file
        zip = zipfile.ZipFile(local_zip, "w", zipfile.ZIP_DEFLATED)
        zip.write(local_index, os.path.basename(local_index))
        zip.close()
        os.remove(local_index)

    return local_zip

# --------------------------------------------------------------
# Helper function processing files
# --------------------------------------------------------------
def process_files(urls, DATADIR, verbose = True, nrows = None):

    from IndexParser import IndexParser
    parser = IndexParser()
    for url in urls:
        zipfile = prepare_zipfile(url, DATADIR, verbose = verbose)
        parser.process_file(zipfile, verbose = verbose, nrows = nrows)


# --------------------------------------------------------------
# Setting up analysis files/urls and process them
# --------------------------------------------------------------
def process_analysis(BASEURL, DATADIR, years, months, nrows = None):

    # Processing 'analysis'
    urls = []
    #for year in range(1997, 1998):
    for year in years:
        for month in months:
            urls.append(f"{BASEURL}/data/ana/pressure/EU_analysis_pressure_params_{year:04d}-{month:02d}.grb.index")
            urls.append(f"{BASEURL}/data/ana/surf/EU_analysis_surf_params_{year:04d}-{month:02d}.grb.index")

    process_files(urls, DATADIR, nrows = nrows, verbose = True)


# --------------------------------------------------------------
# Forecasts
# --------------------------------------------------------------
def process_forecasts(BASEURL, DATADIR, years, months, version = 0, nrows = None):

    import datetime as dt

    urls = []
    for year in years:
        for month in months:
            ## Control run
            urls.append(f"{BASEURL}/data/fcs/pressure/EU_forecast_ctr_pressure_params_{year:04d}-{month:02d}_{version}.grb.index")
            urls.append(f"{BASEURL}/data/fcs/surf/EU_forecast_ctr_surf_params_{year:04d}-{month:02d}_{version}.grb.index")

            # EFI
            urls.append(f"{BASEURL}/data/fcs/efi/EU_forecast_efi_params_{year:04d}-{month:02d}_{version}.grb.index")

            # Ensemble members
            curr = dt.date(year, month, 1)
            end  = (dt.date(year, month + 1, 1) if month < 12 else dt.date(year + 1, 1, 1)) - dt.timedelta(1)
            while curr <= end:
                date = curr.strftime("%Y-%m-%d")
                # Enemble
                urls.append(f"{BASEURL}/data/fcs/pressure/EU_forecast_ens_pressure_params_{date}_{version}.grb.index")
                urls.append(f"{BASEURL}/data/fcs/surf/EU_forecast_ens_surf_params_{date}_{version}.grb.index")
                curr += dt.timedelta(1)

        process_files(urls, DATADIR, nrows = nrows, verbose = True)


# --------------------------------------------------------------
# Reforecasts
# --------------------------------------------------------------
def process_reforecasts(BASEURL, DATADIR, years, months, version = 0, nrows = None):

    import datetime as dt

    #https://storage.ecmwf.europeanweather.cloud/benchmark-dataset/data/rfcs/surf/EU_reforecast_ctr_surf_params_2018-01_0.grb.index
    #https://storage.ecmwf.europeanweather.cloud/benchmark-dataset/data/rfcs/pressure/EU_reforecast_ctr_pressure_params_2017-02_0.grb.index
    #https://storage.ecmwf.europeanweather.cloud/benchmark-dataset/data/rfcs/surf/EU_reforecast_ens_surf_params_2018-01-01_0.grb.index
    #https://storage.ecmwf.europeanweather.cloud/benchmark-dataset/data/rfcs/pressure/EU_reforecast_ens_pressure_params_2017-06-26_0.grb.index

    urls = []
    for year in years:
        for month in months:
            ## Control run
            urls.append(f"{BASEURL}/data/rfcs/surf/EU_reforecast_ctr_surf_params_{year:04d}-{month:02d}_{version}.grb.index")
            urls.append(f"{BASEURL}/data/rfcs/pressure/EU_reforecast_ctr_pressure_params_{year:04d}-{month:02d}_{version}.grb.index")

            # Ensemble members
            curr = dt.date(year, month, 1)
            end  = (dt.date(year, month + 1, 1) if month <= 12 else dt.date(year + 1, 1, 1)) - dt.timedelta(1)
            while curr <= end:
                # Skip if not Monday (0) or Thursday (3)
                if not  curr.timetuple().tm_wday in [0, 3]:
                    curr += dt.timedelta(1)
                    continue
                date = curr.strftime("%Y-%m-%d")
                # Enemble
                urls.append(f"{BASEURL}/data/rfcs/surf/EU_reforecast_ens_surf_params_{date}_{version}.grb.index")
                urls.append(f"{BASEURL}/data/rfcs/pressure/EU_reforecast_ens_pressure_params_{date}_{version}.grb.index")
                curr += dt.timedelta(1)

        process_files(urls, DATADIR, nrows = nrows, verbose = True)



# --------------------------------------------------------------
# Main script part
# --------------------------------------------------------------
if __name__ == "__main__":


    #obj = Parser()
    #data = obj.parse_file(file, "test")
    BASEURL = "https://storage.ecmwf.europeanweather.cloud/benchmark-dataset"
    DATADIR = "_data"

    ##### TESTING
    ###from IndexParser import IndexParser
    ###parser = IndexParser()
    ###files  = ["_data/EU_forecast_ctr_pressure_params_2017-01_0.grb.index.zip",
    ###          "_data/EU_forecast_ctr_surf_params_2017-01_0.grb.index.zip",
    ###          "_data/EU_forecast_efi_params_2017-01_0.grb.index.zip",
    ###          "_data/EU_forecast_ens_pressure_params_2017-01-01_0.grb.index.zip",
    ###          "_data/EU_forecast_ens_surf_params_2017-01-01_0.grb.index.zip"]
    ###for file in files:
    ###    data = parser.process_file(file, verbose = True)
    ###data   = parser.process_file("_data/EU_forecast_ctr_pressure_params_2017-01_0.grb.index.zip",
    ###        verbose = True)

    ##### TESTING
    ###process_analysis(BASEURL,    DATADIR, [2017], range(1, 13), nrows = 10)
    ###process_forecasts(BASEURL,   DATADIR, [2017], [1], nrows = 10)
    ###process_reforecasts(BASEURL, DATADIR, [2017], [1], nrows = 10)

    ## TESTING
    process_analysis(BASEURL,    DATADIR, [2017], range(1, 13))
    process_forecasts(BASEURL,   DATADIR, [2017], [1])
    process_reforecasts(BASEURL, DATADIR, [2017], [1])





