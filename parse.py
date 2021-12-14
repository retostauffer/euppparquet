




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


if __name__ == "__main__":


    #obj = Parser()
    #data = obj.parse_file(file, "test")
    BASEURL = "https://storage.ecmwf.europeanweather.cloud/benchmark-dataset"
    DATADIR = "_data"

    process_analysis(BASEURL, DATADIR, [2017], range(1, 13))






