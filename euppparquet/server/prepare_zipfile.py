
# --------------------------------------------------------------
# Helper function to get ziped index files
# --------------------------------------------------------------
def prepare_zipfile(url, dir, verbose = True):
    """prepare_zipfile(url, dir, verbose = True)

    Used for development or if the parquet dataset is created
    on a machine which is not the server (no local access to the
    grib index files).

    Uses the basename of the URL and appends 'dir' and '.zip' (file extension)
    which is used to locally store the file. If the file does not already exist
    on disc, this function downloads the GRIB index file, creates a ZIP
    archive, and stores this archive into the directory given on 'dir'.  This
    ZIP file can then be used in combination with the IndexParser to parse the
    content and write the information into the parquet files.

    Parameter
    =========
    url : str
        URL to the GRIB index file to be processed.
    dir : str
        Where to store the zip file. 
    verbose : bool
        Verbosity level, defaults to True.

    Return
    ======
    Returns the name of the zip file on disc (local file name).
    """

    import os
    import requests
    import zipfile
    import tempfile

    local_index = os.path.join(dir, os.path.basename(url))
    local_zip   = os.path.join(dir, os.path.basename(url) + ".zip")
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
