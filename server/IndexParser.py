


import os
import re
import sys
import json

import pandas as pd
import datetime as dt

class IndexParser:

    PARQUET_PARTITIONING_ANALYSIS = ["year", "month", "day"]
    PARQUET_PARTITIONING_FORECAST = ["version", "product", "year", "month", "day"]
    # Used for testing ...
    #PARQUET_PARTITIONING_FORECAST = ["version", "year", "month", "day"]
    #PARQUET_PARTITIONING_FORECAST = ["year", "month", "day", "step"]
    #PARQUET_PARTITIONING_FORECAST = ["year", "month", "day", "param"]

    def _parse_filename_(self, file):
        """_parse_filename_(file)

        For internal use, parses the file naems to extract
        some information. Files must follow as pecific file
        name convention; else will fail.

        Parameters
        ==========
        file : str
            Path to the GRIB index file to be processed.

        Returns
        =======
        Dictionary with datatype, product, and kind.
        """
        from os import path

        # Getting datatype/kind
        mtch = re.match(r"^(EU_)(.*?)(?=(_params)).*\.grb\.index\.zip$", path.basename(file))
        if not mtch:
            raise Exception("Filename '{path.basename(file)}' not in expected format")
        mtch = mtch.groups()[1].split("_")

        # Version
        vers = re.match(".*_([0-9]+)\.grb\.index.*$", path.basename(file))
        vers = int(vers.group(1)) if vers else None

        if len(mtch) == 4:
            res = dict(type = mtch[0], product = mtch[1], kind = mtch[2], version = vers)
        else:
            res = dict(type = mtch[0], product = mtch[1], kind = mtch[1], version = vers)

        # Force re-write 'ctr' to 'ens'
        for n in ["product", "kind"]:
            if res[n] == "ctr": res[n] = "ens"

        return res


    def process_file(self, file, verbose = False, nrows = None):
        """process_file(file, dataset)

        Reads the GRIB index file from the zip file provided on argument 'file'
        and creates a pandas.DataFrame out of it. After few modifications this
        information will be stored in a parquet dataset defined by 'dataset'.

        Parameters
        ==========
        file : str
            Name of the zip file to be parsed. Expecting a zip archive
            of a GRIB index file for now.
        verbose : bool
            Set verbosity level, defaults to False.
        nrows : positive integer
            If set, only nrows rows from each file will be processed (for development only).
        """ 

        # Getting sane
        if not isinstance(file, str):
            raise TypeError("Input 'file' must be string.")
        if not os.path.isfile(file):
            raise TypeError(f"File '{file}' does not exist.")
        if not re.match(".*\.zip$", file):
            raise TypeError(f"File '{file}' is not a zip file as expected.")

        if not isinstance(verbose, bool):
            raise TypeError("Input 'verbose' must be True or False.")
        if not nrows is None and not isinstance(nrows, int):
            raise TypeError("Wrong input on 'nrows'.")
        if isinstance(nrows, int):
            if not nrows > 0:
                raise ValueError("If 'nrows' is set it musbe positive (not {nrows}).")

        # Extracting some information from the file name first
        file_info = self._parse_filename_(file)

        # Open zip file; get file names, and process them one by one (typically
        # there is only one in there!!)
        import zipfile
        if verbose: print(f"Processing {file}")
        try:
            zip = zipfile.ZipFile(file)
        except:
            raise Exception(f"Problems unzipping {file}")

        # Store result
        res = []
        counter = 0

        # Processing files ...
        for filename in zip.namelist():
            if not os.path.isdir(filename):

                for rec in zip.open(filename):

                    res.append(json.loads(rec))
                    counter += 1

                    if not nrows is None and counter >= nrows: break

        # Convert list of dicts to DataFrame
        try:
            data = pd.DataFrame(res)
        except Exception as e:
            raise Exception("Problems creating the pandas.DataFrame")

        # Extracting step end (end of step range)
        def extract_step(x):
            mtch = re.match("^([0-9]+-)?([0-9]+)$", x)
            if not mtch:
                raise Exception(f"Cannot decode step = '{x}'.")
            return int(mtch.group(2))
        data.step = data.step.apply(extract_step)

        # Appending date and time information for parquet partitions
        if file_info["type"] == "analysis":
            data.date     = pd.to_datetime(data.date + " " + data.time, format = "%Y%m%d %H%M")
            data.date     = data.date + pd.to_timedelta(data.step, "hour")
            data["year"]  = data.date.dt.year
            data["month"] = data.date.dt.month
            data["day"]   = data.date.dt.day
            data["hour"]  = data.date.dt.hour
            del data["step"]
            del data["date"]
            del data["time"]
            partition_cols = self.PARQUET_PARTITIONING_ANALYSIS
        else:
            # Manipulate parameter name if we have levelist.
            if "levelist" in data.columns:
                data.param = data.param + data.levelist
                del data["levelist"]

            # In case we have an ensemble but no number we got
            # forecast control run. Set number to 0.
            if not "number" in data.columns:
                data["number"] = 0

            # Adding file/dataset version
            data["version"] = file_info["version"]

            # Adding product. Either ens (ctr gets ens in _parse_filename_)
            # or efi.
            data["product"] = file_info["product"]

            # Manipulate date/time information (model run initialization)
            data.date       = pd.to_datetime(data.date, format = "%Y%m%d")
            data["year"]    = data.date.dt.year
            data["month"]   = data.date.dt.month
            data["day"]     = data.date.dt.day
            del data["date"]
            partition_cols = self.PARQUET_PARTITIONING_FORECAST

        # Dropping a series of columns
        if True:
            for colname in ["domain", "levtype", "class", "type", "stream", "expver",
                            "_leg_number", "_param_id"]:
                if colname in data.columns: del data[colname]

        zip.close()                # Close the file after opening it
        del zip

        # Find all unique paths for the parquet check
        paths = [str(x) for x in data._path.unique()]
        check = self._check_records_by_filename_(f"{file_info['type']}.parquet", paths)
        if not check:
            # Write parquet file
            n_data = data.shape[0]
            if verbose: print(f"    Writing {n_data} entries into parquet '{file_info['type']}'.")
            try:
                data.to_parquet(f"{file_info['type']}.parquet", partition_cols = partition_cols)
            except Exception as e:
                raise Exception(f"Whoops, problem writing parquet data; {e}")
        else:
            if verbose: print(f"    File already processed; don't add it to the parquet file again.")

        # Return the object for testing
        return data


    def _check_records_by_filename_(self, dataset, paths):
        """_check_records_by_filename_(dataset, paths):

        Used internally to check if we have already processed this file (by
        path) to avoid adding the same information twice which will create
        duplicated entries in the parquet file.

        Parameter
        =========
        dataset : str
            Name of the parquet dataset to be checked.
        paths : str or list of str
            Name of the paths being processed; will check if this file
            already exists in the parquet dataset.

        Return
        ======
        Returns None if no entry is found, else a positive integer.
        """

        from os.path import isdir
        import pyarrow.parquet as parquet
        import pandas as pd

        # Only if parquet data set exists
        if isdir(dataset):
            if isinstance(paths, str): paths = [paths]
            if not isinstance(paths, list):
                raise TypeError("Wrong input on 'paths', must be single str or list.")
            for p in paths:
                if not isinstance(p, str):
                    raise TypeError("Wrong input on 'paths', all list entries must be string.")

            # Prepare filter, fetch data, and return either None if not found
            # or a positive integer (number of entries found).
            filters = [("_path", "=", p) for p in paths]
            res = pd.read_parquet(dataset, engine = "pyarrow", filters = filters, columns = ["_path"])

            return None if res.shape[0] == 0 else res.shape[0]

        # Dataset does not yet exist
        return None







