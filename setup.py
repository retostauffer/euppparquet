
##from distutils.command.build import build
from setuptools import setup

ISRELEASED    = False
VERSION       = "0.0.1"
FULLVERSION   = VERSION
write_version = True

# Write version string
def write_version_py(filename=None):
    cnt = """\
version = '%s'
short_version = '%s'
isreleased = %s
"""
    from os import path
    if not filename:
        filename = path.join(path.dirname(__file__), "version.py")

    a = open(filename, "w")
    try:
        a.write(cnt % (FULLVERSION, VERSION, ISRELEASED))
    finally:
        a.close()

if write_version:
    write_version_py()

# Setup

setup(name        = "euppparquet",     # This is the package name
      version     = VERSION,           # Current package version, what else
      description = "European PostProcessing Benchmark Data",
      long_description = "Provides access to GRIB index data for the EUPP project and routines to create the parquet files stored in the cloud used for fast and simple GRIB index data access",
      classifiers = [
        "Development Status :: 3 - Alpha",
        #"Development Status :: 4 - Beta",
        "GNU Lesser General Public License v2 (GPL-2)",
        "Programming Language :: Python :: 3.8",
      ],
      keywords         = "EUPP",
      url              = "https://github.com/retostauffer/euppparquet",
      maintainer       = "Reto Stauffer",
      maintainer_email = "reto.stauffer@uibk.ac.at",
      author           = "Reto Stauffer [aut,cre]",
      license          = "GPL-2",
      install_requires = ["requests", "pandas", "pyarrow"],
      packages         = ["euppparquet"],

      scripts = ["bin/eupp_make_parquet",
                 "bin/eupp_get_parquet"],

      include_package_data = False,
      czip_save        = False)

