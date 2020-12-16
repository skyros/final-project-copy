import datetime
import os

import dask.dataframe as dd
import pandas as pd
import requests
from csci_utils.luigi.dask.target import CSVTarget, ParquetTarget
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import ExternalTask, LocalTarget, Task

from ..utils import LocalShapeFileTarget, S3ShapeFileTarget


class DailyCovidData(Task):
    """
    Saves a csv from the daily covid tracking API

    Data From The Covid Tracking Project at The Atlantic
    https://covidtracking.com/
    """

    SALT = str(datetime.date.today())
    API_Path = "https://api.covidtracking.com/v1/states/daily.csv"
    SHARED_DIRECTORY = os.path.join("data", SALT, "DailyCovidData")

    output = TargetOutput(
        file_pattern=os.path.join(SHARED_DIRECTORY, "0"),
        ext=".csv",
        target_class=LocalTarget,
    )

    def run(self):
        with self.output().open("w") as f:
            csv_data = requests.get(self.API_Path).text
            f.write(csv_data)


class DaskFSDailyCovidData(Task):
    """A little bit of a hack - Task that reclassifies the existing path as a dask CSVTarget"""

    SALT = str(datetime.date.today())
    SHARED_DIRECTORY = os.path.join("data", SALT, "DailyCovidData")
    requires = Requires()
    covid_data = Requirement(DailyCovidData)

    output = TargetOutput(
        file_pattern=SHARED_DIRECTORY + "/",
        ext="",
        target_class=CSVTarget,
        flag=None,
        glob="*.csv",
    )


class StatePopulation(Task):
    """
    Task that gets state population data

    Data From The US Census Bureau
    https://www.census.gov/
    """

    API_Path = "https://api.census.gov/data/2019/pep/population?get=COUNTY,POP,NAME&for=state:*"
    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}/"),
        ext="",
        target_class=ParquetTarget,
        glob="*.parquet",
    )

    def run(self):
        response = requests.get(self.API_Path)
        df = pd.DataFrame(response.json()[1:], columns=response.json()[0])
        ddf = dd.from_pandas(df, npartitions=1)
        self.output().write_dask(ddf, compression="gzip")


class ShapeFiles(ExternalTask):
    """
    ExternalTask that gets state shapefiles from S3

    Data From The US Census Bureau
    https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html
    """

    S3_PATH = "s3://csci-e-29-skyros-project-data/shapefiles/"

    output = TargetOutput(file_pattern=S3_PATH, ext="", target_class=S3ShapeFileTarget)


class LocalShapeFiles(Task):
    """
    Task That Takes S3 Target and Saves Locally
    This is here to allow the Pipeline to Run Without Access to my S3 Buckets
    """

    requires = Requires()
    s3_data = Requirement(ShapeFiles)
    output = TargetOutput(
        file_pattern="shapefiles",
        target_class=LocalShapeFileTarget,
        ext="",
    )

    def run(self):
        # Reads Input Writes Output
        gdf = self.input()["s3_data"].read_gpd()
        self.output().write_gpd(gdf)
