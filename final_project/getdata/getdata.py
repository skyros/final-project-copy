import os

import requests
from csci_utils.luigi.dask.target import CSVTarget
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import ExternalTask, LocalTarget, Task

from ..utils import LocalShapeFileTarget


class DailyCovidData(Task):
    """
    Saves a csv from the daily covid tracking API

    Data From The Covid Tracking Project at The Atlantic
    https://covidtracking.com/
    """

    API_Path = "https://api.covidtracking.com/v1/states/daily.csv"
    SHARED_DIRECTORY = os.path.join("data", "DailyCovidData")

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

    SHARED_DIRECTORY = os.path.join("data", "DailyCovidData")
    requires = Requires()
    covid_data = Requirement(DailyCovidData)

    output = TargetOutput(
        file_pattern=SHARED_DIRECTORY + "/",
        ext="",
        target_class=CSVTarget,
        flag=None,
        glob="*.csv",
    )


class StatePopulation(ExternalTask):
    """
    ExternalTask that gets state population data

    Data From The US Census Bureau
    https://www.census.gov/newsroom/press-kits/2019/national-state-estimates.html
    """

    output = TargetOutput(
        file_pattern="data_s3/state_data/",
        ext="",
        target_class=CSVTarget,
        flag=None,
        glob="*.csv",
    )


class ShapeFiles(ExternalTask):
    """
    ExternalTask that gets state shapefiles

    Data From The US Census Bureau
    https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html
    """

    output = TargetOutput(
        file_pattern="data_s3/cb_2019_us_state_20m/",
        ext="",
        target_class=LocalShapeFileTarget,
    )
