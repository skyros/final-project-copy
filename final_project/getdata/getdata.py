import os

import requests
from csci_utils.luigi.dask.target import CSVTarget
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import ExternalTask, LocalTarget, Task

from ..utils import ShapeFileTarget

covid_path = os.path.join("data", "DailyCovidData")


class DailyCovidData(Task):
    """Saves a csv from the daily covid tracking API"""

    API_Path = "https://api.covidtracking.com/v1/states/daily.csv"

    output = TargetOutput(
        file_pattern=os.path.join(covid_path, "0"),
        ext=".csv",
        target_class=LocalTarget,
    )

    def run(self):
        with self.output().open("w") as f:
            csv_data = requests.get(self.API_Path).text
            f.write(csv_data)


class DaskFSDailyCovidData(Task):
    """A little bit of a hack - Task that reclassifies the existing path as a dask CSVTarget"""

    requires = Requires()
    covid_data = Requirement(DailyCovidData)

    output = TargetOutput(
        file_pattern=covid_path + "/",
        ext="",
        target_class=CSVTarget,
        flag=None,
        glob="*.csv",
    )


class StatePopulation(ExternalTask):
    """ExternalTask that gets state population data"""

    output = TargetOutput(
        file_pattern="data/to_s3_data/state_data/",
        ext="",
        target_class=CSVTarget,
        flag=None,
        glob="*.csv",
    )


class ShapeFiles(ExternalTask):
    """ExternalTask that gets state shapefiles"""

    output = TargetOutput(
        file_pattern="data/to_s3_data/cb_2019_us_state_20m",
        ext="",
        target_class=ShapeFileTarget,
    )
