import os

from csci_utils.luigi.dask import ParquetTarget
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import Task

from ..getdata import DailyCovidData, ShapeFiles, StatePopulation
from ..shapefiletarget import ShapeFileTarget


class CondensedShapefile(Task):
    """Selects Relevent Shapefile Columns/Rows and Saves Locally"""

    requires = Requires()
    shapefile = Requirement(ShapeFiles)
    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}"),
        target_class=ShapeFileTarget,
        ext="",
    )

    def run(self):
        # print(self.input()["shapefile"])
        gdf = self.input()["shapefile"].read_gpd()[
            ["STATEFP", "STUSPS", "NAME", "geometry"]
        ]
        gdf = gdf[gdf["STATEFP"].astype(int).isin(contiguous_states)]
        print(gdf)
        self.output().write_gpd(gdf)


class CondensedStatePop(Task):

    requires = Requires()
    populations = Requirement(StatePopulation)
    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}/"),
        ext="",
        target_class=ParquetTarget,
        glob="*.parquet",
    )

    def run(self):
        print(self.input()["populations"].__dict__)
        ddf = self.input()["populations"].read_dask(
            usecols=["STATE", "NAME", "POPESTIMATE2019"]
        )
        ddf = ddf[ddf["STATE"].isin(contiguous_states)]
        # self.output().write_dask(ddf, compression="gzip")


class CleanedCovidData(Task):
    requires = Requires()
    covid_numbers = Requirement(DailyCovidData)
    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}/"),
        ext="",
        target_class=ParquetTarget,
        glob="*.parquet",
    )

    def run(self):
        ddf = self.input()["covid_numbers"].read_dask()
        print(ddf.columns)


contiguous_states = [
    1,
    4,
    5,
    6,
    8,
    9,
    10,
    12,
    13,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    31,
    32,
    33,
    34,
    35,
    36,
    37,
    38,
    39,
    40,
    41,
    42,
    44,
    45,
    46,
    47,
    48,
    49,
    50,
    51,
    53,
    54,
    55,
    56,
]
