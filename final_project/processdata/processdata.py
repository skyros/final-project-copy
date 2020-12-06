import os

import geopandas as gpd
from csci_utils.luigi.dask import ParquetTarget
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import LocalTarget, Task

from ..getdata import DailyCovidData, ShapeFiles, StatePopulation


class CondensedShapefile(Task):
    """Selects Relevent Shapefile Columns/Rows and Saves Locally"""

    requires = Requires()
    shapefile = Requirement(ShapeFiles)
    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}"),
        target_class=LocalTarget,
        ext="",
    )

    def run(self):
        gdf = gpd.read_file(self.input()["shapefile"].path)[
            ["STATEFP", "NAME", "geometry"]
        ]
        gdf = gdf[gdf["STATEFP"].isin(contiguous_states)]
        gdf.to_file(self.output().path)


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
        ddf = self.input()["populations"].read_dask(
            usecols=["STATE", "NAME", "POPESTIMATE2019"]
        )
        ddf = ddf[ddf["STATE"].isin(contiguous_states)]
        self.output().write_dask(ddf, compression="gzip")


class CleanedCovidData(Task):
    requires = Requires()
    populations = Requirement(DailyCovidData)


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
