import os

from csci_utils.luigi.dask import ParquetTarget
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import Task

from ..getdata import DaskFSDailyCovidData, ShapeFiles, StatePopulation
from ..utils import ShapeFileTarget


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
        # Select columns
        gdf = self.input()["shapefile"].read_gpd()[
            ["STATEFP", "STUSPS", "NAME", "geometry"]
        ]

        # Select rows only of contiguous states
        gdf = gdf[gdf["STATEFP"].astype(int).isin(contiguous_states)]

        # Save file
        self.output().write_gpd(gdf)


class CondensedStatePop(Task):
    """Selects Relevent State Populations Columns/Rows and Saves Locally"""

    requires = Requires()
    populations = Requirement(StatePopulation)
    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}/"),
        ext="",
        target_class=ParquetTarget,
        glob="*.parquet",
    )

    def run(self):

        # Load Rows for State, State FP, and 2019 Population estimate
        ddf = self.input()["populations"].read_dask(
            usecols=["STATE", "NAME", "POPESTIMATE2019"]
        )

        # Select Only Rows From Contiguous States
        ddf = ddf[ddf["STATE"].isin(contiguous_states)]

        # Save File
        self.output().write_dask(ddf, compression="gzip")


class CleanedCovidData(Task):
    requires = Requires()
    covid_numbers = Requirement(DaskFSDailyCovidData)
    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}/"),
        ext="",
        target_class=ParquetTarget,
        glob="*.parquet",
    )

    def run(self):

        columns = [
            "date",
            "state",
            "death",
            "hospitalizedCurrently",
            "inIcuCurrently",
            "onVentilatorCurrently",
            "fips",
        ]

        numcols = [
            "hospitalizedCurrently",
            "inIcuCurrently",
            "onVentilatorCurrently",
            "death",
        ]

        ddf = self.input()["covid_numbers"].read_dask(
            parse_dates=["date"], usecols=columns, dtype={"death": "float64"}
        )

        ddf = ddf.fillna(value={col: 0 for col in numcols})
        ddf[numcols] = ddf[numcols].astype(int)

        ddf = ddf[ddf["fips"].isin(contiguous_states)]

        out = ddf
        self.output().write_dask(out, compression="gzip")


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
