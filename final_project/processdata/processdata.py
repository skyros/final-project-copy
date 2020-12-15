import os

import geopandas as gp
import pandas as pd
from csci_utils.luigi.dask import ParquetTarget
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import Task

from ..getdata import DaskFSDailyCovidData, ShapeFiles, StatePopulation
from ..utils import LocalShapeFileTarget


class CondensedShapefile(Task):
    """Selects Relevent Shapefile Columns/Rows and Saves Locally"""

    requires = Requires()
    shapefile = Requirement(ShapeFiles)
    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}"),
        target_class=LocalShapeFileTarget,
        ext="",
    )

    def run(self):
        # Select columns
        gdf = self.input()["shapefile"].read_gpd()[
            ["STATEFP", "STUSPS", "NAME", "geometry"]
        ]

        # Select rows only of contiguous states
        gdf = gdf[gdf["STATEFP"].astype(int).isin(contiguous_states)]

        # Convert StateFP to int
        gdf[["STATEFP"]] = gdf[["STATEFP"]].astype(int)

        # Save file
        self.output().write_gpd(gdf)


class CleanedStatePop(Task):
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

        numcols = ["POP", "state"]

        # Load Rows for State, State FP, and 2019 Population estimate
        ddf = self.input()["populations"].read_dask(columns=["POP", "NAME", "state"])

        # Changes numcols to ints
        ddf[numcols] = ddf[numcols].astype(int)

        # Select Only Rows From Contiguous States
        ddf = ddf[ddf["state"].isin(contiguous_states)]

        # Rename Column for Merging
        ddf = ddf.rename(columns={"state": "STATEFP"})

        # Save File
        self.output().write_dask(ddf, compression="gzip")


class CleanedCovidData(Task):
    """Selects Relevant Rows and Cleans Covid Data"""

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

        # Fill numerical NAs and convert to int
        ddf = ddf.fillna(value={col: 0 for col in numcols})
        ddf[numcols] = ddf[numcols].astype(int)

        # Select only contiguous states
        ddf = ddf[ddf["fips"].isin(contiguous_states)]

        # Rename fips
        ddf = ddf.rename(columns={"fips": "STATEFP"})

        self.output().write_dask(ddf, compression="gzip")


class PopulationStats(Task):
    """Normalizes Statistics to State Population"""

    requires = Requires()
    covid_data = Requirement(CleanedCovidData)
    state_populations = Requirement(CleanedStatePop)

    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}/"),
        ext="",
        target_class=ParquetTarget,
        glob="*.parquet",
    )

    def run(self):
        # Load DataFrames
        covid_df = self.input()["covid_data"].read_dask()
        pop_df = self.input()["state_populations"].read_dask()

        # Merge DataFrames
        ddf = covid_df.merge(pop_df)

        # Calculate New Statistic for Deaths per 100 thousand people
        ddf["deathsp100k"] = ddf.apply(
            lambda x: x["death"] / x["POP"] * 100000,
            axis=1,
            meta=(None, "float64"),
        )

        # Save DataFrame
        self.output().write_dask(ddf, compression="gzip")


class MergedData(Task):
    """Merges the normalized statistics with the GeoPandas shapefile"""

    requires = Requires()
    data = Requirement(PopulationStats)
    shapefile = Requirement(CondensedShapefile)

    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}"),
        target_class=LocalShapeFileTarget,
        ext="",
    )

    def run(self):
        # Load DataFrames
        gdf = self.input()["shapefile"].read_gpd()
        ddf = self.input()["data"].read_dask()

        # Take only the most recent date
        ddf = ddf[ddf["date"] == ddf["date"].max()]

        # Unfortunately have to drop to Pandas here because dask does not play well with geopandas
        # However, most of the dataframe has been reduced so we have cut down on compute time
        pdf = ddf.compute()

        # Merge Dataframes
        df = pd.merge(pdf, gdf)

        # Converting date back to str as geopandas had trouble writing
        df[["date"]] = df[["date"]].astype(str)

        # Converting dataframe to geodataframe
        gdf = gp.GeoDataFrame(df)

        # Save Geodataframe
        self.output().write_gpd(gdf)


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
