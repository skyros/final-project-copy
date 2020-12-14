#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `final_project` package."""

import csv
import os
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
from csci_utils.luigi.dask import CSVTarget, ParquetTarget
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import ExternalTask, LocalTarget, build
from shapely.geometry import Polygon

from final_project.getdata import (
    DailyCovidData,
    DaskFSDailyCovidData,
    ShapeFiles,
    StatePopulation,
)
from final_project.processdata import (
    CleanedCovidData,
    CondensedShapefile,
    CondensedStatePop,
    MergedData,
)
from final_project.utils import LocalShapeFileTarget
from final_project.visualizedata import VisualizedData

# remove, placeholders for later
ExternalTask
MergedData
VisualizedData
csv


class CleaningTests(TestCase):
    def test_shape_data(self):
        """Test Functionality of ShapeFile Tasks"""

        with TemporaryDirectory() as tmp:
            data_path = os.path.join(tmp, "test_shapefile")
            target_path = os.path.join(tmp, "output")

            # Data to be Cleaned
            data = {
                "STATEFP": ["10", "15"],
                "STUSPS": ["DE", "HI"],
                "NAME": ["Delaware", "Hawaii"],
                "geometry": [
                    Polygon([(0, 0), (1, 1), (1, 0)]),
                    Polygon([(0, 0), (1, 1), (1, 0)]),
                ],
                "fake_category": ["test1", "test2"],
            }

            # What Data Should Look Like After Cleaning
            cleaned_data = {
                "STATEFP": [10],
                "STUSPS": ["DE"],
                "NAME": ["Delaware"],
                "geometry": [Polygon([(0, 0), (1, 1), (1, 0)])],
            }

            # Writing Unlcean Data to File
            gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")
            gdf.to_file(data_path)

            class TestShapeFiles(ShapeFiles):
                output = TargetOutput(
                    file_pattern=data_path,
                    ext="",
                    target_class=LocalShapeFileTarget,
                )

            class TestCondensedShapeFile(CondensedShapefile):

                requires = Requires()
                shapefile = Requirement(TestShapeFiles)
                output = TargetOutput(
                    file_pattern=os.path.join(target_path, "{task.__class__.__name__}"),
                    target_class=LocalShapeFileTarget,
                    ext="",
                )

            build([TestCondensedShapeFile()], local_scheduler=True)

            actual = gpd.read_file(os.path.join(target_path, "TestCondensedShapefile"))
            expected = gpd.GeoDataFrame(cleaned_data)

            # Check to see if Data is Clean
            pd.testing.assert_frame_equal(expected, actual)

    def test_pop_data(self):
        """Test Functionality of Population Data Tasks"""

        with TemporaryDirectory() as tmp:
            data_path = os.path.join(tmp, "popdata")

            # Data to be Cleaned
            data = {
                "STATE": ["10", "15"],
                "NAME": ["Delaware", "Hawaii"],
                "POPESTIMATE2019": ["1234567", "1234567"],
                "fake_category": ["test1", "test2"],
            }

            # What Data Should Look Like After Cleaning
            cleaned_data = {
                "STATEFP": [10],
                "NAME": ["Delaware"],
                "POPESTIMATE2019": [1234567],
            }

            # # Writing Unlcean Data to File
            df = pd.DataFrame(data)
            df.to_csv(data_path + ".csv")

            class TestStatePopulation(StatePopulation):
                output = TargetOutput(
                    file_pattern=tmp + "/",
                    ext="",
                    target_class=CSVTarget,
                    flag=None,
                    glob="*.csv",
                )

            class TestCondensedStatePop(CondensedStatePop):

                requires = Requires()
                populations = Requirement(TestStatePopulation)
                output = TargetOutput(
                    file_pattern=os.path.join(tmp, "{task.__class__.__name__}/"),
                    ext="",
                    target_class=ParquetTarget,
                    glob="*.parquet",
                )

            build([TestCondensedStatePop()], local_scheduler=True)

            actual = dd.read_parquet(
                os.path.join(tmp, "TestCondensedStatePop")
            ).compute()
            expected = pd.DataFrame(cleaned_data)

            # Checks to see if the Data Processed is Correct
            pd.testing.assert_frame_equal(expected, actual)

    @patch("requests.get")
    def test_covid_data(self, API_Response):
        """Test Functionality of Covid Data Tasks"""

        with TemporaryDirectory() as tmp:
            data_path = os.path.join(tmp, "covid_data")

            # Data to be Cleaned
            data = {
                "date": [
                    "2000-01-01",
                    "2000-01-01",
                    "2000-01-01",
                    "2000-01-01",
                    "2000-01-01",
                    "2000-01-01",
                ],
                "state": ["DE", "AR", "AL", "AZ", "CA", "HI"],
                "death": ["2000", None, "2000", "2000", "2000", "2000"],
                "hospitalizedCurrently": ["3000", "3000", None, "3000", "3000", "3000"],
                "inIcuCurrently": ["1000", "1000", "1000", None, "1000", "1000"],
                "onVentilatorCurrently": ["500", "500", "500", "500", None, "500"],
                "fips": ["10", "5", "1", "4", "6", "15"],
                "fake_category": ["test1", "test2", "test3", "test4", "test5", "test6"],
            }

            # What Data Should Look Like After Cleaning
            cleaned_data = {
                "date": [
                    pd.to_datetime("2000-1-1"),
                    pd.to_datetime("2000-1-1"),
                    pd.to_datetime("2000-1-1"),
                    pd.to_datetime("2000-1-1"),
                    pd.to_datetime("2000-1-1"),
                ],
                "state": ["DE", "AR", "AL", "AZ", "CA"],
                "death": [2000, 0, 2000, 2000, 2000],
                "hospitalizedCurrently": [3000, 3000, 0, 3000, 3000],
                "inIcuCurrently": [1000, 1000, 1000, 0, 1000],
                "onVentilatorCurrently": [500, 500, 500, 500, 0],
                "STATEFP": [10, 5, 1, 4, 6],
            }

            # Writing Unlcean Data to File
            df = pd.DataFrame(data)
            df.to_csv(data_path + ".csv")

            # Immitating Response Object
            class MockedAPIResponse:
                text = open(data_path + ".csv").read()

            # Mock API Response
            API_Response.return_value = MockedAPIResponse()

            class TestDailyCovidData(DailyCovidData):
                output = TargetOutput(
                    file_pattern=tmp,
                    ext=".csv",
                    target_class=LocalTarget,
                )

            class TestDaskFSDailyCovidData(DaskFSDailyCovidData):
                requires = Requires()
                Requirement(TestDailyCovidData)
                output = TargetOutput(
                    file_pattern=tmp + "/",
                    ext="",
                    target_class=CSVTarget,
                    flag=None,
                    glob="*.csv",
                )

            class TestCleanedCovidData(CleanedCovidData):

                requires = Requires()
                covid_numbers = Requirement(TestDaskFSDailyCovidData)
                output = TargetOutput(
                    file_pattern=os.path.join(tmp, "{task.__class__.__name__}/"),
                    ext="",
                    target_class=ParquetTarget,
                    glob="*.parquet",
                )

            build(
                [TestCleanedCovidData()],
                local_scheduler=True,
            )

            actual = dd.read_parquet(
                os.path.join(tmp, "TestCleanedCovidData")
            ).compute()
            expected = pd.DataFrame(cleaned_data)

            pd.testing.assert_frame_equal(expected, actual)
