#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `final_project` package."""

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
    LocalShapeFiles,
    ShapeFiles,
    StatePopulation,
)
from final_project.processdata import (
    CleanedCovidData,
    CleanedStatePop,
    CondensedShapefile,
    MergedData,
    PopulationStats,
)
from final_project.utils import BokehTarget, LocalShapeFileTarget
from final_project.visualizedata import VisualizedData


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

            # Just Inserted This Task Here For Coverage
            class TestLocalShapeFiles(LocalShapeFiles):
                requires = Requires()
                s3_data = Requirement(TestShapeFiles)
                output = TargetOutput(
                    file_pattern=os.path.join(tmp, "shapefiles"),
                    target_class=LocalShapeFileTarget,
                    ext="",
                )

            class TestCondensedShapeFile(CondensedShapefile):

                requires = Requires()
                shapefile = Requirement(TestLocalShapeFiles)
                output = TargetOutput(
                    file_pattern=os.path.join(target_path, "{task.__class__.__name__}"),
                    target_class=LocalShapeFileTarget,
                    ext="",
                )

            build([TestCondensedShapeFile()], local_scheduler=True)

            actual = gpd.read_file(os.path.join(target_path, "TestCondensedShapeFile"))
            expected = gpd.GeoDataFrame(cleaned_data)

            # Check to see if Data is Clean
            pd.testing.assert_frame_equal(expected, actual)

    @patch("requests.get")
    def test_pop_data(self, API_Response):
        """Test Functionality of Population Data Tasks"""

        with TemporaryDirectory() as tmp:

            # Data to be Cleaned
            data = [
                ["POP", "NAME", "state", "fake_category"],
                ["1234567", "Delaware", "10", "test1"],
                ["1234567", "Hawaii", "15", "test2"],
            ]

            # What Data Should Look Like After Cleaning
            cleaned_data = {
                "POP": [1234567],
                "NAME": ["Delaware"],
                "STATEFP": [10],
            }

            # Immitating Response Object
            class MockedAPIResponse:
                def json(self):
                    return data

            # Mock API Response
            API_Response.return_value = MockedAPIResponse()

            class TestStatePopulation(StatePopulation):
                output = TargetOutput(
                    file_pattern=os.path.join(tmp, "{task.__class__.__name__}/"),
                    ext="",
                    target_class=ParquetTarget,
                    glob="*.parquet",
                )

            class TestCleanedStatePop(CleanedStatePop):

                requires = Requires()
                populations = Requirement(TestStatePopulation)
                output = TargetOutput(
                    file_pattern=os.path.join(tmp, "{task.__class__.__name__}/"),
                    ext="",
                    target_class=ParquetTarget,
                    glob="*.parquet",
                )

            build([TestCleanedStatePop()], local_scheduler=True)

            actual = dd.read_parquet(os.path.join(tmp, "TestCleanedStatePop")).compute()
            expected = pd.DataFrame(cleaned_data)

            print(actual)
            print(expected)

            # Checks to see if the Data Processed is Correct
            pd.testing.assert_frame_equal(expected, actual)

    @patch("requests.get")
    def test_covid_data(self, API_Response):
        """Test Functionality of Covid Data Tasks"""

        with TemporaryDirectory() as tmp:
            api_path = os.path.join(tmp, "covid_data")
            target_path = os.path.join(tmp, "local_covid_data")

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
            df.to_csv(api_path + ".csv")

            # Immitating Response Object
            class MockedAPIResponse:
                text = open(api_path + ".csv").read()

            # Mock API Response
            API_Response.return_value = MockedAPIResponse()

            class TestDailyCovidData(DailyCovidData):
                output = TargetOutput(
                    file_pattern=os.path.join(target_path, "0"),
                    ext=".csv",
                    target_class=LocalTarget,
                )

            class TestDaskFSDailyCovidData(DaskFSDailyCovidData):
                requires = Requires()
                covid_data = Requirement(TestDailyCovidData)

                output = TargetOutput(
                    file_pattern=target_path + "/",
                    ext="",
                    target_class=CSVTarget,
                    flag=None,
                    glob="*.csv",
                )

            build(
                [TestDaskFSDailyCovidData()],
                local_scheduler=True,
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


class MergingTests(TestCase):
    def test_merge(self):
        """Tests Dataframe Merges"""
        with TemporaryDirectory() as tmp:
            # Making Testing File Directory
            geo_file = os.path.join(tmp, "geo")
            pop_file = os.path.join(tmp, "pop")
            covid_file = os.path.join(tmp, "covid")
            os.makedirs(geo_file)
            os.makedirs(pop_file)
            os.makedirs(covid_file)

            # Unmerged Data
            geo_data = {
                "STATEFP": [10],
                "STUSPS": ["DE"],
                "geometry": [Polygon([(0, 0), (1, 1), (1, 0)])],
            }

            pop_data = {
                "STATEFP": [10],
                "NAME": ["Delaware"],
                "POP": [300000],
            }

            covid_data = {
                "date": [
                    pd.to_datetime("2000-1-1"),
                    pd.to_datetime("2000-1-2"),
                ],
                "death": [20, 30],
                "STATEFP": [10, 10],
            }

            # Loading Data
            geo_frame = gpd.GeoDataFrame(geo_data)
            pop_frame = pd.DataFrame(pop_data)
            covid_frame = pd.DataFrame(covid_data)

            # Converting Non Geo Frames to Dask
            pop_frame = dd.from_pandas(pop_frame, npartitions=1)
            covid_frame = dd.from_pandas(covid_frame, npartitions=1)

            # Saving to Files the Be Merged
            geo_frame.to_file(geo_file)
            pop_frame.to_parquet(pop_file)
            covid_frame.to_parquet(covid_file)

            # Luigi Tasks to Grab Test Data
            class TestGeoFile(ExternalTask):
                output = TargetOutput(
                    file_pattern=geo_file,
                    target_class=LocalShapeFileTarget,
                    ext="",
                )

            class TestPopFile(ExternalTask):
                output = TargetOutput(
                    file_pattern=pop_file + "/",
                    ext="",
                    target_class=ParquetTarget,
                    flag=None,
                    glob="*.parquet",
                )

            class TestCovidFile(ExternalTask):
                output = TargetOutput(
                    file_pattern=covid_file + "/",
                    ext="",
                    target_class=ParquetTarget,
                    flag=None,
                    glob="*.parquet",
                )

            # Task to Test
            class TestPopulationStats(PopulationStats):

                requires = Requires()
                covid_data = Requirement(TestCovidFile)
                state_populations = Requirement(TestPopFile)

                output = TargetOutput(
                    file_pattern=os.path.join(tmp, "{task.__class__.__name__}/"),
                    ext="",
                    target_class=ParquetTarget,
                    glob="*.parquet",
                )

            class TestMergedData(MergedData):
                requires = Requires()
                data = Requirement(TestPopulationStats)
                shapefile = Requirement(TestGeoFile)

                output = TargetOutput(
                    file_pattern=os.path.join(tmp, "{task.__class__.__name__}"),
                    target_class=LocalShapeFileTarget,
                    ext="",
                )

            # Running Test Tasks
            build(
                [TestMergedData()],
                local_scheduler=True,
            )

            # Expected Data
            expected_merged_data = {
                "date": [
                    "2000-01-02",
                ],
                "death": [30],
                "STATEFP": [10],
                "NAME": ["Delaware"],
                "POP": [300000],
                "deathsp100": [10.0],
                "STUSPS": ["DE"],
                "geometry": [Polygon([(0, 0), (1, 1), (1, 0)])],
            }

            # Comparing Expected/Actual Dataframes
            actual = gpd.read_file(os.path.join(tmp, "TestMergedData"))
            expected = gpd.GeoDataFrame(expected_merged_data)

            pd.testing.assert_frame_equal(expected, actual)


class VisualizingTests(TestCase):
    def test_visualize(self):
        """Tests Data Visualization Task for Output"""
        with TemporaryDirectory() as tmp:
            merge_folder = os.path.join(tmp, "merged")

            expected_merged_data = {
                "date": [
                    "2000-01-02",
                ],
                "death": [30],
                "STATEFP": [10],
                "NAME": ["Delaware"],
                "POP": [300000],
                "deathsp100": [10.0],
                "STUSPS": ["DE"],
                "geometry": [Polygon([(0, 0), (1, 1), (1, 0)])],
            }

            gdf = gpd.GeoDataFrame(expected_merged_data)
            os.makedirs(merge_folder)
            gdf.to_file(merge_folder)

            class TestMergedData(ExternalTask):
                output = TargetOutput(
                    file_pattern=merge_folder,
                    target_class=LocalShapeFileTarget,
                    ext="",
                )

            class TestVisualizedData(VisualizedData):
                requires = Requires()
                data = Requirement(TestMergedData)
                output = TargetOutput(
                    file_pattern=os.path.join(tmp, "{task.__class__.__name__}"),
                    ext=".html",
                    target_class=BokehTarget,
                )

            build(
                [TestVisualizedData()],
                local_scheduler=True,
            )

            self.assertTrue(
                os.path.exists(os.path.join(tmp, "TestVisualizedData.html"))
            )
