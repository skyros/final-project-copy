#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `final_project` package."""

import os
from tempfile import TemporaryDirectory
from unittest import TestCase

import dask
import geopandas as gpd
import pandas as pd
from csci_utils.luigi.dask import CSVTarget, ParquetTarget
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import ExternalTask, build
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
dask
CSVTarget
ParquetTarget
ExternalTask
DailyCovidData
DaskFSDailyCovidData
StatePopulation
CleanedCovidData
CondensedStatePop
MergedData
VisualizedData


class CleaningTests(TestCase):
    def test_shape_data(self):
        """Test Functionality of Covid Data Tasks"""

        with TemporaryDirectory() as tmp:
            data_path = os.path.join(tmp, "test_shapefile")
            target_path = os.path.join(tmp, "output")

            # data to be cleaned
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
