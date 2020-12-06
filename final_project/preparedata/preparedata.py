import os
import json

import geopandas as gpd
from dask.dataframe import from_pandas

from csci_utils.hash_str.hash_str import get_user_id
from csci_utils.luigi.dask import CSVTarget, ParquetTarget
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import BoolParameter, ExternalTask, Parameter, Task, LocalTarget
from bokeh.plotting import figure
from bokeh.io import show, save
from bokeh.models import GeoJSONDataSource, HoverTool, LinearColorMapper
from bokeh.palettes import YlOrRd as palette


class StateShapeFiles(ExternalTask):
    """This task gets shape file from a bucket in s3"""

    ##TODO

    # resolution = Parameter(default='5m')

    # output = TargetOutput(
    #     file_pattern="s3://cscie29-"
    #     + get_user_id("skyros")
    #     + "-project-data/shapefile/",
    #     target_class=CSVTarget,
    #     storage_options={
    #         "requester_pays": True,
    #     },
    #     ext=".shp",
    #     flag=None,
    #     glob="",
    # )


# class DataFile(Task):
#     output = TargetOutput(file_pattern="data",ext="")

#     def run(self):
#         os.mkdir(output.path)


class DailyCovidData(ExternalTask):

    output = TargetOutput(
        file_pattern="data/to_s3_data/all-states-history.csv", ext=".csv"
    )


class StatePopulation(ExternalTask):

    output = TargetOutput(
        file_pattern="data/to_s3_data/nst-est2019-popchg2010_2019", ext=".csv"
    )


class ShapeFiles(ExternalTask):

    output = TargetOutput(file_pattern="data/to_s3_data/cb_2019_us_state_20m", ext="")


class CondensedShapefile(Task):
    """Selects Relevent Shapefile Columns and Saves Locally"""

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
        gdf.to_file(self.output().path)


class GeoJSONFile(Task):
    """Converts GeoDataFrame to JSON"""

    requires = Requires()
    shapefile = Requirement(CondensedShapefile)
    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}"),
        ext=".geojson",
        target_class=LocalTarget,
    )

    def run(self):
        gdf = gpd.read_file(self.input()["shapefile"].path)
        gdf.to_file(self.output().path, driver="GeoJSON")


class VisualizedData(Task):
    """Takes a GeoJSON file and visualizes the data using Bokeh"""

    requires = Requires()
    data = Requirement(GeoJSONFile)
    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}"),
        ext=".html",
        target_class=LocalTarget,
    )

    def run(self):
        gdf = gpd.read_file(self.input()['data'].path)
        gdf = gdf.to_json()

        gdf = GeoJSONDataSource(geojson = gdf)

        p = figure()
        color_mapper = LinearColorMapper(palette = palette[8])
        p.patches(
            source = gdf,
            fill_color = {'field' :'STATEFP', 'transform' : color_mapper}
                )
        hovering = HoverTool()
        hovering.tooltips = [
                            ('State', '@NAME'), 
                            ('Number', '@STATEFP')
                            ]
        p.add_tools(hovering)
        save(p, filename=self.output().path)