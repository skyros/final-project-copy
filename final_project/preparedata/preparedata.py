import os

import geopandas
from dask.dataframe import from_pandas

from csci_utils.hash_str.hash_str import get_user_id
from csci_utils.luigi.dask import CSVTarget, ParquetTarget
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import BoolParameter, ExternalTask, Parameter, Task, LocalTarget


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


class TempStateShapeFiles(ExternalTask):

    output = TargetOutput(file_pattern="junk/cb_2019_us_state_20m",ext=".shp")


class ContiguousUSAShapefile(Task):
    """Selects Relevent Shapefile Columns and Saves Locally"""

    requires = Requires()
    shapefile = Requirement(TempStateShapeFiles)
    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}"),
        target_class=LocalTarget,
        ext=""
    )

    def run(self):
        gdf = geopandas.read_file(self.input()["shapefile"].path)[
            ["STATEFP", "NAME", "geometry"]
        ]
        gdf.to_file(self.output().path)


# class GeoJSONFile(Task):

#     requires = Requires()
#     shapefile = Requirement(TempStateShapeFiles)
#     output = TargetOutput(
#         file_pattern=os.path.join("data", "{task.__class__.__name__}"),
#         ext=".geojson",
#         target_class=LocalTarget,
#     )

#     def run(self):
#         os.mkdir(self.output().path)
#         gdf = geopandas.read_file(self.input()["shapefile"].path)[
#             ["STATEFP", "NAME", "geometry"]
#         ]
#         gdf.to_file(self.output().path, driver="GeoJSON")
