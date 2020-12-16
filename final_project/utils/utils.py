import os

import geopandas
from bokeh.io import output_file, save
from geopandas import read_file
from luigi import LocalTarget
from luigi.contrib.s3 import S3Target


class ShapeFileTarget:
    """Base Luigi Targets for Shapefiles"""

    def read_gpd(self, **kwargs):
        return read_file(self.path, **kwargs)

    def write_gpd(self, dataframe, **kwargs):
        # Creates a Directory to Write to if it Doesnt Exist
        if os.path.isdir(self.path) is False:
            os.makedirs(self.path)
        return geopandas.GeoDataFrame.to_file(dataframe, self.path, **kwargs)

    # @classmethod
    # def _read(cls, path, **kwargs):
    #     return read_file(path, **kwargs)

    # @classmethod
    # def _write(cls, dataframe, path, **kwargs):
    #     return geopandas.GeoDataFrame.to_file(dataframe, path, **kwargs)


class LocalShapeFileTarget(ShapeFileTarget, LocalTarget):
    """ShapeFileTarget and LocalTarget Mixin"""

    pass


class S3ShapeFileTarget(ShapeFileTarget, S3Target):
    """ShapeFileTarget and S3Target Mixin"""

    pass


class BokehTarget(LocalTarget):
    """Luigi Target For Bokeh (Outputs Only)"""

    def save_bokeh(self, figure, **kwargs):
        output_file(self.path)
        save(figure)
