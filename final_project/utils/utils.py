import geopandas
from geopandas import read_file
from luigi import LocalTarget


class ShapeFileTarget(LocalTarget):
    def read_gpd(self, **kwargs):
        return self._read(self.path, **kwargs)

    @classmethod
    def _read(cls, path, **kwargs):
        return read_file(path, **kwargs)

    def write_gpd(self, dataframe, **kwargs):
        return self._write(dataframe, self.path, **kwargs)

    @classmethod
    def _write(cls, dataframe, path, **kwargs):
        return geopandas.GeoDataFrame.to_file(dataframe, path, **kwargs)
