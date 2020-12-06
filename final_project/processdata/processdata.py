import os

import geopandas as gpd
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import LocalTarget, Task

from ..getdata import ShapeFiles


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
        gdf = gdf[~gdf["NAME"].isin(["Hawaii", "Alaska", "Puerto Rico"])]
        gdf.to_file(self.output().path)
