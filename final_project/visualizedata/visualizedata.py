import os

import geopandas as gpd
from bokeh.io import save
from bokeh.models import GeoJSONDataSource, HoverTool, LinearColorMapper
from bokeh.palettes import YlOrRd as palette
from bokeh.plotting import figure
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import LocalTarget, Task

from ..processdata import MergedData


class VisualizedData(Task):
    """Takes a GeoJSON file and visualizes the data using Bokeh"""

    requires = Requires()
    data = Requirement(MergedData)
    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}"),
        ext=".html",
        target_class=LocalTarget,
    )

    def run(self):
        gdf = gpd.read_file(self.input()["data"].path)
        gdf = gdf.to_json()

        gdf = GeoJSONDataSource(geojson=gdf)

        p = figure()
        color_mapper = LinearColorMapper(palette=palette[8])
        p.patches(
            source=gdf, fill_color={"field": "STATEFP", "transform": color_mapper}
        )
        hovering = HoverTool()
        hovering.tooltips = [("State", "@NAME"), ("Number", "@STATEFP")]
        p.add_tools(hovering)
        save(p, filename=self.output().path)
