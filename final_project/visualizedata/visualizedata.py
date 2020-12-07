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
    """Takes a GeoJSON shapefile and visualizes the data using Bokeh"""

    sort_by = "death"

    requires = Requires()
    data = Requirement(MergedData)
    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}"),
        ext=".html",
        target_class=LocalTarget,
    )

    def run(self):
        gdf = gpd.read_file(self.input()["data"].path)

        low = gdf[self.sort_by].max()
        high = gdf[self.sort_by].min()

        gjdf = gdf.to_json()

        gjdf = GeoJSONDataSource(geojson=gjdf)

        p = figure()
        color_mapper = LinearColorMapper(palette=palette[8], low=low, high=high)
        p.patches(
            source=gjdf, fill_color={"field": self.sort_by, "transform": color_mapper}
        )
        hovering = HoverTool()
        hovering.tooltips = [
            ("State", "@NAME"),
            ("Number", "@STATEFP"),
            ("Covid-19 Deaths", "@death"),
        ]
        p.add_tools(hovering)
        save(p, filename=self.output().path)
