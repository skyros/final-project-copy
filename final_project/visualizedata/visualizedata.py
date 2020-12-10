import os

from bokeh.io import output_file, save
from bokeh.models import GeoJSONDataSource, HoverTool, LinearColorMapper
from bokeh.palettes import YlOrRd as palette
from bokeh.plotting import figure
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import LocalTarget, Task

from ..processdata import MergedData


class VisualizedData(Task):
    """Takes a GeoPandas shapefile and visualizes the data using Bokeh"""

    sort_by = "deathsp100"

    requires = Requires()
    data = Requirement(MergedData)
    output = TargetOutput(
        file_pattern=os.path.join("data", "{task.__class__.__name__}"),
        ext=".html",
        target_class=LocalTarget,
    )

    def run(self):
        gdf = self.input()["data"].read_gpd()

        low = gdf[self.sort_by].max()
        high = gdf[self.sort_by].min()

        gjdf = gdf.to_json()
        source = GeoJSONDataSource(geojson=gjdf)

        p = figure(
            title="Total Covid Deaths Per 100 Thousand Population",
            x_axis_location=None,
            y_axis_location=None,
        )
        p.grid.grid_line_color = None

        color_mapper = LinearColorMapper(palette=palette[8], low=low, high=high)
        p.patches(
            source=source,
            fill_color={"field": self.sort_by, "transform": color_mapper},
            line_color="black",
        )
        hovering = HoverTool()
        hovering.tooltips = [
            ("State", "@NAME"),
            ("Covid-19 Deaths", "@death"),
            ("Covid-19 Deaths per 100k", "@deathsp100"),
        ]
        p.add_tools(hovering)
        output_file(self.output().path)
        save(p)
