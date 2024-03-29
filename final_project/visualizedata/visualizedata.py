import os

from bokeh.models import GeoJSONDataSource, HoverTool, LinearColorMapper
from bokeh.palettes import YlOrRd as palette
from bokeh.plotting import figure
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from luigi import Task

from ..processdata import MergedData
from ..utils import BokehTarget, Salter


class VisualizedData(Task):
    """Takes a GeoPandas shapefile and visualizes the data using Bokeh"""

    sort_by = "deathsp100"
    salter = Salter()
    SALT = salter.date_salt()

    requires = Requires()
    data = Requirement(MergedData)
    output = TargetOutput(
        file_pattern=os.path.join("{task.__class__.__name__}-" + SALT),
        ext=".html",
        target_class=BokehTarget,
    )

    def run(self):
        gdf = self.input()["data"].read_gpd()

        low = gdf[self.sort_by].max()
        high = gdf[self.sort_by].min()

        gjdf = gdf.to_json()
        source = GeoJSONDataSource(geojson=gjdf)

        fig = figure(
            title="Total Covid Deaths Per 100 Thousand Population",
            x_axis_location=None,
            y_axis_location=None,
            plot_width=1000,
            plot_height=600,
        )
        fig.grid.grid_line_color = None

        color_mapper = LinearColorMapper(palette=palette[8], low=low, high=high)
        fig.patches(
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
        fig.add_tools(hovering)
        self.output().save_bokeh(fig)
