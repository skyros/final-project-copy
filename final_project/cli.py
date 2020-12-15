import argparse
import os
import webbrowser

import dask.dataframe as dd
from luigi import build

from .visualizedata import VisualizedData

parser = argparse.ArgumentParser(
    prog="python -m final_project",
    description="Trigger Luigi Pipeline That Generates an HTML Visualization of Covid-19 Data.\nGenerated Data/HTML is saved in the Data Directory",
)
parser.add_argument(
    "-o",
    "--open",
    action="store_true",
    default=False,
    help="Open Visualization In Browser After Saving",
)
parser.add_argument(
    "-i",
    "--info",
    action="store_true",
    default=False,
    help="Show Information About Data: Date/Death Totals",
)


def main(args=None):

    args = parser.parse_args()

    build(
        [VisualizedData()],
        local_scheduler=True,
    )

    if args.open:
        url = "file://" + os.path.join(os.getcwd(), "data", "VisualizedData.html")
        webbrowser.open(url)

    if args.info:
        ddf = dd.read_parquet("data/PopulationStats", columns=["date", "death"])
        df = ddf.groupby("date").sum().compute()
        date = df.index[0].strftime("%B %d, %Y")
        tota_deaths = df.iloc[0][0]
        txt = "Data Current Through: {}\nRecorded United States Covid-19 Deaths: {}".format(
            date, tota_deaths
        )
        print(txt)
