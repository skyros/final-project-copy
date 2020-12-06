from csci_utils.luigi.task import TargetOutput
from luigi import ExternalTask


class StateShapeFiles(ExternalTask):
    """This task gets shape file from a bucket in s3"""

    # TODO

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
