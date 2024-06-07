import os
import platform
import sys

sys.path.insert(0, os.getcwd())
from src.core.base import BaseClass
from src.core.params import Param
from src.services.enrichment_service import *
from pyspark.sql.functions import StructType

class SampleExpr(BaseClass):

    def __init__(self, param: BaseParam, app_name: str):
        super().__init__(param, app_name)
        self.param_dict = dict()

    def init_static_params(self):
        print("Initialize static parameters")
        self.param_dict = {}

    def execute(self):
        print("Inside execute method")

        self.display_spark_settings()

        print(self.spark.version)

        df = self.spark.createDataFrame(data=[], schema=StructType([]))
        df.show()

        source_df: DataFrame = self.read_df_wrapper(self.base_dataset_conf)
        source_df.show(5, False)


if __name__ == "__main__":
    print("Main application started..")
    if platform.system().startswith("Windows"):
        cmd_args = "local sample 10 20 30 --skip_local".split()
    else:
        cmd_args = sys.argv[1:]

    file_name = os.path.basename(__file__)
    param = Param(cmd_args, file_name)
    print(param)

    service_obj = SampleExpr(param, param.job_name)
    service_obj.main()
