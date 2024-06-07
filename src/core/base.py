from pyspark.sql import DataFrame
from src.core.sparksession import SparkClass
from src.core.refconfig import BaseRefConfig
from src.core.params import BaseParam
from src.utils.env import *
from src.services.datasource_service import DataConfig
from src.utils.read_write_utils import read_df_from_source, save_dataframe


class BaseClass(SparkClass, BaseRefConfig):

    def __init__(self, param: BaseParam, app_name: str):
        self.app_name = app_name
        self.env = param.environment
        self.log_level = param.log_level
        self.runtime_conf = param.runtime_conf
        self.param = param

    def main(self):
        self.init()
        self.init_static_params()
        self.execute()
        self.stop()

    def init(self):
        set_environment_var(self.runtime_conf, self.env)
        self.init_spark_session(self.app_name, self.env, self.log_level)
        self.init_configurations(self.param, self.param.job_name, self.env)

        if self.param.is_debug:
            self.display_configurations()
            self.display_spark_settings()

    def init_static_params(self):
        pass

    def execute(self):
        pass

    def stop(self):
        self.stop_spark_session()

    def read_df_wrapper(self, data_config: DataConfig):
        return read_df_from_source(self.spark, data_config, self.param.environment, self.param.skip_local,
                                   self.param_dict)

    def save_df_wrapper(self, df: DataFrame, data_config: DataConfig):
        if data_config.overwrite_condition is not None:
            data_config.overwrite_condition = data_config.overwrite_condition.format_map(self.param_dict)
        save_dataframe(df, data_config, self.param.environment, self.param.skip_local, self.db_service)
