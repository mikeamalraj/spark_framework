from pyspark.sql import SparkSession
import sys
import platform
from src.utils.fileutil import *
from src.root import SPARK_CONF_PATH

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


class SparkClass:

    def __int__(self):
        self.spark: SparkSession = None

    def init_spark_session(self, app_name: str, env: str, log_level: str = None) -> SparkSession:
        try:
            def create_builder() -> SparkSession.Builder:
                builder = SparkSession \
                    .builder \
                    .appName(app_name)
                return set_master_config(builder)

            def set_master_config(builder: SparkSession.Builder) -> SparkSession.Builder:
                builder.master("local[4]") if is_local else builder.enableHiveSupport()
                return set_session_config(builder)

            def set_session_config(builder: SparkSession.Builder) -> SparkSession.Builder:
                if config and env in config:
                    for key, value in config[env].items():
                        builder.config(key, value)
                return builder

            def create_spark_session(builder: SparkSession.Builder) -> SparkSession:
                return builder.getOrCreate()

            def set_log_level(spark: SparkSession) -> None:
                spark.sparkContext.setLogLevel("WARN") if is_local else spark.sparkContext.setLogLevel(
                    log_level.upper()) if isinstance(log_level, str) else None

        except Exception as e:
            print("Error on line {}".format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

        is_local = platform.system().startswith("Windows")
        config: dict = read_config_file(SPARK_CONF_PATH)
        spark_builder = create_builder()
        spark = create_spark_session(spark_builder)
        set_log_level(spark)
        self.spark = spark
        return self.spark

    def get_spark_session(self):
        return self.spark if isinstance(self.spark, SparkSession) else None

    def display_spark_settings(self):
        settings = self.spark.sparkContext.getConf().getAll() if isinstance(self.spark, SparkSession) else None
        print("******************************* SPARK CONFIGS ****************************")
        print(settings)

    def stop_spark_session(self):
        self.spark.stop() if isinstance(self.spark, SparkSession) else None
