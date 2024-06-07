import unittest
from pyspark.sql import SparkSession
from src.core.sparksession import SparkClass


class SparkSessionTestWrapper(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark: SparkSession = SparkClass().init_spark_session("Pyspark unit test", "TEST")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
