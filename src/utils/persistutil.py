from pyspark.sql import SparkSession, DataFrame
from pyspark import StorageLevel
import py4j


class PersistUtil:
    spark: SparkSession = None

    @classmethod
    def persist(cls, df: DataFrame, table_name: str, storage_level=StorageLevel.MEMORY_AND_DISK):
        cls.spark = df.sparkSession
        j_df = df._jdf
        j_table_name = cls.spark._jvm.scala.Some(table_name)
        j_storage_level = cls.spark._sc._getJavaStorageLevel(storage_level)
        cls.spark._jsparkSession.sharedState().cacheManager().cacheQuery(j_df, j_table_name, j_storage_level)
        df.is_cached = True
        return df

    @classmethod
    def unpersist(cls, table_name):
        is_present = False
        for id, dataframe in cls.spark._jsc.getPersistentRDDs().items():
            cached_table_name = str(py4j.java_gateway.get_method(dataframe, "name")())
            if cached_table_name.lower() == f"In-memory table {table_name}".lower():
                dataframe.unpersist(True)
                is_present = True

        if is_present:
            print(f"UnPersisted DataFrame -> {table_name}")
        else:
            print(f"DataFrame  {table_name} doesn't exist in cache!")

    @classmethod
    def clear_all_cache(cls):
        cls.spark._jsparkSession.catalog().clearCache()
